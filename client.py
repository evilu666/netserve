
import sys
from model import *
from messaging import send_message, receive_message
import zmq
from abc import abstractmethod

from transformers import pipeline
from prettytable import PrettyTable

import argparse

import progressbar


__DEBUG = False

# https://stackoverflow.com/a/1094933
def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def send_request(req: Request, timeout: int = 30) -> Response:
    global socket

    send_message(socket, req)
    resp = receive_message(socket)

    if __DEBUG: print(f"Response type: {resp.TYPE_NAME}, Content: {resp.to_json()}")
    if resp.TYPE_NAME == ErrorResponse.TYPE_NAME:
        print("Error: " + resp.msg, file = sys.stderr)
        return False

    return resp


def send_async_request(req: Request, result_request_type: ResultRequest, timeout: int = 30) -> Response:
    global socket
    global subscriber

    send_message(socket, req)
    jobInfo: JobInfoResponse = receive_message(socket)

    if __DEBUG: print(f"Job scheduled with id {jobInfo.job_id}", file=sys.stderr)


    is_done = False

    pb = progressbar.ProgressBar(max_value=1.0)

    while not is_done:
        topic, update = receive_message(subscriber, has_topic=True)

        if isinstance(update, JobStatusResponse):
            if update.status != JobStatus.DONE:
                if __DEBUG: print(f"Received status update for job with id {update.job_id}: {JobStatus(update.status).name}", file=sys.stderr)
            else:
                if __DEBUG: print(f"Received last status update for job with id {update.job_id}: {JobStatus(update.status).name}", file=sys.stderr)
                if pb.currval != 1.0:
                    pb.update(1.0)
                is_done = True
        else:
            pb.update(update.progress)

    send_message(socket, result_request_type(jobInfo.job_id))
    resp: Response = receive_message(socket)

    if __DEBUG: print(f"Response type: {resp.TYPE_NAME}, Content: {resp.to_json()}")
    if resp.TYPE_NAME == ErrorResponse.TYPE_NAME:
        print("Error: " + resp.msg, file = sys.stderr)
        return False

    return resp

def handle_model_mode(args):
    if args.action in {"start", "stop", "restart"}:
        if not args.model:
            print("Must provide model with start, stop or restart action!", file=sys.stderr)
            return

        print(args.action.capitalize() + ("ing model '%s'... " % args.model), end = "")
        resp = send_async_request(PipelineControlRequest(model = args.model, control_type = ControlType[args.action.upper()]), PipelineControlResultRequest)
        if resp:
            print("DONE")
        else:
            print("ERROR")
    elif args.action == "list":
        resp = send_request(ModelListingRequest(filter_regex = args.filter if args.filter else None))
        if resp:
            if args.names_only:
                for model in resp.models:
                    print(model.name)
            else:
                tbl = PrettyTable();
                tbl.field_names = ["Name", "Size", "Status"]
                for model in resp.models:
                    tbl.add_row([model.name, sizeof_fmt(model.size), PipelineStatus(model.status).name])
                print(tbl)
    elif args.action == "install":
        if not args.model:
            print("Error: Must provide a model to install!", file=sys.stderr)
            return
        print("Installing model '%s', this may take a while... " % args.model)
        resp = send_async_request(ModelInstallRequest(args.model), ModelInstallResultRequest)

def handle_generate_mode(args):
    text = None
    if args.stdin:
        text = "\n".join([line.rstrip("\n\r") for line in sys.stdin])
    else:
        if not args.text:
            print("Error: must provide text when not reading from stdin!")
            return
        text = args.text

    resp = send_async_request(TextGenerationRequest(args.model, text, max_length=args.max_length), TextGenerationResultRequest)
    if resp:
        print(resp.text)

def handle_converse_mode(args):
    if len(args.past_input) != len(args.past_response):
        print("Error: past inputs length must match past responses length")
        return

    resp = send_async_request(ConversationRequest(
        args.model,
        args.input,
        past_inputs = args.past_input,
        past_responses = args.past_response,
        min_length=args.min_length),
        ConversationResultRequest
    )

    if resp:
        print(resp.response)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Interact with ai models using the huggingface transformers library")
    parser.add_argument("--port", required=False, default=3892, type=int, help="Port to connect on (only needed for certain protocols)")
    parser.add_argument("--host", required=False, default="127.0.0.1", help="Hostname connect to (only needed for certain protocols)")
    parser.add_argument("--protocol", required=False, default="ipc", choices=("ipc", "tcp"), help="Protocol to use for the connection")
    parser.add_argument("--socket", required=False, default="/tmp/netserve", help="Path or name of the socket to bind to (only needed for certain protocols)")
    parser.add_argument("-d", "--debug", required=False, default=False, action="store_true", help="Enabled debug log")
    parser.add_argument("-t", "--timeout", default = 30, type=int, help="Timeout in seconds to wait for command completion before aborting")

    root_subparsers = parser.add_subparsers()

    model_parser = root_subparsers.add_parser("model", help="List, install, start or stop available models")
    model_parser.add_argument("action", choices = ["start", "stop", "restart", "list", "install"], help="The action to perform")
    model_parser.add_argument("model", nargs = '?', type=str, help="The model for which to perform the action (not needed for the list action)")
    model_parser.add_argument("-f", "--filter", nargs = '?', type=str, help="Pattern to use for filtering when using the list option")
    model_parser.add_argument("--names-only", action="store_true", help="When set will only print a list of model names")
    model_parser.set_defaults(func = handle_model_mode)

    generate_parser = root_subparsers.add_parser("generate", help="Generate text using the provided input")
    generate_parser.add_argument("model", type=str, help="The model to use for text generation")
    generate_parser.add_argument("text", type=str, nargs = '?', help="The text to use as input for generation (not needed when reading from standard input)")
    generate_parser.add_argument("-i", "--stdin", action="store_true", help="When set to true will read the input text from the standard input")
    generate_parser.add_argument("-m", "--max-length", type=int, default=50, help="Maximum number of additional tokens to generate")
    generate_parser.set_defaults(func = handle_generate_mode)

    converse_parser = root_subparsers.add_parser("converse", help="Gnerate text in a conversational format")
    converse_parser.add_argument("model", type=str, help="The model to use for text generation")
    converse_parser.add_argument("input", type=str, help="The user input to get a response for")
    converse_parser.add_argument("--past-input", type=str, nargs="*", default=[], help="A past user input, can be specified multiple times but must match the past response count")
    converse_parser.add_argument("--past-response", type=str, nargs="*", default=[], help="A past respone, can be specified multiple times but must match the past user input count")
    converse_parser.add_argument("-m", "--min-length", type=int, default=50, help="Minimum number of tokens to generate")
    converse_parser.set_defaults(func = handle_converse_mode)

    args = parser.parse_args()

    __DEBUG = args.debug

    if args.func:
        context = zmq.Context()
        socket = context.socket(zmq.REQ)

        subscriber = context.socket(zmq.SUB)

        if args.protocol == "ipc":
            socket.connect(f"{args.protocol}://{args.socket}")
            subscriber.connect(f"{args.protocol}://{args.socket}_sub")
        else:
            socket.connect(f"{args.protocol}://{args.host}:{args.port}")
            subscriber.connect(f"{args.protocol}://{args.host}:{args.port+1}")

        subscriber.setsockopt_string(zmq.SUBSCRIBE, Topics.JOB_STATUS)
        subscriber.setsockopt_string(zmq.SUBSCRIBE, Topics.JOB_PROGRESS)

        args.func(args)
