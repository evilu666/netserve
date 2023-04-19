
import sys
from model import *
import zmq
from abc import abstractmethod

from transformers import pipeline
from prettytable import PrettyTable

import argparse


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

    socket.send(bytes(req.TYPE_NAME + " " + req.to_json(), 'utf-8'))
    result = str(socket.recv(), "utf-8")

    response_type = result[:result.index(" ")]
    response_text = result[result.index(" ")+1:]

    if __DEBUG: print("Response type:", response_type, "Text:", response_text, file=sys.stderr)

    resp = parse_response(response_type, response_text)

    if response_type == ErrorResponse.TYPE_NAME:
        print("Error: " + resp.msg, file = sys.stderr)
        return False
    else:
        return resp

def handle_model_mode(args):
    if args.action in {"start", "stop", "restart"}:
        if not args.model:
            print("Must provide model with start, stop or restart action!", file=sys.stderr)
            return

        print(args.action.capitalize() + ("ing model '%s'... " % args.model), end = "")
        resp = send_request(PipelineControlRequest(model = args.model, control_type = ControlType[args.action.upper()]))
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
                tbl.field_names = ["Name", "Size"]
                for model in resp.models:
                    tbl.add_row([model.name, sizeof_fmt(model.size)])
                print(tbl)
    elif args.action == "install":
        if not args.model:
            print("Error: Must provide a model to install!", file=sys.stderr)
            return
        print("Installing model '%s', this may take a while... " % args.model, end="")
        resp = send_request(ModelInstallRequest(args.model))

        if resp:
            print("DONE")
        else:
            print("ERROR")


def handle_generate_mode(args):
    text = None
    if args.stdin:
        text = "\n".join([line.rstrip("\n\r") for line in sys.stdin])
    else:
        if not args.text:
            print("Error: must provide text when not reading from stdin!")
            return
        text = args.text

    resp = send_request(TextGenerationRequest(args.model, text, max_length=args.max_length))
    if resp:
        print(resp.text)

def handle_converse_mode(args):
    if len(args.past_input) != len(args.past_response):
        print("Error: past inputs length must match past responses length")
        return

    resp = send_request(ConversationRequest(args.model, args.input, past_inputs = args.past_input, past_responses = args.past_response, min_length=args.min_length))
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

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    if args.protocol == "ipc":
        socket.connect(f"{args.protocol}://{args.socket}")
    else:
        socket.connect(f"{args.protocol}://{args.host}:{args.port}")

    args.func(args)
