
import sys
from model import *
import zmq
from abc import abstractmethod

from transformers import pipeline
from prettytable import PrettyTable

import argparse

context = zmq.Context()
socket = context.socket(zmq.REQ)
socket.connect("ipc:///tmp/netserve")

__DEBUG = False

# https://stackoverflow.com/a/1094933
def sizeof_fmt(num, suffix="B"):
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def send_request(req: Request, timeout: int = 30) -> Response:
    socket.send(bytes(req.TYPE_NAME, 'utf-8'))
    socket.recv()

    socket.send(bytes(req.to_json(), 'utf-8'))

    response_type = str(socket.recv(), "utf-8")
    socket.send(ACK)

    response_text = str(socket.recv(), "utf-8")

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
            tbl = PrettyTable();
            tbl.field_names = ["Name", "Size"]
            for model in resp.models:
                tbl.add_row([model.name, sizeof_fmt(model.size)])
            print("Models:")
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
        text = "\n".join([line for line in stdin])
    else:
        if not args.text:
            print("Error: must provide text when not reading from stdin!")
            return
        text = args.text

    resp = send_request(TextGenerationRequest(args.model, text))
    if resp:
        print(resp.text)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Interact with ai models using the huggingface transformers library")
    parser.add_argument("-t", "--timeout", default = 30, type=int, help="Timeout in seconds to wait for command completion before aborting")
    root_subparsers = parser.add_subparsers()

    model_parser = root_subparsers.add_parser("model", help="List, install, start or stop available models")
    model_parser.add_argument("action", choices = ["start", "stop", "restart", "list", "install"], help="The action to perform")
    model_parser.add_argument("model", nargs = '?', type=str, help="The model for which to perform the action (not needed for the list action)")
    model_parser.add_argument("-f", "--filter", nargs = '?', type=str, help="Pattern to use for filtering when using the list option")
    model_parser.set_defaults(func = handle_model_mode)

    generate_parser = root_subparsers.add_parser("generate", help="Generate text using the provided input")
    generate_parser.add_argument("model", type=str, help="The model to use for text generation")
    generate_parser.add_argument("text", type=str, nargs = '?', help="The text to use as input for generation (not needed when reading from standard input)")
    generate_parser.add_argument("-i", "--stdin", action="store_true", help="When set to true will read the input text from the standard input")
    generate_parser.set_defaults(func = handle_generate_mode)

    args = parser.parse_args()
    args.func(args)


