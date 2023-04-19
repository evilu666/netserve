import sys
import traceback
from model import *
import zmq
from abc import abstractmethod

from transformers import pipeline, Conversation
from huggingface_hub import snapshot_download, scan_cache_dir

import argparse

__DEBUG = True




class GlobalState:
    running_pipelines = dict()

class RequestHandler:

    def __init__(self, request_class):
        self.request_class = request_class

    def __call__(self, request_text: str):
        req = parse_request(self.request_class.TYPE_NAME, request_text)
        return self.handle_request(req)

    @abstractmethod
    def handle_request(self, req: Request) -> Response:
        pass

class SimpleRequestHandler(RequestHandler):

    def __init__(self, request_class, handler_func):
        super().__init__(request_class)
        self.handler_func = handler_func

    def handle_request(self, req: Request) -> Response:
        return self.handler_func(req)

__request_handlers = {}

def handler(request_class):
    def __handler(func):
        __request_handlers[request_class.TYPE_NAME] = SimpleRequestHandler(request_class, func)
        return func
    return __handler

@handler(PipelineControlRequest)
def handle_pipeline_control(req: PipelineControlRequest) -> PipelineControlResponse:
    if req.control_type == ControlType.STOP or req.control_type == ControlType.RESTART:
        if req.model in GlobalState.running_pipelines and GlobalState.running_pipelines[req.model]:
            del GlobalState.running_pipelines[req.model]
    if req.control_type == ControlType.START or req.control_type == ControlType.RESTART:
        if req.model not in GlobalState.running_pipelines or not GlobalState.running_pipelines[req.model]:
            try:
                GlobalState.running_pipelines[req.model] = pipeline(model = req.model)
            except Exception as e:
                traceback.print_exc()
                return ErrorResponse(str(e))
        return PipelineControlResponse(PipelineStatus.RUNNING)
    else:
        return PipelineControlResponse( PipelineStatus.STOPPED)

@handler(ModelListingRequest)
def handle_model_listing(req: ModelListingRequest) -> ModelListingResponse:
    models = []
    for repo_info in scan_cache_dir().repos:
        if repo_info.repo_type == "model":
            models.append(ModelInfo(repo_info.repo_id, repo_info.size_on_disk))
    return ModelListingResponse(models)


@handler(TextGenerationRequest)
def handle_text_generation(req: TextGenerationRequest) -> TextGenerationResponse:
    if req.model not in GlobalState.running_pipelines or not GlobalState.running_pipelines[req.model]:
        GlobalState.running_pipelines[req.model] = pipeline(model = req.model)

    try:
        return TextGenerationResponse(GlobalState.running_pipelines[req.model](req.text, max_new_tokens = req.max_length)[0]["generated_text"])
    except Exception as e:
        traceback.print_exc()
        return ErrorResponse(str(e))

@handler(ConversationRequest)
def handle_conversation(req: ConversationRequest) -> ConversationResponse:
    if req.model not in GlobalState.running_pipelines or not GlobalState.running_pipelines[req.model]:
        GlobalState.running_pipelines[req.model] = pipeline(model = req.model)

    try:
        conv = Conversation(req.text, req.uuid, req.past_inputs, req.past_responses)
        conv = GlobalState.running_pipelines[req.model](conv, min_length_for_response = req.min_length)
        return ConversationResponse(conv.generated_responses[-1])
    except Exception as e:
        traceback.print_exc()
        return ErrorResponse(str(e))


@handler(ModelInstallRequest)
def handle_model_install(req: ModelInstallRequest) -> ModelInstallResponse:
    try:
        snapshot_download(req.model)
        return ModelInstallResponse()
    except Exception as e:
        traceback.print_exc()
        return ErrorResponse(str(e))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Interact with ai models using the huggingface transformers library")
    parser.add_argument("--port", required=False, default=3892, type=int, help="Port to listen on (only needed for certain protocols)")
    parser.add_argument("--host", required=False, default="127.0.0.1", help="Hostname of the interface to bind to (only needed for certain protocols)")
    parser.add_argument("--protocol", required=False, default="ipc", choices=("ipc", "tcp"), help="Protocol to listen for requests on")
    parser.add_argument("--socket", required=False, default="/tmp/netserve", help="Path or name of the socket to bind to (only needed for certain protocols)")
    parser.add_argument("-d", "--debug", required=False, default=False, action="store_true", help="Enabled debug log")

    args = parser.parse_args()

    __DEBUG = args.debug

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    if args.protocol == "ipc":
        socket.bind(f"{args.protocol}://{args.socket}")
    else:
        socket.bind(f"{args.protocol}://{args.host}:{args.port}")

    while True:
        req = str(socket.recv(), "utf-8")
        request_type = req[:req.index(" ")]
        request_text = req[req.index(" ")+1:]

        if __DEBUG: print("Request text:", request_text)

        resp = None
        if request_type not in __request_handlers:
            resp = ErrorResponse("No handler registered for request of type: %s" % request_type)
        else:
            handler = __request_handlers[request_type]
            resp = handler(request_text)

        if __DEBUG: print("Sending response: ", resp)
        socket.send(bytes(resp.TYPE_NAME + " " + resp.to_json(), 'utf-8'))
