from model import *
import zmq
from abc import abstractmethod

from transformers import pipeline

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("ipc:///tmp/netserve")

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
                return ErrorResponse(str(e))
        return PipelineControlResponse(PipelineStatus.RUNNING)
    else:
        return PipelineControlResponse( PipelineStatus.STOPPED)

@handler(TextGenerationRequest)
def handle_text_generation(req: TextGenerationRequest) -> TextGenerationResponse:
    if req.model not in GlobalState.running_pipelines or not GlobalState.running_pipelines[req.model]:
        GlobalState.running_pipelines[req.model] = pipeline(model = req.model)

    try:
        return TextGenerationResponse(GlobalState.running_pipelines[req.model](req.text, max_length = req.max_length)[0]["generated_text"])
    except Exception as e:
        return ErrorResponse(str(e))

while True:
    request_type: str = str(socket.recv(), "utf-8")
    socket.send(ACK)
    print("Received request of type: %s" % request_type)

    request_text = str(socket.recv(), "utf-8")

    print("Request text:", request_text)

    resp = None
    if request_type not in __request_handlers:
        resp = ErrorResponse("No handler registered for request of type: %s" % request_type)
    else:
        handler = __request_handlers[request_type]
        resp = handler(request_text)

    print("Sending response: ", resp)
    socket.send(bytes(resp.TYPE_NAME, 'utf-8'))
    socket.recv()

    socket.send(bytes(resp.to_json(), 'utf-8'))
