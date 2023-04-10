from __future__ import annotations

import json
from io import StringIO
from typing_extensions import TypedDict
from dataclass_wizard import JSONWizard
from enum import Enum, auto
from dataclasses import dataclass

ACK = b'OK'

__requests = {}
__responses = {}
def request( name: str):
    def req(cls):
        cls.TYPE_NAME = name
        __requests[name] = cls
        return cls
    return req

def response(name: str):
    def resp(cls):
        cls.TYPE_NAME = name
        __responses[name] = cls
        return cls
    return resp

def parse_request(request_type: str, req: str) -> Request:
    return __requests[request_type].from_json(req)

def parse_response(response_type: str, resp: str) -> Response:
    return __responses[response_type].from_json(resp)

class Request(JSONWizard):
    pass

class Response(JSONWizard):
    pass

@response("Error")
@dataclass
class ErrorResponse(Response):
    msg: str = None

@request("TextGeneration")
@dataclass
class TextGenerationRequest(Request):
    model: str
    text: str
    max_length: int = 50

@response("TextGeneration")
@dataclass
class TextGenerationResponse(Response):
    text: str

class ControlType(Enum):
    START = auto()
    STOP = auto()
    RESTART = auto()

class PipelineStatus(Enum):
    UNKNOWN = auto()
    RUNNING = auto()
    STOPPED = auto()

@request("PipelineControl")
@dataclass
class PipelineControlRequest(Request):
    model: str
    control_type: ControlType

@response("PipelineControl")
@dataclass
class PipelineControlResponse(Response):
    status: PipelineStatus

@request("PipelineListing")
@dataclass
class PipelineListingRequest(Request):
    filter_regex: str = None

@response("PipelineListing")
@dataclass
class PipelineListingResponse(Response):
    pipelines: list[str]
