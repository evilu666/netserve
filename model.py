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

@dataclass
class ResultRequest(Request):
    job_id: str

@request("TextGenerationResult")
@dataclass
class TextGenerationResultRequest(ResultRequest):
    pass

@response("TextGeneration")
@dataclass
class TextGenerationResponse(Response):
    text: str

@request("Conversation")
@dataclass
class ConversationRequest(Request):
    model: str
    text: str
    past_inputs: list[str] | None = None
    past_responses: list[str]| None = None
    min_length: int = 50
    uuid: str | None = None

@request("ConversationResult")
@dataclass
class ConversationResultRequest(ResultRequest):
    pass

@response("Conversation")
@dataclass
class ConversationResponse(Response):
    response: str

class ControlType(Enum):
    START = 1
    STOP = 2
    RESTART = 3

class PipelineStatus(Enum):
    STARTING = 1
    RUNNING = 2
    STOPPED = 3
    INSTALLING = 4

@request("PipelineControl")
@dataclass
class PipelineControlRequest(Request):
    model: str
    control_type: ControlType

@request("PipelineControlResult")
class PipelineControlResultRequest(ResultRequest):
    pass

@response("PipelineControl")
@dataclass
class PipelineControlResponse(Response):
    status: PipelineStatus

@dataclass
class ModelInfo(JSONWizard):
    name: str
    size: int
    status: PipelineStatus

@request("ModelListing")
@dataclass
class ModelListingRequest(Request):
    filter_regex: str = None

@request("ModelListingResult")
@dataclass
class ModelListingResultRequest(ResultRequest):
    pass

@response("ModelListing")
@dataclass
class ModelListingResponse(Response):
    models: list[ModelInfo]

@request("ModelInstall")
@dataclass
class ModelInstallRequest(Request):
    model: str

@request("ModelInstallResult")
@dataclass
class ModelInstallResultRequest(ResultRequest):
    pass

@response("ModelInstall")
@dataclass
class ModelInstallResponse(Response):
    pass

@response("JobInfo")
@dataclass
class JobInfoResponse(Response):
    job_id: str

class JobStatus(Enum):
    SCHEDULED = 1
    RUNNING = 2
    DONE = 3

@response("JobStatus")
@dataclass
class JobStatusResponse(Response):
    job_id: str
    status: JobStatus

@response("JobProgress")
@dataclass
class JobProgressResponse(Response):
    job_id: str
    progress: float

class Topics:
    JOB_STATUS = "status"
    JOB_PROGRESS = "progress"
