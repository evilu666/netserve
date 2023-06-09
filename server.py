import sys
import traceback
from model import *
from messaging import send_message, receive_message, receive_message_async
from abc import abstractmethod
from datetime import datetime

from transformers import pipeline, Conversation
from huggingface_hub import snapshot_download, scan_cache_dir

from uuid import uuid4
import psutil

from ray.actor import ActorHandle
import ray
from queue import Queue
from asyncio import Event, Lock
import asyncio
import zmq
import zmq.asyncio
import threading
import concurrent.futures

import time

from tqdm.auto import tqdm as std_tqdm
import argparse

NOOP = lambda x: None

__DEBUG = True

__tasks = set()

def run_sync(coroutine, callback: callable = NOOP):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        loop.set_debug(True)
        print("Running as task", file=sys.stderr)
        task = loop.create_task(coroutine)
        print("Scheduled task:", task, file=sys.stderr)
        task.add_done_callback(lambda f: callback(f.result()))

        __tasks.add(task)
        task.add_done_callback(__tasks.discard)
    else:
        print("Running in own loop", file=sys.stderr)
        callback(asyncio.run(func(*args, **kwargs)))

def model_key(model: str, revision: str | None = None) -> str:
    if revision:
        return f"{model}$$${revision}"
    return model

def model_name(model: str, revision: str | None = None) -> str:
    if revision:
        return f"{model} ({revision})"
    return model

@ray.remote
class ResultCache:
    results: dict

    def __init__(self):
        self.results = {}

    def put_result(self, uuid: str, result):
        self.results[uuid] = result

    def pop_result(self, uuid: str):
        result = self.results[uuid]
        print("Popping result with id:", uuid)
        del self.results[uuid]

        JobHandler.pop_job_result(uuid)

        return result

class JobHandler:
    jobs: dict = {}

    @staticmethod
    def reserve_id():
        uuid = str(uuid4())
        while uuid in JobHandler.jobs:
            uuid = str(uuid4())

        JobHandler.jobs[uuid] = False
        return uuid

    @staticmethod
    def put_job(uuid: str, ref):
        JobHandler.jobs[uuid] = ref

    @staticmethod
    def pop_job_result(uuid: str):
        del JobHandler.jobs[uuid]

    @staticmethod
    def has_job(job_id: str):
        return job_id in JobHandler.jobs and JobHandler.jobs[job_id] is not False

@ray.remote
class ProgressActor:
    #progress: dict
    #event: Event
    #lock: Lock

    progress_events: Queue

    def __init__(self):
        #self.progress = {}
        #self.event = Event()
        #self.lock = Lock()
        self.progress_events = Queue()

    def update(self, uuid: str, progress: float, callback: callable = NOOP):
        #async def __run():
        #    async with self.lock:
        #        self.progress[uuid] = progress
        #        self.event.set()
        #return run_sync(__run(), callback)
        self.progress_events.put({'job_id': uuid, 'progress': progress})

    def update_done(self, uuid: str, callback: callable = NOOP):
        #print(f"Updating DONE on job with id {uuid}", file=sys.stderr)
        #async def __run():
        #    async with self.lock:
        #        self.progress[uuid] = True
        #        self.event.set()
        #        print(f"Updated DONE on job with id {uuid}", file=sys.stderr)
        #return run_sync(__run(), callback)
        self.progress_events.put({'job_id': uuid, 'progress': True})

    def get_updates(self):
        #print("Started getting updates", file=sys.stderr)
        #async def __run():
        #    print("Waiting for updates", file=sys.stderr)
        #    await self.event.wait()
        #    updates = None
        #    async with self.lock:
        #        updates = self.progress.copy()
        #        self.progress = {}

        #    print("Got updates:", updates, file=sys.stderr)
        #    self.event.clear()
        #    return updates

        #run_sync(__run(), callback)
        updates = {}
        while not self.progress_events.empty():
            update = self.progress_events.get()
            updates[update['job_id']] = update['progress']
            time.sleep(0.1)
        return updates

async def handle_progress(socket_bind: str, progress_actor: ActorHandle):
    global context
    socket = context.socket(zmq.PUB)
    if __DEBUG: print(f"Binding pub socket to:", socket_bind, file=sys.stderr)
    socket.bind(socket_bind)

    while True:
        updates = await progress_actor.get_updates.remote()

        if len(updates) == 0:
            await asyncio.sleep(0.1)
            continue

        if __DEBUG: print("Updates:", updates, file=sys.stderr)

        for uuid, progress in updates.items():
            msg = None
            topic = None
            if isinstance(progress, bool):
                msg = JobStatusResponse(uuid, JobStatus.DONE)
                topic = Topics.JOB_STATUS
            else:
                msg = JobProgressResponse(uuid, progress)
                topic = Topics.JOB_PROGRESS

            if __DEBUG: print("Sending Update:", msg, file=sys.stderr)
            await send_message(socket, msg, topic=topic)

@ray.remote
class PipelineActor:
    running_pipelines: dict
    starting_pipelines: set
    installing_pipelines: set
    control_lock: Lock
    pipeline_locks: dict

    def __init__(self):
        self.running_pipelines = {}
        self.starting_pipelines = set()
        self.installing_pipelines = set()
        self.control_lock = Lock()
        self.pipeline_locks = {}


    def get_status(self, name: str, revision: str | None = None) -> PipelineStatus:
        key = model_key(name, revision = revision)
        if key in self.starting_pipelines:
            return PipelineStatus.STARTING

        if key in self.installing_pipelines:
            return PipelineStatus.INSTALLING

        if key in self.running_pipelines and self.running_pipelines[key]:
            return PipelineStatus.RUNNING

        return PipelineStatus.STOPPED

    def mark_installing(self, name: str, revision: str | None = None):
        self.installing_pipelines.add(model_key(name, revision))

    def mark_install_done(self, name: str, revision: str | None = None):
        self.installing_pipelines.discard(model_key(name, revision))

    def start_pipeline(self, name: str, callback: callable, revision: str | None = None):
        key = model_key(name, revision)
        async def __run():
            #if __DEBUG: print("Acquiring control lock")
            async with self.control_lock:
                #if __DEBUG: print("Acquired control lock")
                if key not in self.pipeline_locks:
                    self.pipeline_locks[key] = Lock()

            #if __DEBUG: print(f"Acquiring pipeline lock for pipeline {name}")
            async with self.pipeline_locks[key]:
                #if __DEBUG: print(f"Acquired pipeline lock for pipeline {name}")
                if self.get_status(name, revision) == PipelineStatus.STOPPED:
                    self.starting_pipelines.add(key)
                    #if __DEBUG: print(f"Allocating pipeline {name}")
                    try:
                        self.running_pipelines[key] = pipeline(model=name, revision=revision)
                    except Exception as e:
                        return ErrorResponse(str(e))
                    finally:
                    #if __DEBUG: print(f"Allocated pipeline {name}")
                        self.starting_pipelines.remove(key)
        return run_sync(__run(), callback)

    def stop_pipeline(self, name: str, callback: callable, revision: str | None = None):
        key = model_key(name, revision)
        async def __run():
            async with self.control_lock:
                if key not in self.pipeline_locks:
                    self.pipeline_locks[key] = Lock()

            async with self.pipeline_locks[key]:
                if self.get_status(name, revision) != PipelineStatus.STOPPED:
                    del self.running_pipelines[key]
        return run_sync(__run(), callback)

    def restart_pipeline(self, name: str, callback: callable, revision: str | None = None):
        key = model_key(name, revision)
        async def __run():
            async with self.control_lock:
                if key not in self.pipeline_locks:
                    self.pipeline_locks[key] = Lock()

            async with self.pipeline_locks[key]:
                if self.get_status(name, revision) != PipelineStatus.STOPPED:
                    del self.running_pipelines[key]
                self.starting_pipelines.add(key)
                try:
                    self.running_pipelines[key] = pipeline(model=name, revision=revision)
                except Exception as e:
                    return ErrorResponse(str(e))
                finally:
                    self.starting_pipelines.discard(key)
        return run_sync(__run(), callback)

    def execute_pipeline(self, name: str, *args, revision: str | None = None, **kwargs):
        try:
            return self.running_pipelines[model_key(name, revision)](*args, **kwargs)
        except Exception as e:
            return ErrorResponse(str(e))

class GlobalState:
    progress_actor: ActorHandle
    pipeline_actor: ActorHandle
    result_actor: ActorHandle

    def __init__(self):
        self.progress_actor = ProgressActor.options(max_concurrency = 10).remote()
        self.pipeline_actor = PipelineActor.options(max_concurrency = 10).remote()
        self.result_actor = ResultCache.options(max_concurrency = 10).remote()

class RequestHandler:

    def __init__(self, request_class):
        self.request_class = request_class

    def __call__(self, state: GlobalState, req: Request):
        return self.handle_request(state, req)

    @abstractmethod
    def handle_request(self, state: GlobalState, req: Request) -> Response:
        pass

    def __repr__(self):
        return f"RequestHandler{self.request_class}"

class SimpleRequestHandler(RequestHandler):

    def __init__(self, request_class, handler_func):
        super().__init__(request_class)
        self.handler_func = handler_func

    def handle_request(self, state: GlobalState, req: Request) -> Response:
        return self.handler_func(state, req)


__request_handlers = {}

def handler(request_class):
    def __handler(func):
        __request_handlers[request_class.TYPE_NAME] = SimpleRequestHandler(request_class, func)
        return func
    return __handler

@ray.remote
def start_pipeline(job_id: str, name: str, pipeline_actor: ActorHandle, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None, update_done = True, on_done: callable = NOOP):
    display_name = model_name(name, revision)
    if __DEBUG: print(f"Scheduling start of pipeline {display_name}", file=sys.stderr)

    async def __run():
        if __DEBUG: print(f"Starting pipeline {display_name}", file=sys.stderr)

        def __on_started(result: ErrorResponse | None):
            if result:
                ray.get(progress_actor.update_done.remote(job_id))
                ray.get(result_actor.put_result.remote(job_id, result))
            else:
                if __DEBUG: print(f"Started pipeline {display_name}", file=sys.stderr)
                if update_done:
                    if __DEBUG: print(f"Setting job with id {job_id} to DONE", file=sys.stderr)
                    ray.get(progress_actor.update_done.remote(job_id))
                    ray.get(result_actor.put_result.remote(job_id, PipelineStatus.RUNNING))
                on_done(PipelineStatus.RUNNING)

        ray.get(pipeline_actor.start_pipeline.remote(name, __on_started, revision = revision))
    run_sync(__run())

@ray.remote
def stop_pipeline(job_id: str, name: str, pipeline_actor: ActorHandle, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None, update_done = True):
    display_name = model_name(name, revision)
    async def __run():
        if __DEBUG: print(f"Stopping pipeline {display_name}", file=sys.stderr)
        def __on_started(result: ErrorResponse | None):
            if result:
                ray.get(progress_actor.update_done.remote(job_id))
                ray.get(result_actor.put_result.remote(job_id, result))
            else:
                if __DEBUG: print(f"Stopped pipeline {display_name}", file=sys.stderr)
                if update_done:
                    ray.get(progress_actor.update_done.remote(job_id))
                    ray.get(result_actor.put_result.remote(job_id, PipelineStatus.STOPPED))

        ray.get(pipeline_actor.stop_pipeline.remote(name, __on_started, revision = revision))
    run_sync(__run())

@ray.remote
def restart_pipeline(job_id: str, name: str, pipeline_actor: ActorHandle, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None, update_done = True):
    display_name = model_name(name, revision)
    async def __run():
        if __DEBUG: print(f"Restarting pipeline {display_name}", file=sys.stderr)
        def __on_started(result: ErrorResponse | None):
            if result:
                ray.get(progress_actor.update_done.remote(job_id))
                ray.get(result_actor.put_result.remote(job_id, result))
            else:
                if __DEBUG: print(f"Restarted pipeline {display_name}", file=sys.stderr)
                if update_done:
                    ray.get(progress_actor.update_done.remote(job_id))
                    ray.get(result_actor.put_result.remote(job_id, PipelineStatus.RUNNING))

        ray.get(pipeline_actor.restart_pipeline.remote(name, __on_started, revision = revision))
    run_sync(__run())

@handler(PipelineControlRequest)
def handle_pipeline_control(state: GlobalState, req: PipelineControlRequest) -> JobInfoResponse | ErrorResponse:
    if ray.get(state.pipeline_actor.get_status.remote(req.model, revision = req.revision)) == PipelineStatus.INSTALLING:
        return ErrorResponse("Pipeline is currently installing")

    uuid = JobHandler.reserve_id()

    ref = None
    if req.control_type == ControlType.STOP:
        ref = stop_pipeline.remote(uuid, req.model, state.pipeline_actor, state.progress_actor, state.result_actor, revision = req.revision)
    elif req.control_type == ControlType.START:
        ref = start_pipeline.remote(uuid, req.model, state.pipeline_actor, state.progress_actor, state.result_actor, revision = req.revision)
    else:
        ref = restart_pipeline.remote(uuid, req.model, state.pipeline_actor, state.progress_actor, state.result_actor, revision = req.revision)

    JobHandler.put_job(uuid, ref)
    return JobInfoResponse(uuid)

@handler(PipelineControlResultRequest)
def handle_pipeline_control_result(state: GlobalState, req: PipelineControlResultRequest) -> PipelineControlResponse | ErrorResponse:
    if not JobHandler.has_job(req.job_id):
        print(f("No result for job with id {req.job_id} found"))
        return ErrorResponse(f"Couldn't find a job with id {req.job_id} or it has not been started yet!")

    print("Getting result for job with id:", req.job_id, file=sys.stderr)

    status_or_error = ray.get(state.result_actor.pop_result.remote(req.job_id))
    print("Popped result:", status_or_error, file=sys.stderr)
    return status_or_error if isinstance(status_or_error, ErrorResponse) else PipelineControlResponse(status_or_error)

@handler(ModelListingRequest)
def handle_model_listing(state: GlobalState, req: ModelListingRequest) -> ModelListingResponse:
    models = []
    for repo_info in scan_cache_dir().repos:
        if repo_info.repo_type == "model":
            if repo_info.revisions:
                for revision_info in repo_info.revisions:
                    ref = next(iter(revision_info.refs)) if revision_info.refs else None
                    try:
                        status = ray.get(state.pipeline_actor.get_status.remote(repo_info.repo_id, revision = ref))
                        print("status")
                        models.append(ModelInfo(
                            repo_info.repo_id,
                            revision_info.size_on_disk,
                            status,
                            revision = ref
                        ))
                    except Exception as e:
                        print_exc()
                        print(e)
            else:
                models.append(ModelInfo(
                    repo_info.repo_info,
                    repo_info.size_on_disk,
                    ray.get(state.pipeline_actor.get_status.remote(repo_info.repo_id))
                ))
    return ModelListingResponse(models)

@ray.remote
def generate_text(job_id: str, model: str, text: str, max_length: int, pipeline_actor: ActorHandle, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None):
    display_name = model_name(model, revision)
    def __generate_text(_):
        if __DEBUG: print(f"Generating text with model {display_name}")
        result = None
        try:
            result = ray.get(pipeline_actor.execute_pipeline.remote(model, text, revision = revision, max_new_tokens = max_length))[0]['generated_text']
        except Exception as e:
            traceback.print_exc()
            result = ErrorResponse(str(e))

        ray.get(progress_actor.update_done.remote(job_id))
        ray.get(result_actor.put_result.remote(job_id, result))

    if pipeline_actor.get_status.remote(model, revision = revision) != PipelineStatus.RUNNING:
        ray.get(start_pipeline.remote(job_id, model, pipeline_actor, progress_actor, result_actor, update_done = False, revision = revision, on_done = __generate_text))
    else:
        __generate_text(None)

@handler(TextGenerationRequest)
def handle_text_generation(state: GlobalState, req: TextGenerationRequest) -> JobInfoResponse:
    uuid = JobHandler.reserve_id()
    ref = generate_text.remote(uuid, req.model, req.text, req.max_length, state.pipeline_actor, state.progress_actor, state.result_actor, revision = req.revision)
    JobHandler.put_job(uuid, ref)

    return JobInfoResponse(uuid)

@handler(TextGenerationResultRequest)
def handle_text_generation_result(state: GlobalState, req: TextGenerationResultRequest) -> TextGenerationResponse | ErrorResponse:
    if not JobHandler.has_job(req.job_id):
        print(f("Error: No result for job with id {req.job_id} found"), file=sys.stderr)
        return ErrorResponse(f"Couldn't find a job with id {req.job_id} or it has not been started yet!")

    print("Getting result for job with id:", req.job_id, file=sys.stderr)

    text_or_error = ray.get(state.result_actor.pop_result.remote(req.job_id))
    print("Popped result:", text_or_error, file=sys.stderr)
    return text_or_error if isinstance(text_or_error, ErrorResponse) else TextGenerationResponse(text_or_error)

@ray.remote
def generate_response(job_id: str, model: str, conversation_id: str, text: str, past_inputs: list, past_responses: list, pipeline_actor: ActorHandle, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None):
    display_name = model_name(model, revision)
    def __generate_response(_):
        if __DEBUG: print(f"Generating response with model {display_name}")
        conv = Conversation(text, conversation_id, past_inputs, past_responses)
        result = None
        try:
            result = ray.get(pipeline_actor.execute_pipeline.remote(model, conv, revision = revision))
            result = result.generated_responses[-1]
        except Exception as e:
            traceback.print_exc()
            result = ErrorResponse(str(e))

        ray.get(progress_actor.update_done.remote(job_id))
        ray.get(result_actor.put_result.remote(job_id, result))

    if pipeline_actor.get_status.remote(model) != PipelineStatus.RUNNING:
        print("Rev:", revision)
        ray.get(start_pipeline.remote(job_id, model, pipeline_actor, progress_actor, result_actor, update_done = False, revision = revision, on_done = __generate_response))
    else:
        __generate_response(None)
    #
    #async def __run():
    #    if await pipeline_actor.get_status.remote(model) != PipelineStatus.RUNNING:
    #        await start_pipeline(job_id, model, pipeline_actor, progress_actor, False)

    #    conv = Conversation(text, conversation_id, past_inputs, past_responses)
    #    result = None

    #    try:
    #        result = await pipeline_actor.execute_pipeline.remote(model, conv)
    #        result = result.generated_responses[-1]
    #    except Exception as e:
    #        traceback.print_exc()
    #        result = ErrorResponse(str(e))

    #    await progress_actor.update_done.remote(job_id)
    #    return result
    #run_sync(__run(), lambda s: ray.get(result_actor.put_result.remote(job_id, s)))

@handler(ConversationRequest)
def handle_conversation(state: GlobalState, req: ConversationRequest) -> JobInfoResponse:
    uuid = JobHandler.reserve_id()
    ref = generate_response.remote(uuid, req.model, req.uuid, req.text, req.past_inputs, req.past_responses, state.pipeline_actor, state.progress_actor, state.result_actor, revision = req.revision)
    JobHandler.put_job(uuid, ref)

    return JobInfoResponse(uuid)

@handler(ConversationResultRequest)
def handle_conversation_result(state: GlobalState, req: ConversationResultRequest) -> ConversationResponse | ErrorResponse:
    if not JobHandler.has_job(req.job_id):
        print(f("Error: No result for job with id {req.job_id} found"), file=sys.stderr)
        return ErrorResponse(f"Couldn't find a job with id {req.job_id} or it has not been started yet!")

    print("Getting result for job with id:", req.job_id, file=sys.stderr)

    text_or_error = ray.get(state.result_actor.pop_result.remote(req.job_id))
    print("Popped result:", text_or_error, file=sys.stderr)
    return text_or_error if isinstance(text_or_error, ErrorResponse) else ConversationResponse(text_or_error)


@ray.remote
def install_model(job_id: str, model: str, progress_actor: ActorHandle, result_actor: ActorHandle, revision: str | None = None):
    async def __run():

        class ProgressHandler(std_tqdm):
            def update(self, n=1):
                display = super(ProgressHandler, self).update(n)
                if display:
                    ray.get(progress_actor.update.remote(job_id, self.n / self.total))
                return display

            def display(self, *args, **kwargs):
                pass


        result = None
        try:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(pool, lambda: snapshot_download(model, revision = revision, max_workers=4, tqdm_class=ProgressHandler))
            result = True
        except Exception as e:
            traceback.print_exc()
            result = ErrorResponse(str(e))

        ray.get(progress_actor.update_done.remote(job_id))
        ray.get(result_actor.put_result.remote(job_id, result))
    run_sync(__run())

@handler(ModelInstallRequest)
def handle_model_install(state: GlobalState, req: ModelInstallRequest) -> JobInfoResponse:
    if ray.get(state.pipeline_actor.get_status.remote(req.model, revision = req.revision)) == PipelineStatus.INSTALLING:
        return ErrorResponse("Model is currently installing")

    uuid = JobHandler.reserve_id()
    ref = install_model.remote(uuid, req.model, state.progress_actor, state.result_actor, revision = req.revision)
    JobHandler.put_job(uuid, ref)

    return JobInfoResponse(uuid)

@handler(ModelInstallResultRequest)
def handle_model_install_result(state: GlobalState, req: ModelInstallResultRequest) -> ModelInstallResponse | ErrorResponse:
    if not JobHandler.has_job(req.job_id):
        print(f("Error: No result for job with id {req.job_id} found"), file=sys.stderr)
        return ErrorResponse(f"Couldn't find a job with id {req.job_id} or it has not been started yet!")

    print("Getting result for job with id:", req.job_id, file=sys.stderr)

    success_or_error = ray.get(state.result_actor.pop_result.remote(req.job_id))
    print("Popped result:", success_or_error, file=sys.stderr)
    return success_or_error if isinstance(success_or_error, ErrorResponse) else ModelInstallResponse()

def default_thread_count():
    lthreads = psutil.cpu_count(logical=True)
    if lthreads > 3:
        return lthreads - 2;
    return lthreads

def init_ray(thread_count: int):
    ray.init(num_cpus=thread_count, ignore_reinit_error=True, local_mode=True)


async def handle_requests(socket_bind: str, state: GlobalState):
    global context
    socket = context.socket(zmq.REP)
    socket.bind(socket_bind)

    while True:
        req = await receive_message_async(socket, is_response = False)

        if __DEBUG: print("Received Request:", req)

        resp = None

        if req.TYPE_NAME not in __request_handlers:
            resp = ErrorResponse("No handler registered for request of type: %s" % req.TYPE_NAME)
        else:
            handler = __request_handlers[req.TYPE_NAME]
            if __DEBUG: print("Using handler:", handler, file=sys.stderr)
            resp = handler(state, req)

        if __DEBUG: print("Sending response: ", resp, file=sys.stderr)
        await send_message(socket, resp)

async def debug_loop():
    while True:
        if len(__tasks) > 0:
            print("########## DEBUG INFO ##########")
            print("Pending tasks:", file=sys.stderr)
            for t in __tasks:
                print("\t", t, file=sys.stderr)
            print("########## DEBUG INFO ##########")
        await asyncio.sleep(5)

async def main_loop(event_loop, server_socket_bind: str, publisher_socket_bind: str):
    state = GlobalState()
    progress_task = event_loop.create_task(handle_progress(publisher_socket_bind, state.progress_actor))
    server_task = event_loop.create_task(handle_requests(server_socket_bind, state))
    #debug_task = event_loop.create_task(debug_loop())
    await asyncio.wait([progress_task, server_task])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description = "Interact with ai models using the huggingface transformers library")
    parser.add_argument("--port", required=False, default=3892, type=int, help="Port to listen on (only needed for certain protocols)")
    parser.add_argument("--host", required=False, default="127.0.0.1", help="Hostname of the interface to bind to (only needed for certain protocols)")
    parser.add_argument("--protocol", required=False, default="ipc", choices=("ipc", "tcp"), help="Protocol to listen for requests on")
    parser.add_argument("--socket", required=False, default="/tmp/netserve", help="Path or name of the socket to bind to (only needed for certain protocols)")
    parser.add_argument("--threads", required=False, type=int, default=default_thread_count(), help="Number of threads to use for model inference")
    parser.add_argument("-d", "--debug", required=False, default=False, action="store_true", help="Enabled debug log")

    args = parser.parse_args()

    __DEBUG = args.debug

    server_socket_bind = None
    publisher_socket_bind = None
    if args.protocol == "ipc":
        server_socket_bind = f"{args.protocol}://{args.socket}"
        publisher_socket_bind = f"{args.protocol}://{args.socket}_sub"
    else:
        server_socket_bind = f"{args.protocol}://{args.host}:{args.port}"
        publisher_socket_bind = f"{args.protocol}://{args.host}:{args.port+1}"


    context = zmq.asyncio.Context(io_threads=2)
    init_ray(args.threads)

    loop = asyncio.get_event_loop()
    loop.set_debug(__DEBUG)
    loop.run_until_complete(main_loop(loop, server_socket_bind, publisher_socket_bind))
    loop.close()
