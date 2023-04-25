from zmq import Socket
from model import Response, Request, ErrorResponse, parse_response, parse_request

def send_message(socket: Socket, msg: Request | Response, topic: str | None = None):
    msg_str = None
    if topic is not None:
        msg_str = f"{topic} {msg.TYPE_NAME} {msg.to_json()}"
    else:
        msg_str = f"{msg.TYPE_NAME} {msg.to_json()}"

    return socket.send(bytes(msg_str, 'utf-8'))

async def receive_message_async(socket: Socket, is_response: bool = True, has_topic: bool = False) -> Response | Request:
    msg = str(await socket.recv(), 'utf-8')

    msg_topic = None
    msg_type = None
    msg_text = None

    space = msg.index(" ")

    if has_topic:
        msg_topic = msg[:space]

        space2 = msg.index(" ", space+1)
        msg_type = msg[space+1:space2]
        msg_text = msg[space2+1:]
    else:
        msg_type = msg[:space]
        msg_text = msg[space+1:]
    parsed_msg = parse_response(msg_type, msg_text) if is_response else parse_request(msg_type, msg_text)
    return (msg_topic, parsed_msg) if has_topic else parsed_msg

def receive_message(socket: Socket, is_response: bool = True, has_topic: bool = False) -> Response | Request:
    msg = str(socket.recv(), 'utf-8')

    msg_topic = None
    msg_type = None
    msg_text = None

    space = msg.index(" ")

    if has_topic:
        msg_topic = msg[:space]

        space2 = msg.index(" ", space+1)
        msg_type = msg[space+1:space2]
        msg_text = msg[space2+1:]
    else:
        msg_type = msg[:space]
        msg_text = msg[space+1:]

    parsed_msg = parse_response(msg_type, msg_text) if is_response else parse_request(msg_type, msg_text)
    return (msg_topic, parsed_msg) if has_topic else parsed_msg
