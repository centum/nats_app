from functools import wraps

from nats.aio.msg import Msg

from nats_app.marshaling import normalize_payload, to_bytes


def responder():
    def _responder(next_handler):
        @wraps(next_handler)
        async def wrap(msg: Msg):
            result = await next_handler(msg)
            if result is not None:
                if not isinstance(result, bytes):
                    result = to_bytes(normalize_payload(result))
                await msg.respond(result)

        return wrap

    return _responder


def errors_handler(error_handler):
    def _errors_handler(next_handler):
        @wraps(next_handler)
        async def wrap(msg: Msg):
            try:
                return await next_handler(msg)
            except Exception as e:
                return error_handler(msg, e)

        return wrap

    return _errors_handler
