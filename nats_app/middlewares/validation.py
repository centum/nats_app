from functools import wraps
from typing import Optional

from nats.aio.msg import Msg
from pydantic import validate_call

from nats_app.marshaling import from_bytes, normalize_payload, to_bytes


def bytes_to_str(by_pass_msg: Optional[str] = None):
    _by_pass_msg = None

    def _func(next_handler):
        @wraps(next_handler)
        async def wrap(msg: Msg):
            data = from_bytes(msg.data)
            kwargs = {}
            if by_pass_msg:
                kwargs[by_pass_msg] = msg
            result = await next_handler(data, **kwargs)
            if result is not None and not isinstance(result, bytes):
                result = to_bytes(normalize_payload(result))
            return result

        return wrap

    if not callable(by_pass_msg):
        _by_pass_msg = by_pass_msg
        return _func
    return _func(by_pass_msg)


def validate_args(by_pass_msg: Optional[str] = None):
    _by_pass_msg = None

    def _func(next_handler):
        @wraps(next_handler)
        async def wrap(msg: Msg):
            data = from_bytes(msg.data)
            args, kwargs = data.get("args", []), data.get("kwargs", {})
            if _by_pass_msg:
                kwargs[_by_pass_msg] = msg
            result = await validate_call(next_handler)(*args, **kwargs)
            if result is not None and not isinstance(result, bytes):
                result = to_bytes(normalize_payload(result))
            return result

        return wrap

    if not callable(by_pass_msg):
        _by_pass_msg = by_pass_msg
        return _func
    return _func(by_pass_msg)
