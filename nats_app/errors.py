import logging

from fastapi.encoders import jsonable_encoder
from nats.aio.msg import Msg
from pydantic import ValidationError

try:
    from sentry_sdk import capture_exception
except ImportError:
    capture_exception = None


logger = logging.getLogger(__name__)


def default_error_handler(msg: Msg, e: Exception):
    logger.exception(f"got exception during handle request on subject: '{msg.subject}'")
    if isinstance(e, ValidationError):
        return {"errors": jsonable_encoder(e.errors(), exclude={"input", "url"})}

    if capture_exception:
        capture_exception(e)

    tb = e.__traceback__
    while tb.tb_next:
        tb = tb.tb_next
    kwargs = {
        "detail": str(e),
        "func": tb.tb_frame.f_code.co_name,
        "file": tb.tb_frame.f_code.co_filename.split("/")[-1],
        # "line": str(tb.tb_lineno),
    }

    return kwargs
