import logging
from datetime import datetime
from functools import wraps

from nats.aio.msg import Msg

logger = logging.getLogger(__name__)


def log_request(next_handler):
    @wraps(next_handler)
    async def handler(msg: Msg):
        start = datetime.now()
        result = None
        try:
            result = await next_handler(msg)
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            logger.info(f"NATS RPC: {msg.subject}({msg.data}) -> {result} took {datetime.now() - start}")
        return result

    return handler
