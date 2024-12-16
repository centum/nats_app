import asyncio
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Callable, Optional

from nats.aio import subscription
from nats.aio.client import Callback
from nats.js import api, client


@dataclass
class SubscriptionMeta:
    subject: str
    queue: str = ""
    handler: Optional[Callable[[Any], Awaitable[None]]] = None
    future: Optional[asyncio.Future] = None
    max_msgs: int = 0
    pending_msgs_limit: int = subscription.DEFAULT_SUB_PENDING_MSGS_LIMIT
    pending_bytes_limit: int = subscription.DEFAULT_SUB_PENDING_BYTES_LIMIT


@dataclass
class PushSubscriptionMeta:
    subject: str
    queue: Optional[str] = None
    cb: Optional[Callback] = None
    durable: Optional[str] = None
    stream: Optional[str] = None
    config: Optional[api.ConsumerConfig] = None
    manual_ack: bool = False
    ordered_consumer: bool = False
    idle_heartbeat: Optional[float] = None
    flow_control: bool = False
    pending_msgs_limit: int = client.DEFAULT_JS_SUB_PENDING_MSGS_LIMIT
    pending_bytes_limit: int = client.DEFAULT_JS_SUB_PENDING_BYTES_LIMIT
    deliver_policy: Optional[api.DeliverPolicy] = None
    headers_only: Optional[bool] = None
    inactive_threshold: Optional[float] = None


@dataclass
class PullSubscriptionMeta:
    handler: Callback
    subject: str
    durable: Optional[str] = None
    stream: Optional[str] = None
    config: Optional[api.ConsumerConfig] = None
    pending_msgs_limit: int = client.DEFAULT_JS_SUB_PENDING_MSGS_LIMIT
    pending_bytes_limit: int = client.DEFAULT_JS_SUB_PENDING_BYTES_LIMIT
    inbox_prefix: bytes = api.INBOX_PREFIX
    # fetch
    batch: int = (1,)
    timeout: Optional[float] = (None,)
    heartbeat: Optional[float] = None
