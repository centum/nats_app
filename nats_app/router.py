import asyncio
from typing import Optional

from nats.aio import subscription
from nats.js import api, client

from nats_app.meta import PullSubscriptionMeta, PushSubscriptionMeta, SubscriptionMeta


class NATSRouter:
    push_subscribers: list[SubscriptionMeta]
    js_push_subscribers: list[PushSubscriptionMeta]
    js_pull_subscribers: list[PullSubscriptionMeta]
    prefix: str = ""

    def __init__(self, prefix: str = ""):
        self.push_subscribers = []
        self.js_push_subscribers = []
        self.js_pull_subscribers = []
        prefix = prefix or ""
        self.prefix = prefix.strip().strip(".")

    def _add_subject(self, subject: str):
        subject = subject.strip().strip(".")
        return self.prefix + "." + subject if self.prefix else subject

    def push_subscribe(
        self,
        subject,
        queue: str = "",
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = subscription.DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = subscription.DEFAULT_SUB_PENDING_BYTES_LIMIT,
        by_pass_msg: Optional[str] = False,
    ):
        def wrapper(fn):
            self.push_subscribers.append(
                SubscriptionMeta(
                    subject=self._add_subject(subject),
                    queue=queue,
                    handler=fn,
                    future=future,
                    max_msgs=max_msgs,
                    pending_msgs_limit=pending_msgs_limit,
                    pending_bytes_limit=pending_bytes_limit,
                )
            )

        return wrapper

    def js_push_subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        idle_heartbeat: Optional[float] = None,
        flow_control: bool = False,
        pending_msgs_limit: int = client.DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = client.DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        deliver_policy: Optional[api.DeliverPolicy] = None,
        headers_only: Optional[bool] = None,
        inactive_threshold: Optional[float] = None,
    ):
        def wrapper(fn):
            self.js_push_subscribers.append(
                PushSubscriptionMeta(
                    subject=self._add_subject(subject),
                    queue=queue,
                    cb=fn,
                    durable=durable,
                    stream=stream,
                    config=config,
                    manual_ack=manual_ack,
                    ordered_consumer=ordered_consumer,
                    idle_heartbeat=idle_heartbeat,
                    flow_control=flow_control,
                    pending_msgs_limit=pending_msgs_limit,
                    pending_bytes_limit=pending_bytes_limit,
                    deliver_policy=deliver_policy,
                    headers_only=headers_only,
                    inactive_threshold=inactive_threshold,
                )
            )

        return wrapper

    def js_pull_subscribe(
        self,
        subject: str,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        pending_msgs_limit: int = client.DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = client.DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        batch: int = 1,
        timeout: Optional[float] = None,
        heartbeat: Optional[float] = None,
    ):
        def wrapper(fn):
            self.js_pull_subscribers.append(
                PullSubscriptionMeta(
                    handler=fn,
                    subject=self._add_subject(subject),
                    durable=durable,
                    stream=stream,
                    config=config,
                    pending_msgs_limit=pending_msgs_limit,
                    pending_bytes_limit=pending_bytes_limit,
                    inbox_prefix=inbox_prefix,
                    batch=batch,
                    timeout=timeout,
                    heartbeat=heartbeat,
                )
            )

        return wrapper

    def include_router(self, router):
        for m in router.push_subscribers:
            self.push_subscribers.append(
                SubscriptionMeta(
                    subject=self._add_subject(m.subject),
                    queue=m.queue,
                    handler=m.handler,
                    future=m.future,
                    max_msgs=m.max_msgs,
                    pending_msgs_limit=m.pending_msgs_limit,
                    pending_bytes_limit=m.pending_bytes_limit,
                )
            )

        for m in router.js_push_subscribers:
            self.js_push_subscribers.append(
                PushSubscriptionMeta(
                    subject=self._add_subject(m.subject),
                    queue=m.queue,
                    cb=m.cb,
                    durable=m.durable,
                    stream=m.stream,
                    config=m.config,
                    manual_ack=m.manual_ack,
                    ordered_consumer=m.ordered_consumer,
                    idle_heartbeat=m.idle_heartbeat,
                    flow_control=m.flow_control,
                    pending_msgs_limit=m.pending_msgs_limit,
                    pending_bytes_limit=m.pending_bytes_limit,
                    deliver_policy=m.deliver_policy,
                    headers_only=m.headers_only,
                    inactive_threshold=m.inactive_threshold,
                )
            )

        for m in router.js_pull_subscribers:
            self.js_pull_subscribers.append(
                PullSubscriptionMeta(
                    subject=self._add_subject(m.subject),
                    handler=m.handler,
                    durable=m.durable,
                    stream=m.stream,
                    config=m.config,
                    pending_msgs_limit=m.pending_msgs_limit,
                    pending_bytes_limit=m.pending_bytes_limit,
                    inbox_prefix=m.inbox_prefix,
                    batch=m.batch,
                    timeout=m.timeout,
                    heartbeat=m.heartbeat,
                )
            )
