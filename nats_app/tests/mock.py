import asyncio
import random
import string
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import ParseResult, urlparse

import nats.js
from nats.aio.client import DEFAULT_INBOX_PREFIX
from nats.aio.msg import Msg
from nats.js.api import (
    INBOX_PREFIX,
    AccountInfo,
    AccountLimits,
    APIStats,
    ConsumerConfig,
    DeliverPolicy,
    PubAck,
    StreamConfig,
    StreamInfo,
    StreamState,
)
from nats.js.client import (
    DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
    Callback,
)
from nats.nuid import NUID

from nats_app.meta import PullSubscriptionMeta, SubscriptionMeta

_characters = string.ascii_letters + string.digits


def _get_random_id(k) -> str:
    return "".join(random.choices(_characters, k=k))


class MockSubscriber:
    def __init__(self, client, subject, queue):
        self.client = client
        self.subject = subject
        self.subscriber_id = _get_random_id(5)

        group_subscribers = client._subscribers.get(subject) or {}
        subscribers = group_subscribers.get(queue, {})
        subscriber_id = _get_random_id(5)
        subscribers[subscriber_id] = self
        group_subscribers[queue] = subscribers

        self.client._subscribers[subject] = group_subscribers
        self._stream_data = []

    async def unsubscribe(self):
        self.client._subscribers[self.subject].pop(self.subscriber_id, None)

    async def consumer_info(self):
        @dataclass
        class ConsumerInfo:
            name: str
            stream_name: str

        return ConsumerInfo(name=self.subject, stream_name=self.subject)

    def _get_collect_data(self):
        async def _handler(msg: Msg):
            self._stream_data.append(msg)

        return _handler

    async def fetch(
        self,
        batch: int = 1,
        timeout: Optional[float] = 5,
        heartbeat: Optional[float] = None,
    ) -> list[Msg]:
        return self._stream_data


class MockJetStreamContext(nats.js.JetStreamContext):
    _stream = {}
    _subscribers = {}

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._stream = {}
        self._subscribers = {}

    async def account_info(self) -> AccountInfo:
        return AccountInfo(
            memory=0,
            storage=0,
            streams=0,
            consumers=0,
            limits=AccountLimits(
                max_memory=0,
                max_storage=0,
                max_streams=0,
                max_consumers=0,
                max_ack_pending=0,
                memory_max_stream_bytes=0,
                storage_max_stream_bytes=0,
                max_bytes_required=False,
            ),
            api=APIStats(
                total=0,
                errors=0,
            ),
        )

    async def stream_info(self, name: str, subjects_filter: Optional[str] = None) -> StreamInfo:
        return StreamInfo(
            config=StreamConfig(name=name, subjects=[subjects_filter]),
            state=StreamState(messages=1, bytes=1, first_seq=1, last_seq=1, consumer_count=1),
        )

    async def add_stream(self, config: Optional[StreamConfig] = None, **params) -> StreamInfo:
        return StreamInfo(
            config=StreamConfig(),
            state=StreamState(messages=1, bytes=1, first_seq=1, last_seq=1, consumer_count=1),
        )

    async def update_stream(self, config: Optional[StreamConfig] = None, **params) -> StreamInfo:
        return StreamInfo(
            config=StreamConfig(),
            state=StreamState(messages=1, bytes=1, first_seq=1, last_seq=1, consumer_count=1),
        )

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb: Optional[Callback] = None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[ConsumerConfig] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        idle_heartbeat: Optional[float] = None,
        flow_control: bool = False,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        deliver_policy: Optional[DeliverPolicy] = None,
        headers_only: Optional[bool] = None,
        inactive_threshold: Optional[float] = None,
    ):
        assert cb is not None, "test sync push push_subscribe not provided"

        self._stream = self._stream or defaultdict(lambda: defaultdict(list))
        self._stream[subject][queue].append(cb)

        # stream = self._stream.get(subject) or defaultdict(defaultdict(list))
        # stream_group = stream.get(queue) or {}
        # stream_group[queue] = cb
        # stream[subject] = stream_group
        # self._stream = stream

        return MockSubscriber(self, subject, queue)

    async def pull_subscribe(
        self,
        subject: str,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[ConsumerConfig] = None,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        inbox_prefix: bytes = INBOX_PREFIX,
    ):
        ms = MockSubscriber(self, subject, "")
        await self.subscribe(subject, queue="", cb=ms._get_collect_data())
        return ms

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> PubAck:
        groups = self._stream.get(subject, {})
        ret = None
        for name, cbs in groups.items():
            if not name:
                for cb in cbs:
                    ret = await self._call_cb(cb, subject, payload, headers)
            else:
                cb = random.choice(cbs)
                ret = await self._call_cb(cb, subject, payload, headers)
        return ret

    async def _call_cb(self, cb, subject, payload, headers):
        new_inbox = self._nc.new_inbox()

        async def _reply(m):
            pass

        sub_response = await self._nc.subscribe(new_inbox, cb=_reply)
        try:
            m = Msg(
                subject=subject,
                data=payload,
                headers=headers,
                reply=new_inbox,
                _client=self,
            )
            await cb(m)
        finally:
            await sub_response.unsubscribe()

        return PubAck.from_response({"stream": subject, "seq": 1})

    async def publish_async(
        self,
        subject: str,
        payload: bytes = b"",
        wait_stall: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> asyncio.Future[PubAck]:
        raise NotImplementedError()
        # async def _ret() -> PubAck:
        #     return PubAck.from_response({})
        # return asyncio.create_task(_ret())


class MockClient:
    _connected = False
    _subscribers = {}
    """
    _subscribers = {
        "subject1": {
            "": [cb1, cb2, ...],
            "queue1": [cb3, cb4, ...]
        },
        "subject2": {
            "": [cb5, ...],
        }
    }
    """

    def __init__(self, *args, **kwrgs):
        self._subscribers = {}
        self._connected = False
        self._inbox_prefix = bytearray(DEFAULT_INBOX_PREFIX)
        self._nuid = NUID()
        self._js_pull_subscribers = []

    async def connect(self, **options) -> None:
        servers = options.get("servers", "mock")
        if not isinstance(servers, list):
            servers = servers.split(",")
        self._current_server = servers[0]
        self._connected = True

    @property
    def connected_url(self) -> Optional[ParseResult]:
        if self._current_server and self.is_connected:
            return urlparse(self._current_server)
        return None

    async def drain(self) -> None:
        pass

    async def flush(self, timeout: int = 2) -> None:
        pass

    async def close(self) -> None:
        self._connected = False

    def new_inbox(self) -> str:
        next_inbox = self._inbox_prefix[:]
        next_inbox.extend(b".")
        next_inbox.extend(self._nuid.next())
        return next_inbox.decode()

    def _get_rpc(self, subject):
        try:
            return list(list(self._subscribers[subject].values())[0].values())[0]
        except (KeyError, AttributeError) as e:
            raise ValueError("no callee registered for procedure") from e

    async def request(
        self,
        subject: str,
        payload: bytes = b"",
        headers: Optional[dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> Msg:
        rpc_func = self._get_rpc(subject)
        new_inbox = self.new_inbox()

        result = None

        async def _reply(m):
            nonlocal result
            result = m

        sub_response = await self.subscribe(new_inbox, cb=_reply)
        try:
            m = Msg(
                subject=subject,
                data=payload,
                headers=headers,
                reply=new_inbox,
                _client=self,
            )
            await rpc_func.handler(m)
        finally:
            await sub_response.unsubscribe()
        return result

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb=None,
        future=None,
        max_msgs=0,
        pending_msgs_limit=10,
        pending_bytes_limit=1000,
    ):
        group_subscribers = self._subscribers.get(subject) or {}
        subscribers = group_subscribers.get(queue, {})

        subscriber_id = _get_random_id(5)
        subscribers[subscriber_id] = SubscriptionMeta(
            handler=cb,
            subject=subject,
            queue=queue,
            future=future,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        group_subscribers[queue] = subscribers
        self._subscribers[subject] = group_subscribers

        class Subscriber:
            async def unsubscribe(s_self):
                self._subscribers[subject].pop(subscriber_id, None)

        return Subscriber()

    async def publish(
        self,
        subject,
        payload: bytes = b"",
        headers: Optional[dict[str, str]] = None,
        *args,
        **kwargs,
    ) -> None:
        if subject not in self._subscribers:
            # raise ValueError(f"{subject} is not subscribed")
            return

        async def _publish(cb, subject, payload, headers):
            m = Msg(subject=subject, data=payload, headers=headers, _client=self)
            await cb(m)

        for queue, group in self._subscribers[subject].items():
            if not queue:
                for reg in group.values():
                    await _publish(reg.handler, subject, payload, headers)
            else:
                reg = random.choice(list(group.values()))
                await _publish(reg.handler, subject, payload, headers)
        return

    @property
    def last_error(self) -> Optional[Exception]:
        return None

    @property
    def pending_data_size(self) -> int:
        return 1000

    @property
    def is_closed(self) -> bool:
        return not self._connected

    @property
    def is_reconnecting(self) -> bool:
        return False

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def is_connecting(self) -> bool:
        return False

    @property
    def is_draining(self) -> bool:
        return False

    @property
    def is_draining_pubs(self) -> bool:
        return False

    def jetstream(self, **opts) -> MockJetStreamContext:
        return MockJetStreamContext(self, **opts)

    async def push_from_pull_subscribe(self, js, r: PullSubscriptionMeta):
        async def _handler(m: Msg):
            await r.handler([m])

        sub = await js.subscribe(r.subject, queue=r.durable, cb=_handler)
        self._js_pull_subscribers.append(sub)

    async def _js_pull_subscriber_stop(self):
        for sub in self._js_pull_subscribers:
            await sub.unsubsctibe()
