import asyncio
from collections import defaultdict
from typing import Any, Optional

import pytest
from nats.aio.msg import Msg
from nats.js.api import StorageType, StreamConfig

from nats_app.app import NATSApp
from nats_app.middlewares.validation import validate_args
from nats_app.tasks_queue import TaskQueue


async def test_rpc_call(nc):
    @nc.push_subscribe("app.subject.echo", queue="worker")
    async def rpc_func_echo(m: Msg) -> str:
        return f"Response: {m.data}"

    await nc.connect()

    res = await nc.request("app.subject.echo", "test")
    assert isinstance(res, str)
    assert res == "Response: b'\"test\"'"


async def test_rpc_call_w_validate(nc):
    @nc.push_subscribe("app.subject.validate_echo", queue="worker")
    @validate_args(by_pass_msg="msg")
    async def rpc_func_echo(data: str, msg: Any) -> str:
        return f"Response: {data} Msg.data: {msg.data}"

    await nc.connect()

    res = await nc.request("app.subject.validate_echo", {"args": ["test"]})
    assert isinstance(res, str)
    assert res == 'Response: test Msg.data: b\'{"args":["test"]}\''


async def test_rpc_call_w_validate_wo_param(nc):
    @nc.push_subscribe("app.subject.validate_echo", queue="worker")
    @validate_args
    async def rpc_func_echo(data: str) -> str:
        return f"Response: {data}"

    await nc.connect()

    res = await nc.request("app.subject.validate_echo", {"args": ["test"]})
    assert isinstance(res, str)
    assert res == "Response: test"


async def test_rpc_call_invalid_args(nc):
    @nc.push_subscribe("app.subject.validate_echo", queue="worker")
    @validate_args(by_pass_msg="msg")
    async def rpc_func_echo(data: str, val: int, key: bool, var1: Optional[float], var2: float, msg: Any) -> str:
        return f"Response: {data} Msg.data: {msg.data}"

    await nc.connect()

    res = await nc.request(
        "app.subject.validate_echo",
        {"args": ["test", "wow"], "kwargs": {"var1": "oto", "var2": 3.14}},
    )
    assert isinstance(res, dict)
    assert res == {
        "errors": [
            {
                "loc": [1],
                "msg": "Input should be a valid integer, unable to parse string as an integer",
                "type": "int_parsing",
            },
            {
                "loc": ["key"],
                "msg": "Missing required argument",
                "type": "missing_argument",
            },
            {
                "loc": ["var1"],
                "msg": "Input should be a valid number, unable to parse string as a number",
                "type": "float_parsing",
            },
        ]
    }


async def test_publish_w_validate(nc):
    result = {}

    @nc.push_subscribe("app.subject.pub")
    @validate_args()
    async def rpc_func_echo(data: str, data1: int) -> None:
        result["data"] = data
        result["data1"] = data1

    await nc.connect()

    await nc.publish("app.subject.pub", {"args": ["test"], "kwargs": {"data1": 1}})
    assert result["data"] == "test"
    assert result["data1"] == 1


async def test_publish_wo_validate(nc):
    result = {}

    @nc.push_subscribe("app.subject.pub_raw")
    async def rpc_func_echo(m: Msg) -> None:
        result["data"] = m.data

    await nc.connect()

    await nc.publish("app.subject.pub_raw", {"args": ["test"], "kwargs": {"data1": 1}})
    assert result["data"] == b'{"args":["test"],"kwargs":{"data1":1}}'


async def test_publish_w_queue(nc):
    c = defaultdict(int)
    result = {}

    @nc.push_subscribe("app.subject.pub_raw", queue="worker")
    async def rpc_func_echo(m: Msg) -> None:
        c["call"] += 1
        result["data"] = m.data

    @nc.push_subscribe("app.subject.pub_raw", queue="worker")
    async def rpc_func_echo_v2(m: Msg) -> None:
        c["call"] += 1
        result["data"] = m.data

    await nc.connect()

    await nc.publish("app.subject.pub_raw", {"args": ["test"], "kwargs": {"data1": 1}})
    assert result["data"] == b'{"args":["test"],"kwargs":{"data1":1}}'
    assert c["call"] == 1


async def test_publish_wo_queue(nc):
    c = defaultdict(int)
    result = {}

    @nc.push_subscribe("app.subject.pub_raw")
    async def rpc_func_echo(m: Msg) -> None:
        c["call"] += 1
        result["data"] = m.data

    @nc.push_subscribe("app.subject.pub_raw")
    async def rpc_func_echo_v2(m: Msg) -> None:
        c["call"] += 1
        result["data"] = m.data

    await nc.connect()

    await nc.publish("app.subject.pub_raw", {"args": ["test"], "kwargs": {"data1": 1}})
    assert result["data"] == b'{"args":["test"],"kwargs":{"data1":1}}'
    assert c["call"] == 2


async def test_push_subscription(nc):
    result = None

    @nc.js_push_subscribe("app.subject.validate_echo", queue="worker")
    async def handler(msg: Msg):
        nonlocal result
        result = msg.data
        await msg.ack()

    await nc.connect()

    await nc.js.publish("app.subject.validate_echo", b"TEST123")
    assert isinstance(result, bytes)
    assert result == b"TEST123"


async def test_pull_subscription(nc):
    result = None

    @nc.js_pull_subscribe("app.subject.validate_echo", batch=1)
    async def handler(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result
        result = msgs[0].data

    await nc.connect()
    await nc.js.publish("app.subject.validate_echo", b"TEST123")
    assert isinstance(result, bytes)
    assert result == b"TEST123"


async def test_pull_subscription_multihandlers_wo_group(nc):
    result1 = None
    result2 = None

    @nc.js_pull_subscribe("app.subject.validate_echo", batch=1)
    async def handler(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result1
        result1 = msgs[0].data

    @nc.js_pull_subscribe("app.subject.validate_echo", batch=1)
    async def handler2(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result2
        result2 = msgs[0].data

    await nc.connect()
    await nc.js.publish("app.subject.validate_echo", b"TEST123")
    assert isinstance(result1, bytes)
    assert result1 == b"TEST123"
    assert isinstance(result2, bytes)
    assert result2 == b"TEST123"


async def test_pull_subscription_multihandlers_w_one_group(nc):
    result1 = None
    result2 = None

    @nc.js_pull_subscribe("app.subject.validate_echo", durable="group1", batch=1)
    async def handler(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result1
        result1 = msgs[0].data

    @nc.js_pull_subscribe("app.subject.validate_echo", durable="group1", batch=1)
    async def handler2(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result2
        result2 = msgs[0].data

    await nc.connect()
    await nc.js.publish("app.subject.validate_echo", b"TEST123")
    assert (result1 is None and result2 is not None) or (result1 is not None and result2 is None)
    if result1 is not None:
        assert isinstance(result1, bytes)
        assert result1 == b"TEST123"
    else:
        assert result1 is None
        assert isinstance(result2, bytes)
        assert result2 == b"TEST123"


async def test_pull_subscription_multihandlers_w_different_group(nc):
    result1 = None
    result2 = None

    @nc.js_pull_subscribe("app.subject.validate_echo", durable="group1", batch=1)
    async def handler(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result1
        result1 = msgs[0].data

    @nc.js_pull_subscribe("app.subject.validate_echo", durable="group2", batch=1)
    async def handler2(msgs: list[Msg]):
        assert isinstance(msgs, list)
        assert len(msgs) == 1
        nonlocal result2
        result2 = msgs[0].data

    await nc.connect()
    await nc.js.publish("app.subject.validate_echo", b"TEST123")
    assert isinstance(result1, bytes)
    assert result1 == b"TEST123"
    assert isinstance(result2, bytes)
    assert result2 == b"TEST123"


@pytest.mark.asyncio
async def test_create_queue_basic():
    app = NATSApp()
    subjects = ["subject1", "subject2"]
    queue = app.create_queue(subjects=subjects)

    assert isinstance(queue, TaskQueue)
    assert queue.subjects == subjects
    assert queue.stream_name is None
    assert queue.storage == StorageType.FILE
    assert queue.durable is None
    assert queue in app._task_queues


@pytest.mark.asyncio
async def test_create_queue_with_stream_name():
    app = NATSApp()
    subjects = ["subject1"]
    stream_name = "test_stream"
    queue = app.create_queue(subjects=subjects, stream_name=stream_name)

    assert isinstance(queue, TaskQueue)
    assert queue.subjects == subjects
    assert queue.stream_name == stream_name
    assert queue.storage == StorageType.FILE
    assert queue.durable is None
    assert queue in app._task_queues


@pytest.mark.asyncio
async def test_create_queue_with_custom_storage():
    app = NATSApp()
    subjects = ["subject1"]
    storage = StorageType.MEMORY
    queue = app.create_queue(subjects=subjects, storage=storage)

    assert isinstance(queue, TaskQueue)
    assert queue.subjects == subjects
    assert queue.stream_name is None
    assert queue.storage == storage
    assert queue.durable is None
    assert queue in app._task_queues


@pytest.mark.asyncio
async def test_create_queue_with_stream_config():
    app = NATSApp()
    subjects = ["subject1"]
    stream_config = StreamConfig(name="test_stream", subjects=["subject2"], storage=StorageType.FILE)
    queue = app.create_queue(subjects=subjects, stream_config=stream_config)

    assert isinstance(queue, TaskQueue)
    assert queue.subjects == stream_config.subjects
    assert queue.stream_name is stream_config.name
    assert queue.storage == StorageType.FILE
    assert queue.stream_config == stream_config
    assert queue.durable is None
    assert queue in app._task_queues


@pytest.mark.asyncio
async def test_create_queue_with_durable():
    app = NATSApp()
    subjects = ["subject1"]
    durable = "durable_name"
    queue = app.create_queue(subjects=subjects, durable=durable)

    assert isinstance(queue, TaskQueue)
    assert queue.subjects == subjects
    assert queue.stream_name is None
    assert queue.storage == StorageType.FILE
    assert queue.durable == durable
    assert queue in app._task_queues


@pytest.mark.asyncio
def test_register_task_queue_single():
    app = NATSApp()
    task_queue = TaskQueue(subjects=["subject1"])

    app.register_task_queue(task_queue)

    assert len(app._task_queues) == 1
    assert app._task_queues[0] == task_queue


@pytest.mark.asyncio
def test_register_task_queue_multiple():
    app = NATSApp()
    task_queue1 = TaskQueue(subjects=["subject1"])
    task_queue2 = TaskQueue(subjects=["subject2"])

    app.register_task_queue(task_queue1, task_queue2)

    assert len(app._task_queues) == 2
    assert task_queue1 in app._task_queues
    assert task_queue2 in app._task_queues


@pytest.mark.asyncio
def test_register_task_queue_no_duplicates():
    app = NATSApp()
    task_queue = TaskQueue(subjects=["subject1"])

    app.register_task_queue(task_queue)
    app.register_task_queue(task_queue)  # Register the same queue again

    assert len(app._task_queues) == 2  # Ensure duplicates are allowed
    assert app._task_queues.count(task_queue) == 2
