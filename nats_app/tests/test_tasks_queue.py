import json
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from nats.js.api import StorageType, StreamConfig

from nats_app.tasks_queue import MetaTask, TaskParams, TaskQueue, send_task_by_subject


@pytest.fixture
def mock_nc():
    """Fixture to mock the NATS connection."""
    nc = MagicMock()
    nc._jetstream_configs = []
    nc.js_pull_subscribe = Mock()
    nc.publish = AsyncMock()
    return nc


@pytest.fixture
def task_queue():
    """Fixture to create a TaskQueue instance."""
    return TaskQueue(
        subjects=["test.subject.*"],
        stream_name="test_stream",
        storage=StorageType.MEMORY,
    )


def test_task_queue_initialization():
    """Test TaskQueue initialization with default and custom configurations."""
    tq = TaskQueue(subjects=["test.subject.*"], stream_name="test_stream")
    assert tq.stream_name == "test_stream"
    assert tq.subjects == ["test.subject.*"]
    assert tq.storage == StorageType.FILE

    custom_stream_config = StreamConfig(name="custom_stream", subjects=["custom.subject"], storage=StorageType.MEMORY)
    tq_custom = TaskQueue(subjects=["custom.subject"], stream_config=custom_stream_config)
    assert tq_custom.stream_name == "custom_stream"
    assert tq_custom.subjects == ["custom.subject"]
    assert tq_custom.storage == StorageType.MEMORY


def test_default_subject(task_queue):
    """Test the default_subject property."""
    assert task_queue.default_subject == "test.subject.task"


def test_get_subject(task_queue):
    """Test the _get_subject method for valid and invalid subjects."""
    assert task_queue._get_subject("test.subject.task") == "test.subject.task"
    with pytest.raises(ValueError):
        task_queue._get_subject("invalid.subject")


@pytest.mark.asyncio
async def test_set_delay(task_queue, mock_nc):
    """Test the _set_delay method."""
    task_queue.bind(mock_nc)
    delay_fn = task_queue._set_delay("test_task", "test.subject.task")
    await delay_fn(1, key="value")
    mock_nc.publish.assert_called_once()
    args, kwargs = mock_nc.publish.call_args
    assert args[0] == "test.subject.task"
    meta = MetaTask(
        task="test_task",
        args=[1],
        kwargs=dict(key="value"),
    )
    assert json.dumps(meta.model_dump(mode="json")) in json.dumps(args[1])


def test_task_decorator(task_queue):
    """Test the task decorator for registering tasks."""

    @task_queue.task(subject="test.subject.task")
    async def sample_task(arg1, arg2):
        return arg1 + arg2

    task_name = "nats_app.tests.test_tasks_queue.test_task_decorator.<locals>.sample_task"

    assert task_name in task_queue._registered_tasks
    task_params = task_queue._registered_tasks[task_name]
    assert isinstance(task_params, TaskParams)
    assert task_params.subject == "test.subject.task"
    assert task_params.batch == 1


def test_bind(task_queue, mock_nc):
    """Test the bind method to attach tasks to a NATS connection."""

    @task_queue.task(subject="test.subject.task")
    async def sample_task(arg1, arg2):
        return arg1 + arg2

    task_queue.bind(mock_nc)
    assert mock_nc._jetstream_configs[0] == task_queue.stream_config
    mock_nc.js_pull_subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_single_message(task_queue, mock_nc):
    """Test the _subscribe_single_message method."""

    @task_queue.task(subject="test.subject.task")
    async def sample_task(arg1, arg2):
        return arg1 + arg2

    task_queue.bind(mock_nc)
    mock_nc.js_pull_subscribe.assert_called_once()


@pytest.mark.asyncio
async def test_subscribe_batch_messages(task_queue, mock_nc):
    """Test the _subscribe_on_jetstream method."""

    @task_queue.task(subject="test.subject.task", batch=10)
    async def sample_task(data):
        return sum(data)

    task_queue.bind(mock_nc)
    mock_nc.js_pull_subscribe.assert_called_once()

@pytest.mark.asyncio
async def test_send_task_by_subject_valid(mock_nc):
    """Test send_task_by_subject with valid data."""
    subject = "test.subject.task"
    mock_nc.publish = AsyncMock()

    await send_task_by_subject(mock_nc, subject, 1, key="value")
    mock_nc.publish.assert_called_once()
    args, kwargs = mock_nc.publish.call_args
    assert args[0] == subject
    assert "task" in args[1]
    assert "args" in args[1]
    assert "kwargs" in args[1]

@pytest.mark.asyncio
async def test_send_task_by_subject_invalid_subject(mock_nc):
    """Test send_task_by_subject with an invalid subject."""
    with pytest.raises(ValueError):
        await send_task_by_subject(mock_nc, "", 1, key="value")

@pytest.mark.asyncio
async def test_send_task_by_subject_error_handling(mock_nc):
    """Test send_task_by_subject handles publish errors."""
    subject = "test.subject.task"
    mock_nc.publish = AsyncMock(side_effect=Exception("Publish Error"))

    with pytest.raises(Exception, match="Publish Error"):
        await send_task_by_subject(mock_nc, subject, 1, key="value")
