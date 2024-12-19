import pytest
from nats.js import JetStreamContext
from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError

from nats_app.app import NATSApp


@pytest.mark.parametrize(
    "exist, new, fields, expected",
    [
        ({"a": 1, "b": 2}, {"a": 1, "b": 2}, ["a", "b"], True),
        ({"a": 1, "b": 2}, {"a": 1, "b": 3}, ["a", "b"], False),
        ({"a": 1, "b": 2}, {"a": 1, "b": None}, ["a", "b"], True),
        ({"a": 1, "b": 2}, {"a": 1}, ["a", "b"], True),
        ({"a": 1, "b": 2}, {"a": 2, "b": 2}, ["a", "b"], False),
    ],
)
def test_is_equal(exist, new, fields, expected):
    assert NATSApp._is_equal(exist, new, fields) == expected


@pytest.mark.parametrize(
    "exist, new, fields, expected",
    [
        (
            {"a": 1, "b": 2},
            {"a": 1, "b": 3},
            ["a", "b"],
            {"b": {"old": 2, "new": 3}},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 1, "b": None},
            ["a", "b"],
            {},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 2, "b": 2},
            ["a", "b"],
            {"a": {"old": 1, "new": 2}},
        ),
        (
            {"a": 1, "b": 2},
            {"a": 1, "b": 2, "c": 3},
            ["a", "b", "c"],
            {"c": {"old": None, "new": 3}},
        ),
    ],
)
def test_get_change_dict(exist, new, fields, expected):
    assert NATSApp._get_change_dict(exist, new, fields) == expected


@pytest.mark.asyncio
async def test_streams_create_or_update_add_stream(mocker):
    nc = NATSApp(url=["nats://localhost:4222"])
    config = StreamConfig(name="test_stream")
    nc._jetstream_configs.append(config)

    mock_js = mocker.Mock(spec=JetStreamContext)
    mock_js.stream_info.side_effect = NotFoundError
    mock_js.add_stream.return_value = mocker.Mock(as_dict=lambda: {"name": "test_stream"})
    nc._js = mock_js

    await nc._streams_create_or_update()

    mock_js.add_stream.assert_called_once_with(config)
    mock_js.stream_info.assert_called_once_with("test_stream")


@pytest.mark.asyncio
async def test_streams_create_or_update_no_change(mocker):
    nc = NATSApp(url=["nats://localhost:4222"])
    config = StreamConfig(name="test_stream", subjects=["foo"])
    nc._jetstream_configs.append(config)

    mock_js = mocker.Mock(spec=JetStreamContext)
    mock_js.stream_info.return_value = mocker.Mock(config=config)
    nc._js = mock_js

    mocker.patch.object(NATSApp, "_is_equal", return_value=True)

    await nc._streams_create_or_update()

    mock_js.update_stream.assert_not_called()
    mock_js.add_stream.assert_not_called()
    mock_js.stream_info.assert_called_once_with("test_stream")


@pytest.mark.asyncio
async def test_streams_create_or_update_deny_change(mocker):
    nc = NATSApp(url=["nats://localhost:4222"])
    config = StreamConfig(name="test_stream", subjects=["foo"])
    nc._jetstream_configs.append(config)

    mock_js = mocker.Mock(spec=JetStreamContext)
    mock_js.stream_info.return_value = mocker.Mock(config=config)
    nc._js = mock_js

    mocker.patch.object(NATSApp, "_is_equal", side_effect=[False, True])
    mocker.patch.object(NATSApp, "_get_change_dict", return_value={"name": {"old": "test_stream", "new": "new_stream"}})

    with pytest.raises(ValueError, match="nats: stream config params .* deny change"):
        await nc._streams_create_or_update()

    mock_js.update_stream.assert_not_called()
    mock_js.add_stream.assert_not_called()
    mock_js.stream_info.assert_called_once_with("test_stream")
