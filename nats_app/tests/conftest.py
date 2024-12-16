from unittest.mock import patch

import pytest

import nats_app as ncli

from .mock import MockClient


@pytest.fixture
async def nc():
    with patch("nats_app.app.NATS", new=MockClient):
        _nats = ncli.NATSApp(url=["nats://127.0.0.1:4220"])
        yield _nats
