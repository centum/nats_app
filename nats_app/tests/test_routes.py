import pytest
from nats.js import JetStreamContext
from nats.js.api import StreamConfig
from nats.js.errors import NotFoundError

from nats_app.app import NATSApp
from nats_app.router import NATSRouter


@pytest.mark.parametrize(
    "prefix, subsubject, expected",
    [
        pytest.param("", "subject", "subject", id="subject"),
        pytest.param("app", "subject", "app.subject", id="app.subject"),
        pytest.param("app.", "subject", "app.subject", id="app.subject w suffix dot"),
        pytest.param("app.", ".subject", "app.subject", id="app.subject w suffix and prefix dot"),
        pytest.param("app.", ".subject.", "app.subject", id="app.subject w suffix and prefix dot in subroute"),
        pytest.param(".app.", ".subject.", "app.subject", id="app.subject w prefix dot"),
        pytest.param("app.v1", ".route.subject.", "app.v1.route.subject", id="app.v1.route.subject"),
    ],
)
def test_routes_subject(prefix: str, subsubject: str, expected: str):
    assert NATSRouter(prefix=prefix)._add_subject(subsubject) == expected
