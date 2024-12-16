import uuid
from typing import Any

import ujson
from pydantic import BaseModel


def to_bytes(data: Any) -> bytes:
    if isinstance(data, bytes):
        return data
    return ujson.dumps(data).encode("utf-8")


def from_bytes(data: bytes) -> Any:
    return ujson.loads(data.decode("utf-8")) if data else {}


def normalize_payload(data: Any) -> Any:
    if isinstance(data, bytes):
        return data
    if isinstance(data, BaseModel):
        return data.model_dump(mode="json", exclude_none=True)
    if isinstance(data, (dict,)):
        return {k: normalize_payload(v) for k, v in data.items() if v is not None}
    if isinstance(data, (list, tuple)):
        return [normalize_payload(r) for r in data]
    if isinstance(data, uuid.UUID):
        return str(data)
    return data
