from typing import Any

import msgpack


def encode(value: Any) -> bytes:
    return msgpack.packb(value, use_bin_type=True)


def decode(data: bytes) -> Any:
    return msgpack.unpackb(data, raw=False)


def encode_vector_clock(entries: dict[str, int]) -> bytes:
    return encode({"entries": entries})


def decode_vector_clock(data: bytes) -> dict[str, int]:
    raw = decode(data)
    return raw.get("entries", {}) if isinstance(raw, dict) else {}
