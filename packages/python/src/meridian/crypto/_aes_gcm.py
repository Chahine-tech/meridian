"""AES-GCM-256 envelope encryption for CRDT values.

The encrypted sentinel format matches the TypeScript SDK:
    {"$e": 1, "n": "<base64url-nonce>", "d": "<base64url-ciphertext+tag>"}
"""

from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError as exc:
    raise ImportError("Install meridian-crdt[crypto] for encryption support") from exc


def _b64u_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64u_decode(s: str) -> bytes:
    pad = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * (pad % 4))


@dataclass
class AesGcmKey:
    _raw: bytes

    def encrypt(self, value: object) -> dict:
        nonce = os.urandom(12)
        plaintext = json.dumps(value, separators=(",", ":")).encode()
        ciphertext = AESGCM(self._raw).encrypt(nonce, plaintext, None)
        return {"$e": 1, "n": _b64u_encode(nonce), "d": _b64u_encode(ciphertext)}

    def decrypt(self, envelope: dict) -> object:
        nonce = _b64u_decode(envelope["n"])
        ciphertext = _b64u_decode(envelope["d"])
        plaintext = AESGCM(self._raw).decrypt(nonce, ciphertext, None)
        return json.loads(plaintext)

    def export_base64(self) -> str:
        return base64.b64encode(self._raw).decode()

    @staticmethod
    def is_encrypted(value: object) -> bool:
        return isinstance(value, dict) and value.get("$e") == 1 and "n" in value and "d" in value


def generate_key() -> AesGcmKey:
    return AesGcmKey(os.urandom(32))


def import_key(raw_base64: str) -> AesGcmKey:
    return AesGcmKey(base64.b64decode(raw_base64))
