"""Ed25519 keypair for BFT op signing.

The public key bytes are embedded in the token claims (server-side).
Every outgoing op is signed over ``op_bytes``; the server verifies the signature.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from pathlib import Path

try:
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PrivateKey,
        Ed25519PublicKey,
    )
    from cryptography.hazmat.primitives.serialization import (
        Encoding,
        NoEncryption,
        PrivateFormat,
        PublicFormat,
    )
except ImportError as exc:
    raise ImportError("Install meridian-sdk[crypto] for signing support") from exc


@dataclass
class ClientKeypair:
    _private: Ed25519PrivateKey
    _public: Ed25519PublicKey
    public_key_bytes: bytes  # 32-byte raw Ed25519 public key

    def sign(self, op_bytes: bytes) -> bytes:
        return self._private.sign(op_bytes)

    def public_key_base64url(self) -> str:
        return base64.urlsafe_b64encode(self.public_key_bytes).rstrip(b"=").decode()


def generate_keypair() -> ClientKeypair:
    priv = Ed25519PrivateKey.generate()
    pub = priv.public_key()
    raw_pub = pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
    return ClientKeypair(_private=priv, _public=pub, public_key_bytes=raw_pub)


def load_or_generate_keypair(storage_path: Path | None = None) -> ClientKeypair:
    """Load a persisted keypair from disk, or generate and save a new one.

    Defaults to ``~/.meridian/keypair.json``.
    """
    path = storage_path or Path.home() / ".meridian" / "keypair.json"
    if path.exists():
        data = json.loads(path.read_text())
        raw_priv = base64.b64decode(data["private_key"])
        priv = Ed25519PrivateKey.from_private_bytes(raw_priv)
        pub = priv.public_key()
        raw_pub = pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
        return ClientKeypair(_private=priv, _public=pub, public_key_bytes=raw_pub)

    kp = generate_keypair()
    path.parent.mkdir(parents=True, exist_ok=True)
    raw_priv = kp._private.private_bytes(Encoding.Raw, PrivateFormat.Raw, NoEncryption())
    path.write_text(json.dumps({"private_key": base64.b64encode(raw_priv).decode()}))
    return kp


def sign_op(keypair: ClientKeypair, op_bytes: bytes) -> bytes:
    return keypair.sign(op_bytes)
