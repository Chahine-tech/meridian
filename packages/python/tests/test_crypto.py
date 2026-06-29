"""Tests for AES-GCM and Ed25519 crypto helpers."""

import pytest

pytest.importorskip("cryptography", reason="cryptography package required")

from meridian.crypto import (
    AesGcmKey,
    generate_key,
    import_key,
    generate_keypair,
    sign_op,
)


# ── AES-GCM ──────────────────────────────────────────────────────────────────

def test_encrypt_decrypt_roundtrip():
    key = generate_key()
    plaintext = {"name": "Alice", "score": 42}
    envelope = key.encrypt(plaintext)
    assert key.decrypt(envelope) == plaintext


def test_envelope_shape():
    key = generate_key()
    env = key.encrypt("hello")
    assert env["$e"] == 1
    assert isinstance(env["n"], str) and len(env["n"]) > 0
    assert isinstance(env["d"], str) and len(env["d"]) > 0


def test_nonce_is_unique():
    key = generate_key()
    a = key.encrypt("same")
    b = key.encrypt("same")
    assert a["n"] != b["n"]
    assert a["d"] != b["d"]


def test_wrong_key_raises():
    k1, k2 = generate_key(), generate_key()
    env = k1.encrypt("secret")
    with pytest.raises(Exception):
        k2.decrypt(env)


def test_import_key_roundtrip():
    key = generate_key()
    raw = key.export_base64()
    imported = import_key(raw)
    env = key.encrypt("test")
    assert imported.decrypt(env) == "test"


def test_is_encrypted_detects_envelope():
    key = generate_key()
    env = key.encrypt("x")
    assert AesGcmKey.is_encrypted(env)
    assert not AesGcmKey.is_encrypted("hello")
    assert not AesGcmKey.is_encrypted({"foo": "bar"})
    assert not AesGcmKey.is_encrypted({"$e": 1, "n": "abc"})  # missing d


# ── Ed25519 ──────────────────────────────────────────────────────────────────

def test_sign_produces_64_bytes():
    kp = generate_keypair()
    sig = sign_op(kp, b"op-payload")
    assert isinstance(sig, bytes)
    assert len(sig) == 64


def test_public_key_is_32_bytes():
    kp = generate_keypair()
    assert len(kp.public_key_bytes) == 32


def test_keypairs_are_unique():
    a, b = generate_keypair(), generate_keypair()
    assert a.public_key_bytes != b.public_key_bytes


def test_signature_verifies():
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

    kp = generate_keypair()
    msg = b"some op bytes"
    sig = sign_op(kp, msg)

    pub = Ed25519PublicKey.from_public_bytes(kp.public_key_bytes)
    pub.verify(sig, msg)  # raises if invalid


def test_signature_invalid_for_different_message():
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    kp = generate_keypair()
    sig = sign_op(kp, b"correct message")
    pub = Ed25519PublicKey.from_public_bytes(kp.public_key_bytes)
    with pytest.raises(InvalidSignature):
        pub.verify(sig, b"wrong message")
