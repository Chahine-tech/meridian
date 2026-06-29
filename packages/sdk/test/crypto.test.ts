import { describe, it, expect } from "bun:test";
import {
  encryptJson,
  decryptJson,
  importAesGcmKey,
  generateAesGcmKey,
  isEncryptedValue,
} from "../src/crypto/aes-gcm.js";
import {
  generateClientKeypair,
  signOp,
} from "../src/crypto/ed25519.js";

// ── AES-GCM ──────────────────────────────────────────────────────────────────

describe("AES-GCM", () => {
  it("encrypts and decrypts a string value", async () => {
    const { key } = await generateAesGcmKey();
    const enc = await encryptJson(key, "hello world");
    const dec = await decryptJson(key, enc);
    expect(dec).toBe("hello world");
  });

  it("encrypts and decrypts a nested object", async () => {
    const { key } = await generateAesGcmKey();
    const value = { name: "Alice", score: 42, tags: ["a", "b"] };
    const enc = await encryptJson(key, value);
    const dec = await decryptJson(key, enc);
    expect(dec).toEqual(value);
  });

  it("produces a valid EncryptedValue envelope", async () => {
    const { key } = await generateAesGcmKey();
    const enc = await encryptJson(key, 123);
    expect(enc.$e).toBe(1);
    expect(typeof enc.n).toBe("string");
    expect(enc.n.length).toBeGreaterThan(0);
    expect(typeof enc.d).toBe("string");
    expect(enc.d.length).toBeGreaterThan(0);
  });

  it("uses a fresh nonce each time (non-deterministic)", async () => {
    const { key } = await generateAesGcmKey();
    const a = await encryptJson(key, "same value");
    const b = await encryptJson(key, "same value");
    // Different nonces → different ciphertext
    expect(a.n).not.toBe(b.n);
    expect(a.d).not.toBe(b.d);
  });

  it("throws with the wrong key", async () => {
    const { key: k1 } = await generateAesGcmKey();
    const { key: k2 } = await generateAesGcmKey();
    const enc = await encryptJson(k1, "secret");
    await expect(decryptJson(k2, enc)).rejects.toThrow();
  });

  it("throws on tampered ciphertext", async () => {
    const { key } = await generateAesGcmKey();
    const enc = await encryptJson(key, "secret");
    const tampered = { ...enc, d: enc.d.slice(0, -4) + "AAAA" };
    await expect(decryptJson(key, tampered)).rejects.toThrow();
  });

  it("importAesGcmKey roundtrips via rawBase64", async () => {
    const { key: original, rawBase64 } = await generateAesGcmKey();
    const imported = await importAesGcmKey(rawBase64);
    // Both keys encrypt/decrypt interchangeably
    const enc = await encryptJson(original, "test");
    const dec = await decryptJson(imported, enc);
    expect(dec).toBe("test");
  });
});

// ── isEncryptedValue ──────────────────────────────────────────────────────────

describe("isEncryptedValue", () => {
  it("recognises a valid envelope", async () => {
    const { key } = await generateAesGcmKey();
    const enc = await encryptJson(key, "x");
    expect(isEncryptedValue(enc)).toBe(true);
  });

  it("rejects plain strings, numbers, objects", () => {
    expect(isEncryptedValue("hello")).toBe(false);
    expect(isEncryptedValue(42)).toBe(false);
    expect(isEncryptedValue({ foo: "bar" })).toBe(false);
    expect(isEncryptedValue(null)).toBe(false);
    expect(isEncryptedValue({ $e: 1, n: "abc" })).toBe(false); // missing d
  });
});

// ── Ed25519 ───────────────────────────────────────────────────────────────────

describe("Ed25519", () => {
  it("signs and verifies op bytes", async () => {
    const kp = await generateClientKeypair();
    const opBytes = new Uint8Array([1, 2, 3, 4, 5]);
    const sig = await signOp(kp.privateKey, opBytes);
    expect(sig).toBeInstanceOf(Uint8Array);
    expect(sig.byteLength).toBe(64);

    // Verify using the Web Crypto API directly
    const valid = await crypto.subtle.verify(
      { name: "Ed25519" } as AlgorithmIdentifier,
      kp.publicKey,
      sig,
      opBytes,
    );
    expect(valid).toBe(true);
  });

  it("signature is invalid for a different message", async () => {
    const kp = await generateClientKeypair();
    const sig = await signOp(kp.privateKey, new Uint8Array([1, 2, 3]));
    const valid = await crypto.subtle.verify(
      { name: "Ed25519" } as AlgorithmIdentifier,
      kp.publicKey,
      sig,
      new Uint8Array([9, 9, 9]),
    );
    expect(valid).toBe(false);
  });

  it("signature is invalid for a different keypair", async () => {
    const kp1 = await generateClientKeypair();
    const kp2 = await generateClientKeypair();
    const opBytes = new Uint8Array([1, 2, 3]);
    const sig = await signOp(kp1.privateKey, opBytes);
    const valid = await crypto.subtle.verify(
      { name: "Ed25519" } as AlgorithmIdentifier,
      kp2.publicKey,
      sig,
      opBytes,
    );
    expect(valid).toBe(false);
  });

  it("publicKeyBytes is 32 bytes", async () => {
    const kp = await generateClientKeypair();
    expect(kp.publicKeyBytes).toBeInstanceOf(Uint8Array);
    expect(kp.publicKeyBytes.byteLength).toBe(32);
  });

  it("each generateClientKeypair produces a unique key", async () => {
    const a = await generateClientKeypair();
    const b = await generateClientKeypair();
    expect(a.publicKeyBytes).not.toEqual(b.publicKeyBytes);
  });
});
