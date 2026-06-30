/** Encrypted value envelope — stored in place of plaintext in supported CRDT ops. */
export interface EncryptedValue {
  $e: 1;
  /** base64url-encoded 12-byte AES-GCM nonce. */
  n: string;
  /** base64url-encoded ciphertext concatenated with the 16-byte GCM authentication tag. */
  d: string;
}

export function isEncryptedValue(v: unknown): v is EncryptedValue {
  return (
    typeof v === "object" &&
    v !== null &&
    !Array.isArray(v) &&
    (v as Record<string, unknown>).$e === 1 &&
    typeof (v as Record<string, unknown>).n === "string" &&
    typeof (v as Record<string, unknown>).d === "string"
  );
}

function toB64u(buf: ArrayBufferLike): string {
  return btoa(String.fromCharCode(...new Uint8Array(buf)))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}

function fromB64u(s: string): Uint8Array<ArrayBuffer> {
  const padded = s.replace(/-/g, "+").replace(/_/g, "/");
  const padLen = (4 - (padded.length % 4)) % 4;
  const arr = Uint8Array.from(atob(padded + "=".repeat(padLen)), (c) => c.charCodeAt(0));
  return new Uint8Array(arr.buffer);
}

/** Encrypt `value` (any JSON-serializable value) with AES-GCM-256. */
export async function encryptJson(key: CryptoKey, value: unknown): Promise<EncryptedValue> {
  const plaintext = new TextEncoder().encode(JSON.stringify(value));
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, key, plaintext);
  return { $e: 1, n: toB64u(nonce.buffer), d: toB64u(ciphertext) };
}

/**
 * Encrypt `value` with AES-GCM-256 using a deterministic nonce derived from the
 * plaintext (SHA-256(plaintext)[0..12]). Same plaintext + same key → same ciphertext.
 *
 * Required for ORSet encryption: the server keys entries by the encrypted element
 * value, so removes must produce the same ciphertext as the original add.
 *
 * Security: different plaintexts produce different nonces (via SHA-256), so there
 * is no nonce reuse across distinct values. Identical plaintexts produce identical
 * ciphertexts, which only reveals that two entries hold the same value — already
 * visible from the plaintext in a non-encrypted ORSet.
 */
export async function encryptJsonDeterministic(key: CryptoKey, value: unknown): Promise<EncryptedValue> {
  const plaintext = new TextEncoder().encode(JSON.stringify(value));
  // Derive a deterministic 12-byte nonce from the plaintext. `.slice()` creates a
  // fresh 12-byte ArrayBuffer so toB64u does not accidentally encode all 32 bytes.
  const nonce = new Uint8Array(await crypto.subtle.digest("SHA-256", plaintext)).slice(0, 12);
  const ciphertext = await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce }, key, plaintext);
  return { $e: 1, n: toB64u(nonce.buffer), d: toB64u(ciphertext) };
}

/** Decrypt an `EncryptedValue` produced by `encryptJson`. Throws on wrong key or tampered ciphertext. */
export async function decryptJson(key: CryptoKey, enc: EncryptedValue): Promise<unknown> {
  const nonce = fromB64u(enc.n);
  const ciphertext = fromB64u(enc.d);
  const plaintext = await crypto.subtle.decrypt({ name: "AES-GCM", iv: nonce }, key, ciphertext);
  return JSON.parse(new TextDecoder().decode(plaintext));
}

/** Import a raw 32-byte AES-256-GCM key from bytes or a base64url string. */
export async function importAesGcmKey(raw: Uint8Array | string): Promise<CryptoKey> {
  const keyBytes = typeof raw === "string" ? fromB64u(raw) : new Uint8Array(raw);
  return crypto.subtle.importKey("raw", keyBytes, { name: "AES-GCM" }, false, ["encrypt", "decrypt"]);
}

/**
 * Generate a fresh random AES-256-GCM key.
 * Returns the `CryptoKey` for immediate use and its raw bytes as base64url (for distribution).
 */
export async function generateAesGcmKey(): Promise<{ key: CryptoKey; rawBase64: string }> {
  const key = await crypto.subtle.generateKey({ name: "AES-GCM", length: 256 }, true, ["encrypt", "decrypt"]);
  const raw = await crypto.subtle.exportKey("raw", key);
  return { key, rawBase64: toB64u(raw) };
}
