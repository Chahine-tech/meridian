/** An Ed25519 client keypair used for BFT op signing. */
export interface ClientKeypair {
  publicKey: CryptoKey;
  privateKey: CryptoKey;
  /**
   * Raw 32-byte public key.
   * Embed this in the token issuance request as `client_pubkey` (base64url-encoded)
   * so the server can verify op signatures.
   */
  publicKeyBytes: Uint8Array;
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

/**
 * Generate a fresh Ed25519 keypair.
 *
 * Requires Chrome 113+, Firefox 130+, or Node.js 20+.
 */
export async function generateClientKeypair(): Promise<ClientKeypair> {
  const kp = await crypto.subtle.generateKey(
    { name: "Ed25519" } as EcKeyGenParams,
    true,
    ["sign", "verify"],
  ) as CryptoKeyPair;
  const rawPub = new Uint8Array(await crypto.subtle.exportKey("raw", kp.publicKey));
  return { publicKey: kp.publicKey, privateKey: kp.privateKey, publicKeyBytes: rawPub };
}

/** Sign `opBytes` (raw msgpack of a `CrdtOp`) with the client's private key. Returns 64 bytes. */
export async function signOp(privateKey: CryptoKey, opBytes: Uint8Array): Promise<Uint8Array> {
  // Ensure we have a concrete ArrayBuffer (WebCrypto rejects SharedArrayBuffer).
  const sig = await crypto.subtle.sign({ name: "Ed25519" } as AlgorithmIdentifier, privateKey, new Uint8Array(opBytes));
  return new Uint8Array(sig);
}

const LS_PRIV = "meridian:sig_priv_v1";
const LS_PUB = "meridian:sig_pub_v1";

/**
 * Load the client's Ed25519 signing keypair from `localStorage`, or generate and
 * persist one if absent.
 *
 * The private key is persisted in PKCS8 format (base64url). It is accessible to any
 * JavaScript on the same origin, which is acceptable for the BFT threat model
 * (protection against compromised *server* nodes). For client-compromise protection,
 * use a hardware-backed key stored in a secure enclave instead.
 */
export async function loadOrGenerateKeypair(): Promise<ClientKeypair> {
  if (typeof localStorage !== "undefined") {
    const privB64 = localStorage.getItem(LS_PRIV);
    const pubB64 = localStorage.getItem(LS_PUB);
    if (privB64 !== null && pubB64 !== null) {
      try {
        const privBytes = fromB64u(privB64);
        const pubBytes = fromB64u(pubB64);
        const [privateKey, publicKey] = await Promise.all([
          crypto.subtle.importKey("pkcs8", privBytes, { name: "Ed25519" } as EcKeyImportParams, true, ["sign"]),
          crypto.subtle.importKey("raw", pubBytes, { name: "Ed25519" } as EcKeyImportParams, true, ["verify"]),
        ]);
        return { publicKey, privateKey, publicKeyBytes: pubBytes };
      } catch { /* corrupted or algorithm mismatch — regenerate */ }
    }
  }

  const kp = await generateClientKeypair();

  if (typeof localStorage !== "undefined") {
    try {
      const privBytes = new Uint8Array(await crypto.subtle.exportKey("pkcs8", kp.privateKey));
      localStorage.setItem(LS_PRIV, toB64u(privBytes.buffer));
      localStorage.setItem(LS_PUB, toB64u(kp.publicKeyBytes.buffer));
    } catch { /* storage unavailable — keypair is ephemeral for this session */ }
  }

  return kp;
}
