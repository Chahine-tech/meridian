/**
 * Unit tests for sync utilities: VectorClockTracker, codec, token parsing.
 */

import { describe, it, expect } from "bun:test";
import { Effect } from "effect";
import { VectorClockTracker } from "../src/sync/clock.js";
import { encode, decode, encodeClientMsg, decodeServerMsg } from "../src/codec.js";
import { parseToken, checkTokenExpiry } from "../src/auth/token.js";
import { CodecError, TokenParseError, TokenExpiredError } from "../src/errors.js";
import { pack } from "msgpackr";

// ---------------------------------------------------------------------------
// VectorClockTracker
// ---------------------------------------------------------------------------

describe("VectorClockTracker", () => {
  function freshStorage() {
    const store = new Map<string, Record<string, number>>();
    return {
      load: (key: string) => store.get(key) ?? null,
      save: (key: string, vc: Record<string, number>) => { store.set(key, { ...vc }); },
    };
  }

  function tracker(initial: Record<string, number> = {}) {
    return new VectorClockTracker({ namespace: "ns", crdtId: "c", storage: freshStorage(), initial });
  }

  it("starts empty", () => {
    expect(tracker().snapshot()).toEqual({});
  });

  it("observe updates clock", () => {
    const t = tracker();
    t.observe("1", 5);
    expect(t.snapshot()["1"]).toBe(5);
  });

  it("observe ignores older versions", () => {
    const t = tracker({ "1": 10 });
    t.observe("1", 3);
    expect(t.snapshot()["1"]).toBe(10);
  });

  it("merge takes max per key", () => {
    const t = tracker({ "1": 5, "2": 10 });
    t.merge({ "1": 3, "2": 20, "3": 1 });
    const snap = t.snapshot();
    expect(snap["1"]).toBe(5);
    expect(snap["2"]).toBe(20);
    expect(snap["3"]).toBe(1);
  });

  it("reset clears all entries", () => {
    const t = tracker({ "1": 10 });
    t.reset();
    expect(t.snapshot()).toEqual({});
  });

  it("persists to storage on observe", () => {
    const storage = freshStorage();
    const t1 = new VectorClockTracker({ namespace: "ns", crdtId: "x", storage });
    t1.observe("42", 7);
    const t2 = new VectorClockTracker({ namespace: "ns", crdtId: "x", storage });
    expect(t2.snapshot()["42"]).toBe(7);
  });
});

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

describe("codec", () => {
  it("encode/decode roundtrip for plain objects", () => {
    const obj = { foo: "bar", n: 42 };
    expect(decode(encode(obj))).toEqual(obj);
  });

  it("encodeClientMsg produces bytes", () => {
    const bytes = encodeClientMsg({ Subscribe: { crdt_id: "foo" } });
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBeGreaterThan(0);
  });

  it("decodeServerMsg succeeds on valid Ack", async () => {
    const msg = { Ack: { seq: 99 } };
    const bytes = new Uint8Array(encode(msg));
    const result = await Effect.runPromise(decodeServerMsg(bytes));
    expect(result).toEqual(msg);
  });

  it("decodeServerMsg fails with CodecError on unknown variant", async () => {
    const bad = new Uint8Array(encode({ Unknown: {} }));
    const err = await Effect.runPromise(
      decodeServerMsg(bad).pipe(Effect.flip),
    );
    expect(err).toBeInstanceOf(CodecError);
  });

  it("decodeServerMsg fails with CodecError on non-object", async () => {
    const bad = new Uint8Array(encode("not-an-object"));
    const err = await Effect.runPromise(
      decodeServerMsg(bad).pipe(Effect.flip),
    );
    expect(err).toBeInstanceOf(CodecError);
  });

  it("decodeServerMsg succeeds on multi-key map (Schema matches first valid variant)", async () => {
    // Effect Schema union picks first matching variant — Ack wins here.
    // The server never sends multi-key maps; this tests Schema's lenient-but-safe behaviour.
    const bytes = new Uint8Array(
      encode({ Ack: { seq: 1 }, Delta: { crdt_id: "x", delta_bytes: new Uint8Array() } }),
    );
    const result = await Effect.runPromise(decodeServerMsg(bytes));
    // Schema picks whichever variant matches first — just assert it decoded without error
    expect("Ack" in result || "Delta" in result).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Token parsing
// ---------------------------------------------------------------------------

describe("parseToken", () => {
  function makeToken(claims: object): string {
    const payload = pack(claims);
    const b64 = btoa(String.fromCharCode(...payload))
      .replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
    return `${b64}.fakesig`;
  }

  it("parses valid claims", async () => {
    const token = makeToken({
      namespace: "my-room",
      client_id: 42,
      expires_at: Date.now() + 3_600_000,
      permissions: { read: true, write: true, admin: false },
    });
    const claims = await Effect.runPromise(parseToken(token));
    expect(claims.namespace).toBe("my-room");
    expect(claims.client_id).toBe(42);
  });

  it("fails with TokenParseError on missing dot", async () => {
    const err = await Effect.runPromise(parseToken("nodothere").pipe(Effect.flip));
    expect(err).toBeInstanceOf(TokenParseError);
  });

  it("fails with TokenParseError on bad base64", async () => {
    const err = await Effect.runPromise(parseToken("!!!.sig").pipe(Effect.flip));
    expect(err).toBeInstanceOf(TokenParseError);
  });

  it("checkTokenExpiry succeeds for future expiry", async () => {
    const token = makeToken({
      namespace: "ns",
      client_id: 1,
      expires_at: Date.now() + 3_600_000,
      permissions: { read: true, write: false, admin: false },
    });
    const claims = await Effect.runPromise(parseToken(token));
    const result = await Effect.runPromise(checkTokenExpiry(claims));
    expect(result.client_id).toBe(1);
  });

  it("checkTokenExpiry fails with TokenExpiredError for past expiry", async () => {
    const token = makeToken({
      namespace: "ns",
      client_id: 1,
      expires_at: 1,
      permissions: { read: true, write: false, admin: false },
    });
    const claims = await Effect.runPromise(parseToken(token));
    const err = await Effect.runPromise(checkTokenExpiry(claims).pipe(Effect.flip));
    expect(err).toBeInstanceOf(TokenExpiredError);
  });
});
