/**
 * Meridian × Anthropic Claude tool use helpers.
 *
 * Generates Anthropic-compatible `Tool` definitions from a list of CRDT IDs
 * and executes tool calls against a Meridian server via HTTP (no WebSocket).
 *
 * Usage:
 * ```ts
 * import { getMeridianTools, executeMeridianTool } from "meridian-sdk";
 *
 * const tools = getMeridianTools({ baseUrl, token, namespace, crdtIds: ["counter", "tasks"] });
 *
 * // Pass tools to Anthropic messages.create(...)
 * // Then for each tool_use block in the response:
 * const result = await executeMeridianTool({ baseUrl, token, namespace }, toolUseBlock);
 * ```
 */

import { encode, uuidToBytes } from "./codec.js";

export interface Tool {
  name: string;
  description: string;
  input_schema: {
    type: "object";
    properties: Record<string, { type: string; description: string }>;
    required?: string[];
  };
}

export interface ToolUseBlock {
  type: "tool_use";
  id: string;
  name: string;
  input: Record<string, unknown>;
}

export interface MeridianAgentConfig {
  /** HTTP base URL of the Meridian server (e.g. https://meridian.example.com) */
  baseUrl: string;
  /** Bearer token with read+write permissions for the namespace */
  token: string;
  /** Namespace scoping all CRDT operations */
  namespace: string;
}

/**
 * Returns an Anthropic `Tool[]` array for the given CRDT IDs.
 *
 * Each CRDT exposes up to 3 tools:
 * - `meridian_read_{id}`      — read current JSON state
 * - `meridian_increment_{id}` — GCounter/PNCounter increment (amount)
 * - `meridian_set_{id}`       — LwwRegister set (value as JSON string)
 * - `meridian_add_{id}`       — ORSet add (element as JSON string)
 */
export function getMeridianTools(
  config: MeridianAgentConfig,
  crdtIds: string[],
): Tool[] {
  const tools: Tool[] = [];

  for (const id of crdtIds) {
    const safeName = id.replace(/[^a-zA-Z0-9_]/g, "_");

    tools.push({
      name: `meridian_read_${safeName}`,
      description: `Read the current state of the "${id}" CRDT in namespace "${config.namespace}". Returns a JSON object with the CRDT value.`,
      input_schema: {
        type: "object",
        properties: {},
      },
    });

    tools.push({
      name: `meridian_increment_${safeName}`,
      description: `Increment the "${id}" counter CRDT by a given amount. Works for GCounter (positive only) and PNCounter (positive or negative).`,
      input_schema: {
        type: "object",
        properties: {
          amount: { type: "number", description: "The amount to increment (positive for GCounter, positive or negative for PNCounter)" },
          client_id: { type: "number", description: "Unique numeric client ID for this agent (e.g. 1)" },
        },
        required: ["amount", "client_id"],
      },
    });

    tools.push({
      name: `meridian_set_${safeName}`,
      description: `Set the value of the "${id}" LWW register CRDT. The value is serialized as a JSON string.`,
      input_schema: {
        type: "object",
        properties: {
          value: { type: "string", description: "The new value as a JSON string (e.g. '\"hello\"' or '{\"key\": 1}')" },
          client_id: { type: "number", description: "Unique numeric client ID for this agent" },
        },
        required: ["value", "client_id"],
      },
    });

    tools.push({
      name: `meridian_add_${safeName}`,
      description: `Add an element to the "${id}" ORSet CRDT.`,
      input_schema: {
        type: "object",
        properties: {
          element: { type: "string", description: "The element to add (as a JSON string)" },
          client_id: { type: "number", description: "Unique numeric client ID for this agent" },
        },
        required: ["element", "client_id"],
      },
    });
  }

  return tools;
}

/**
 * Execute a Meridian tool call returned by Claude.
 *
 * Returns a JSON string suitable for the `content` field of a `tool_result` block.
 */
export async function executeMeridianTool(
  config: MeridianAgentConfig,
  toolUse: ToolUseBlock,
): Promise<string> {
  const { baseUrl, token, namespace } = config;
  const { name, input } = toolUse;

  // Detect operation type and crdt_id from tool name
  const readMatch = name.match(/^meridian_read_(.+)$/);
  const incrementMatch = name.match(/^meridian_increment_(.+)$/);
  const setMatch = name.match(/^meridian_set_(.+)$/);
  const addMatch = name.match(/^meridian_add_(.+)$/);

  if (readMatch?.[1]) {
    const crdtId = readMatch[1].replace(/_/g, "-");
    return readCrdt(baseUrl, token, namespace, crdtId);
  }

  if (incrementMatch?.[1]) {
    const crdtId = incrementMatch[1].replace(/_/g, "-");
    const amount = Number(input.amount ?? 1);
    const clientId = Number(input.client_id ?? 1);
    return applyCounterOp(baseUrl, token, namespace, crdtId, clientId, amount);
  }

  if (setMatch?.[1]) {
    const crdtId = setMatch[1].replace(/_/g, "-");
    const raw = String(input.value ?? "null");
    let value: unknown;
    try { value = JSON.parse(raw); } catch { value = raw; }
    const clientId = Number(input.client_id ?? 1);
    return applyLwwOp(baseUrl, token, namespace, crdtId, clientId, value);
  }

  if (addMatch?.[1]) {
    const crdtId = addMatch[1].replace(/_/g, "-");
    // Always store as a string so useORSet<string> can read it back directly.
    // If Claude passes a JSON object, re-serialize it to a stable string.
    const rawEl = input.element;
    const element: string = typeof rawEl === "string" ? rawEl : JSON.stringify(rawEl ?? null);
    const clientId = Number(input.client_id ?? 1);
    return applyOrSetOp(baseUrl, token, namespace, crdtId, clientId, element);
  }

  return JSON.stringify({ error: `Unknown tool: ${name}` });
}

async function readCrdt(
  baseUrl: string,
  token: string,
  ns: string,
  id: string,
): Promise<string> {
  const res = await fetch(`${baseUrl}/v1/namespaces/${ns}/crdts/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!res.ok) {
    return JSON.stringify({ error: `HTTP ${res.status}`, crdt_id: id });
  }
  const body = await res.json();
  return JSON.stringify({ crdt_id: id, value: body });
}

async function applyCounterOp(
  baseUrl: string,
  token: string,
  ns: string,
  id: string,
  clientId: number,
  amount: number,
): Promise<string> {
  // Encode as GCounter op if positive, PNCounter op if negative
  const op = amount >= 0
    ? { GCounter: { client_id: clientId, amount } }
    : { PNCounter: { client_id: clientId, amount } };

  const body = encode(op);
  const res = await fetch(`${baseUrl}/v1/namespaces/${ns}/crdts/${id}/ops`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/msgpack",
    },
    body,
  });

  if (!res.ok) {
    return JSON.stringify({ error: `HTTP ${res.status}`, crdt_id: id });
  }
  return JSON.stringify({ ok: true, crdt_id: id, op: "increment", amount });
}

async function applyLwwOp(
  baseUrl: string,
  token: string,
  ns: string,
  id: string,
  clientId: number,
  value: unknown,
): Promise<string> {
  const nowMs = Date.now();
  const op = {
    LwwRegister: {
      author: clientId,
      value,
      hlc: { wall_ms: nowMs, logical: 0, node_id: clientId },
    },
  };

  const body = encode(op);
  const res = await fetch(`${baseUrl}/v1/namespaces/${ns}/crdts/${id}/ops`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/msgpack",
    },
    body,
  });

  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    return JSON.stringify({ error: `HTTP ${res.status}`, crdt_id: id, detail });
  }
  return JSON.stringify({ ok: true, crdt_id: id, op: "set", value });
}

async function applyOrSetOp(
  baseUrl: string,
  token: string,
  ns: string,
  id: string,
  clientId: number,
  element: unknown,
): Promise<string> {
  const op = {
    ORSet: {
      Add: { client_id: clientId, element, tag: uuidToBytes(crypto.randomUUID()) },
    },
  };

  const body = encode(op);
  const res = await fetch(`${baseUrl}/v1/namespaces/${ns}/crdts/${id}/ops`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/msgpack",
    },
    body,
  });

  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    return JSON.stringify({ error: `HTTP ${res.status}`, crdt_id: id, detail });
  }
  return JSON.stringify({ ok: true, crdt_id: id, op: "add", element });
}
