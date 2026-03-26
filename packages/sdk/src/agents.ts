/**
 * Meridian AI agent tool use helpers.
 *
 * Generates provider-compatible tool definitions from a list of CRDT IDs
 * and executes tool calls against a Meridian server via HTTP (no WebSocket).
 *
 * Supported providers:
 * - Anthropic Claude — `getMeridianTools()` (default)
 * - OpenAI GPT       — `toOpenAITools(getMeridianTools(...))`
 * - Google Gemini    — `toGeminiTools(getMeridianTools(...))`
 *
 * Usage (Anthropic):
 * ```ts
 * import { getMeridianTools, executeMeridianTool } from "meridian-sdk";
 *
 * const tools = getMeridianTools(config, ["counter", "tasks"]);
 * // Pass to Anthropic messages.create({ tools })
 * const result = await executeMeridianTool(config, toolUseBlock);
 * ```
 *
 * Usage (OpenAI):
 * ```ts
 * import { getMeridianTools, toOpenAITools, executeOpenAITool } from "meridian-sdk";
 *
 * const tools = toOpenAITools(getMeridianTools(config, ["counter", "tasks"]));
 * // Pass to openai.chat.completions.create({ tools })
 * // For each tool_call in the response — pass directly, no wrapping:
 * const result = await executeOpenAITool(config, call);
 * ```
 *
 * Usage (Gemini):
 * ```ts
 * import { getMeridianTools, toGeminiTools, executeGeminiTool } from "meridian-sdk";
 *
 * const tools = toGeminiTools(getMeridianTools(config, ["counter", "tasks"]));
 * // Pass to model.generateContent({ tools })
 * // For each functionCall part — pass directly, no wrapping:
 * const result = await executeGeminiTool(config, part.functionCall);
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

// ---------------------------------------------------------------------------
// OpenAI adapter
// ---------------------------------------------------------------------------

/** OpenAI-compatible tool definition (Chat Completions `tools` array entry). */
export interface OpenAITool {
  type: "function";
  function: {
    name: string;
    description: string;
    parameters: {
      type: "object";
      properties: Record<string, { type: string; description: string }>;
      required?: string[];
    };
  };
}

/**
 * Convert Meridian tools to OpenAI Chat Completions format.
 *
 * ```ts
 * const tools = toOpenAITools(getMeridianTools(config, crdtIds));
 * // openai.chat.completions.create({ tools })
 * ```
 */
export function toOpenAITools(tools: Tool[]): OpenAITool[] {
  return tools.map(t => ({
    type: "function",
    function: {
      name: t.name,
      description: t.description,
      parameters: {
        type: "object",
        properties: t.input_schema.properties,
        ...(t.input_schema.required ? { required: t.input_schema.required } : {}),
      },
    },
  }));
}

// ---------------------------------------------------------------------------
// Gemini adapter
// ---------------------------------------------------------------------------

/** Gemini FunctionDeclaration (single entry inside a `Tool.functionDeclarations` array). */
export interface GeminiFunctionDeclaration {
  name: string;
  description: string;
  parameters: {
    type: "OBJECT";
    properties: Record<string, { type: string; description: string }>;
    required?: string[];
  };
}

/** Gemini `Tool` wrapper passed to `model.generateContent({ tools })`. */
export interface GeminiTool {
  functionDeclarations: GeminiFunctionDeclaration[];
}

/**
 * Convert Meridian tools to Google Gemini format.
 *
 * ```ts
 * const tools = toGeminiTools(getMeridianTools(config, crdtIds));
 * // model.generateContent({ tools })
 * ```
 */
export function toGeminiTools(tools: Tool[]): GeminiTool {
  return {
    functionDeclarations: tools.map(t => ({
      name: t.name,
      description: t.description,
      parameters: {
        type: "OBJECT",
        properties: t.input_schema.properties,
        ...(t.input_schema.required ? { required: t.input_schema.required } : {}),
      },
    })),
  };
}

// ---------------------------------------------------------------------------
// Provider-specific input types (structural matches — no provider SDK deps)
// ---------------------------------------------------------------------------

/** Anthropic tool_use block — matches `ContentBlock` from `@anthropic-ai/sdk`. */
export interface ToolUseBlock {
  type: "tool_use";
  id: string;
  name: string;
  input: Record<string, unknown>;
}

/**
 * OpenAI tool call — structurally matches `ChatCompletionMessageToolCall`
 * from the `openai` package. Pass directly from `response.choices[0].message.tool_calls`.
 */
export interface OpenAIToolCall {
  id: string;
  type: "function";
  function: {
    name: string;
    /** JSON-encoded arguments string, as returned by the OpenAI API. */
    arguments: string;
  };
}

/**
 * Gemini function call — structurally matches `FunctionCall` from
 * `@google/generative-ai`. Pass directly from `part.functionCall`.
 */
export interface GeminiFunctionCall {
  name: string;
  args: Record<string, unknown>;
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

// ---------------------------------------------------------------------------
// Shared dispatcher (private)
// ---------------------------------------------------------------------------

/**
 * Core dispatch logic — routes a (name, input) pair to the right HTTP operation.
 * All provider-specific executors delegate here.
 */
async function dispatchTool(
  config: MeridianAgentConfig,
  name: string,
  input: Record<string, unknown>,
): Promise<string> {
  const { baseUrl, token, namespace } = config;

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
    // If the model passes a JSON object, re-serialize it to a stable string.
    const rawEl = input.element;
    const element: string = typeof rawEl === "string" ? rawEl : JSON.stringify(rawEl ?? null);
    const clientId = Number(input.client_id ?? 1);
    return applyOrSetOp(baseUrl, token, namespace, crdtId, clientId, element);
  }

  return JSON.stringify({ error: `Unknown tool: ${name}` });
}

// ---------------------------------------------------------------------------
// Provider executors
// ---------------------------------------------------------------------------

/**
 * Execute a Meridian tool call returned by **Anthropic Claude**.
 *
 * Returns a JSON string suitable for the `content` field of a `tool_result` block.
 *
 * ```ts
 * for (const block of response.content) {
 *   if (block.type === "tool_use") {
 *     const result = await executeMeridianTool(config, block);
 *     toolResults.push({ type: "tool_result", tool_use_id: block.id, content: result });
 *   }
 * }
 * ```
 */
export async function executeMeridianTool(
  config: MeridianAgentConfig,
  toolUse: ToolUseBlock,
): Promise<string> {
  return dispatchTool(config, toolUse.name, toolUse.input);
}

/**
 * Execute a Meridian tool call returned by **OpenAI**.
 *
 * Pass `tool_call` directly from `response.choices[0].message.tool_calls` —
 * no wrapping required. Returns a JSON string for the `tool` role message content.
 *
 * ```ts
 * for (const call of response.choices[0].message.tool_calls ?? []) {
 *   const result = await executeOpenAITool(config, call);
 *   messages.push({ role: "tool", tool_call_id: call.id, content: result });
 * }
 * ```
 */
export async function executeOpenAITool(
  config: MeridianAgentConfig,
  toolCall: OpenAIToolCall,
): Promise<string> {
  let input: Record<string, unknown>;
  try {
    input = JSON.parse(toolCall.function.arguments) as Record<string, unknown>;
  } catch {
    return JSON.stringify({ error: "Failed to parse tool call arguments" });
  }
  return dispatchTool(config, toolCall.function.name, input);
}

/**
 * Execute a Meridian tool call returned by **Google Gemini**.
 *
 * Pass `part.functionCall` directly — no wrapping, no id hack required.
 * Returns a JSON string for the `functionResponse` part content.
 *
 * ```ts
 * for (const part of response.candidates?.[0].content.parts ?? []) {
 *   if (part.functionCall) {
 *     const result = await executeGeminiTool(config, part.functionCall);
 *     // pass result back as functionResponse part
 *   }
 * }
 * ```
 */
export async function executeGeminiTool(
  config: MeridianAgentConfig,
  functionCall: GeminiFunctionCall,
): Promise<string> {
  return dispatchTool(config, functionCall.name, functionCall.args);
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
