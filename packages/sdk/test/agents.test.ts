/**
 * Unit tests for agents.ts — no server required.
 */

import { describe, it, expect } from "bun:test";
import { getMeridianTools, executeMeridianTool } from "../src/agents.js";
import type { ToolUseBlock } from "../src/agents.js";

const CONFIG = { baseUrl: "http://localhost:3000", token: "tok", namespace: "test-ns" };

// ---------------------------------------------------------------------------
// getMeridianTools
// ---------------------------------------------------------------------------

describe("getMeridianTools", () => {
  it("generates 4 tools per CRDT ID", () => {
    const tools = getMeridianTools(CONFIG, ["counter"]);
    expect(tools).toHaveLength(4);
  });

  it("generates correct tool names", () => {
    const tools = getMeridianTools(CONFIG, ["my-counter"]);
    const names = tools.map((t) => t.name);
    expect(names).toContain("meridian_read_my_counter");
    expect(names).toContain("meridian_increment_my_counter");
    expect(names).toContain("meridian_set_my_counter");
    expect(names).toContain("meridian_add_my_counter");
  });

  it("handles multiple CRDT IDs", () => {
    const tools = getMeridianTools(CONFIG, ["counter", "tasks", "notes"]);
    expect(tools).toHaveLength(12);
  });

  it("returns empty array for empty crdtIds", () => {
    const tools = getMeridianTools(CONFIG, []);
    expect(tools).toHaveLength(0);
  });

  it("sanitizes special characters in CRDT IDs", () => {
    const tools = getMeridianTools(CONFIG, ["my:crdt/id"]);
    const names = tools.map((t) => t.name);
    // colons and slashes should be replaced with underscores
    expect(names.every((n) => /^meridian_[a-z_]+$/.test(n))).toBe(true);
  });

  it("read tool has no required fields", () => {
    const tools = getMeridianTools(CONFIG, ["counter"]);
    const readTool = tools.find((t) => t.name === "meridian_read_counter");
    expect(readTool).toBeDefined();
    expect(readTool!.input_schema.required).toBeUndefined();
  });

  it("increment tool requires amount and client_id", () => {
    const tools = getMeridianTools(CONFIG, ["counter"]);
    const incrTool = tools.find((t) => t.name === "meridian_increment_counter");
    expect(incrTool!.input_schema.required).toContain("amount");
    expect(incrTool!.input_schema.required).toContain("client_id");
  });

  it("set tool requires value and client_id", () => {
    const tools = getMeridianTools(CONFIG, ["notes"]);
    const setTool = tools.find((t) => t.name === "meridian_set_notes");
    expect(setTool!.input_schema.required).toContain("value");
    expect(setTool!.input_schema.required).toContain("client_id");
  });

  it("add tool requires element and client_id", () => {
    const tools = getMeridianTools(CONFIG, ["tags"]);
    const addTool = tools.find((t) => t.name === "meridian_add_tags");
    expect(addTool!.input_schema.required).toContain("element");
    expect(addTool!.input_schema.required).toContain("client_id");
  });

  it("all tools have descriptions mentioning the CRDT id", () => {
    const tools = getMeridianTools(CONFIG, ["visits"]);
    for (const tool of tools) {
      expect(tool.description).toContain("visits");
    }
  });

  it("read tool description mentions the namespace", () => {
    const tools = getMeridianTools(CONFIG, ["visits"]);
    const readTool = tools.find((t) => t.name === "meridian_read_visits");
    expect(readTool!.description).toContain("test-ns");
  });

  it("all tools have input_schema type object", () => {
    const tools = getMeridianTools(CONFIG, ["x"]);
    for (const tool of tools) {
      expect(tool.input_schema.type).toBe("object");
    }
  });
});

// ---------------------------------------------------------------------------
// executeMeridianTool — unknown tool
// ---------------------------------------------------------------------------

describe("executeMeridianTool", () => {
  it("returns error JSON for unknown tool name", async () => {
    const toolUse: ToolUseBlock = {
      type: "tool_use",
      id: "tu_1",
      name: "meridian_unknown_op",
      input: {},
    };
    const result = await executeMeridianTool(CONFIG, toolUse);
    const parsed = JSON.parse(result);
    expect(parsed.error).toBeDefined();
  });
});
