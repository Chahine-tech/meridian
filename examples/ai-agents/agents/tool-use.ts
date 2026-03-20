/**
 * Meridian × Claude tool use — stateless HTTP example
 *
 * Demonstrates an AI agent that reads and writes Meridian CRDTs using
 * Claude's native tool use, with no persistent WebSocket connection.
 *
 * Run:
 *   MERIDIAN_URL=http://localhost:3000 \
 *   MERIDIAN_TOKEN=<admin-token> \
 *   ANTHROPIC_API_KEY=<key> \
 *   bun run agents/tool-use.ts
 */

import Anthropic from "@anthropic-ai/sdk";
import { getMeridianTools, executeMeridianTool } from "meridian-sdk";
import type { ToolUseBlock } from "meridian-sdk";

const NAMESPACE = "ai-agents";
const CRDT_IDS = ["views", "notes", "tags"];

async function main() {
  const baseUrl = (process.env.MERIDIAN_URL ?? "http://localhost:3000").replace(/^ws/, "http");
  const token = process.env.MERIDIAN_TOKEN;
  const apiKey = process.env.ANTHROPIC_API_KEY;

  if (!token) throw new Error("MERIDIAN_TOKEN is required");
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY is required");

  const agentConfig = { baseUrl, token, namespace: NAMESPACE };
  const anthropic = new Anthropic({ apiKey });

  // Generate Anthropic tool definitions from CRDT IDs
  const tools = getMeridianTools(agentConfig, CRDT_IDS);

  console.log(`[tool-use] Agent starting — namespace: ${NAMESPACE}`);
  console.log(`[tool-use] Available tools: ${tools.map((t) => t.name).join(", ")}`);

  const messages: Anthropic.MessageParam[] = [
    {
      role: "user",
      content: `You are an assistant that manages shared state using Meridian CRDTs.

Your tasks:
1. Read the current value of the "views" counter
2. Increment the "views" counter by 1 (use client_id: 42)
3. Set the "notes" register to the string "Updated by AI agent at ${new Date().toISOString()}" (use client_id: 42)
4. Read the final state of both "views" and "notes"
5. Summarize what you did

Use the available tools to complete these tasks.`,
    },
  ];

  // Agentic loop — keep calling Claude until it stops using tools
  let iteration = 0;
  while (iteration < 10) {
    iteration++;

    const response = await anthropic.messages.create({
      model: "claude-opus-4-6",
      max_tokens: 1024,
      tools,
      messages,
    });

    console.log(`\n[tool-use] Claude response (stop_reason: ${response.stop_reason}):`);

    // Collect tool results for this turn
    const toolResults: Anthropic.ToolResultBlockParam[] = [];

    for (const block of response.content) {
      if (block.type === "text") {
        console.log(`[claude] ${block.text}`);
      } else if (block.type === "tool_use") {
        const toolBlock = block as ToolUseBlock;
        console.log(`[tool]   ${toolBlock.name}(${JSON.stringify(toolBlock.input)})`);

        const result = await executeMeridianTool(agentConfig, toolBlock);
        console.log(`[result] ${result}`);

        toolResults.push({
          type: "tool_result",
          tool_use_id: toolBlock.id,
          content: result,
        });
      }
    }

    // Add Claude's response to the message history
    messages.push({ role: "assistant", content: response.content });

    // If there were tool calls, add the results and continue
    if (toolResults.length > 0) {
      messages.push({ role: "user", content: toolResults });
      continue;
    }

    // No more tool calls — Claude is done
    break;
  }

  console.log("\n[tool-use] Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
