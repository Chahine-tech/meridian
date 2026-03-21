/**
 * Meridian × Claude — persistent agent memory
 *
 * Demonstrates an AI agent that uses Meridian CRDTs as long-term memory.
 * Each run reads its previous state, reasons about it, then writes an updated
 * summary before exiting. The next run picks up exactly where this one left off.
 *
 * Memory layout (namespace: "agent-memory"):
 *   memory-run-count   — GCounter  — total number of runs ever
 *   memory-last-run    — LWW Reg   — JSON blob: { timestamp, summary, insights_added }
 *   memory-insights    — ORSet     — accumulated observations across all runs
 *
 * Run:
 *   MERIDIAN_URL=http://localhost:3000 \
 *   MERIDIAN_TOKEN=<token> \
 *   ANTHROPIC_API_KEY=<key> \
 *   bun run agents/memory.ts
 */

import Anthropic from "@anthropic-ai/sdk";
import { getMeridianTools, executeMeridianTool } from "meridian-sdk";
import type { ToolUseBlock } from "meridian-sdk";

const NAMESPACE = "ai-agents";
const CRDT_IDS = ["memory-run-count", "memory-last-run", "memory-insights"];
const CLIENT_ID = 1;

async function main() {
  const baseUrl = (process.env.MERIDIAN_URL ?? "http://localhost:3000").replace(/^ws/, "http");
  const token = process.env.MERIDIAN_TOKEN;
  const apiKey = process.env.ANTHROPIC_API_KEY;

  if (!token) throw new Error("MERIDIAN_TOKEN is required");
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY is required");

  const agentConfig = { baseUrl, token, namespace: NAMESPACE };
  const anthropic = new Anthropic({ apiKey });
  const tools = getMeridianTools(agentConfig, CRDT_IDS);

  // --- Read current memory state before starting ---
  const [runCount, lastRun, insights] = await Promise.all([
    fetch(`${baseUrl}/v1/namespaces/${NAMESPACE}/crdts/memory-run-count`, {
      headers: { Authorization: `Bearer ${token}` },
    }).then(r => r.ok ? r.json() as Promise<{ total: number }> : null),
    fetch(`${baseUrl}/v1/namespaces/${NAMESPACE}/crdts/memory-last-run`, {
      headers: { Authorization: `Bearer ${token}` },
    }).then(r => r.ok ? r.json() as Promise<{ value: unknown }> : null),
    fetch(`${baseUrl}/v1/namespaces/${NAMESPACE}/crdts/memory-insights`, {
      headers: { Authorization: `Bearer ${token}` },
    }).then(r => r.ok ? r.json() as Promise<string[]> : null),
  ]);

  const currentRun = (runCount?.total ?? 0) + 1;
  const previousSummary = lastRun?.value ?? null;
  const previousInsights: string[] = insights ?? [];

  console.log(`[memory] Starting run #${currentRun}`);
  if (previousSummary) {
    console.log(`[memory] Previous run summary: ${JSON.stringify(previousSummary)}`);
  } else {
    console.log(`[memory] No previous run found — this is the first run.`);
  }
  if (previousInsights.length > 0) {
    console.log(`[memory] Accumulated insights (${previousInsights.length}): ${previousInsights.join(", ")}`);
  }

  // --- Build prompt with memory context ---
  const memoryContext = previousSummary
    ? `Your memory from previous runs:
- This is run #${currentRun}
- Last run summary: ${JSON.stringify(previousSummary)}
- Accumulated insights so far: ${previousInsights.length > 0 ? previousInsights.join("; ") : "none yet"}`
    : `This is your first run — you have no previous memory yet.`;

  const messages: Anthropic.MessageParam[] = [
    {
      role: "user",
      content: `You are a persistent AI agent powered by Meridian CRDTs as long-term memory.

${memoryContext}

Your tasks for this run (#${currentRun}):
1. Increment "memory-run-count" by 1 (client_id: ${CLIENT_ID}) to record this run
2. Add one new insight to "memory-insights" — something you observe or deduce about your run history (client_id: ${CLIENT_ID})
3. Set "memory-last-run" to a JSON object summarizing this run: include run_number, timestamp, what_you_did, and a note_to_future_self (client_id: ${CLIENT_ID})
4. Read back all three CRDTs to confirm the writes succeeded
5. Summarize what you remember from before, what you did this run, and what you want to remember next time

Use the available tools to complete these tasks.`,
    },
  ];

  // --- Agentic loop ---
  let iteration = 0;
  while (iteration < 15) {
    iteration++;

    const response = await anthropic.messages.create({
      model: "claude-opus-4-6",
      max_tokens: 1024,
      tools,
      messages,
    });

    console.log(`\n[memory] Claude response (stop_reason: ${response.stop_reason}):`);

    const toolResults: Anthropic.ToolResultBlockParam[] = [];

    for (const block of response.content) {
      if (block.type === "text") {
        console.log(`[claude] ${block.text}`);
      } else if (block.type === "tool_use") {
        const toolBlock = block as ToolUseBlock;
        console.log(`[tool]   ${toolBlock.name}(${JSON.stringify(toolBlock.input)})`);
        const result = await executeMeridianTool(agentConfig, toolBlock);
        console.log(`[result] ${result}`);
        toolResults.push({ type: "tool_result", tool_use_id: toolBlock.id, content: result });
      }
    }

    messages.push({ role: "assistant", content: response.content });

    if (toolResults.length > 0) {
      messages.push({ role: "user", content: toolResults });
      continue;
    }

    break;
  }

  console.log(`\n[memory] Run #${currentRun} complete. State persisted in Meridian.`);
  console.log(`[memory] Run again to see the agent pick up from where it left off.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
