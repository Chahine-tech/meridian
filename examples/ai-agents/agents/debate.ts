/**
 * Meridian × Claude — Multi-agent debate
 *
 * Two Claude agents debate a topic using Meridian CRDTs as their sole
 * communication channel. Each agent reads the opponent's latest argument
 * from the server before writing its own response — no shared memory,
 * no direct inter-process communication.
 *
 * Agent 1 argues FOR, Agent 2 argues AGAINST.
 * Each round: Agent 1 writes → Agent 2 reads + responds → repeat.
 *
 * CRDT layout (namespace: "ai-agents"):
 *   debate-agent1-argument  LwwRegister  current argument from Agent 1
 *   debate-agent2-argument  LwwRegister  current argument from Agent 2
 *   debate-agent1-history   ORSet        all Agent 1 rounds: JSON {"round":N,"text":"..."}
 *   debate-agent2-history   ORSet        all Agent 2 rounds: JSON {"round":N,"text":"..."}
 *   debate-agent1-round     GCounter     rounds completed by Agent 1
 *   debate-agent2-round     GCounter     rounds completed by Agent 2
 *   debate-topic            LwwRegister  debate topic string
 *   debate-status           LwwRegister  "idle" | "running" | "done"
 *
 * Run:
 *   MERIDIAN_URL=http://localhost:3000 \
 *   MERIDIAN_TOKEN=<token> \
 *   ANTHROPIC_API_KEY=<key> \
 *   DEBATE_TOPIC="Open source AI is safer than closed source AI" \
 *   DEBATE_ROUNDS=3 \
 *   bun run agents/debate.ts
 */

import Anthropic from "@anthropic-ai/sdk";
import { getMeridianTools, executeMeridianTool, encode } from "meridian-sdk";
import type { MeridianAgentConfig, ToolUseBlock } from "meridian-sdk";

const NAMESPACE = "ai-agents";

function uuidToBytes(uuid: string): Uint8Array {
  const hex = uuid.replace(/-/g, "");
  const bytes = new Uint8Array(16);
  for (let i = 0; i < 16; i++) bytes[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  return bytes;
}
const ALL_CRDT_IDS = [
  "debate-agent1-argument",
  "debate-agent2-argument",
  "debate-agent1-history",
  "debate-agent2-history",
  "debate-agent1-round",
  "debate-agent2-round",
  "debate-topic",
  "debate-status",
];

interface DebaterConfig {
  agentId: 1 | 2;
  clientId: number;
  stance: "FOR" | "AGAINST";
  crdtIdOwn: string;
  crdtIdHistoryOwn: string;
  crdtIdRoundOwn: string;
  agentConfig: MeridianAgentConfig;
  anthropic: Anthropic;
}

async function setLww(baseUrl: string, token: string, ns: string, id: string, clientId: number, value: unknown) {
  const nowMs = Date.now();
  const op = {
    LwwRegister: {
      author: clientId,
      value,
      hlc: { wall_ms: nowMs, logical: 0, node_id: clientId },
    },
  };
  await fetch(`${baseUrl}/v1/namespaces/${ns}/crdts/${id}/ops`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/msgpack" },
    body: encode(op),
  });
}

const DRY_RUN = process.argv.includes("--dry-run");

const DRY_RUN_ARGS: Record<string, string[]> = {
  "FOR": [
    "Open source AI enables community-wide security audits, making vulnerabilities discoverable and patchable faster than any single organization could manage.",
    "Transparency in AI systems allows independent researchers to verify safety claims and identify alignment issues before deployment at scale.",
    "The history of cryptography shows that open standards consistently outperform security-through-obscurity — the same principle applies to AI safety.",
  ],
  "AGAINST": [
    "Open source AI lowers the barrier for bad actors to fine-tune dangerous capabilities without any safety guardrails or oversight.",
    "Closed source allows organizations to implement deployment controls and usage monitoring that are impossible once weights are public.",
    "Security through obscurity is a weak argument — but responsible disclosure with coordinated patching windows is not the same as full public release.",
  ],
};

async function runDryRound(
  debater: DebaterConfig,
  round: number,
  totalRounds: number,
): Promise<void> {
  const { agentId, clientId, stance, crdtIdOwn, crdtIdHistoryOwn, crdtIdRoundOwn, agentConfig } = debater;
  const { baseUrl, token, namespace } = agentConfig;
  const arg = DRY_RUN_ARGS[stance]?.[round - 1] ?? `[dry-run] Agent ${agentId} round ${round} argument.`;

  console.log(`\n[Agent${agentId}] Round ${round}/${totalRounds} — ${stance} [DRY RUN]`);
  console.log(`[Agent${agentId}] Argument: ${arg}`);

  await setLww(baseUrl, token, namespace, crdtIdOwn, clientId, arg);

  const historyEntry = JSON.stringify({ round, text: arg });
  const op = {
    ORSet: { Add: { client_id: clientId, element: historyEntry, tag: uuidToBytes(crypto.randomUUID()) } },
  };
  await fetch(`${baseUrl}/v1/namespaces/${namespace}/crdts/${crdtIdHistoryOwn}/ops`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/msgpack" },
    body: encode(op),
  });

  const counterOp = { GCounter: { client_id: clientId, amount: 1 } };
  await fetch(`${baseUrl}/v1/namespaces/${namespace}/crdts/${crdtIdRoundOwn}/ops`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/msgpack" },
    body: encode(counterOp),
  });

  console.log(`[Agent${agentId}] Done.`);
}

async function runAgentRound(
  debater: DebaterConfig,
  round: number,
  totalRounds: number,
  opponentArgCrdtId: string | null,
): Promise<void> {
  if (DRY_RUN) return runDryRound(debater, round, totalRounds);

  const { agentId, clientId, stance, crdtIdOwn, crdtIdHistoryOwn, crdtIdRoundOwn, agentConfig, anthropic } = debater;

  console.log(`\n[Agent${agentId}] Round ${round}/${totalRounds} — ${stance}`);

  const tools = getMeridianTools(agentConfig, ALL_CRDT_IDS);

  const userPrompt = opponentArgCrdtId
    ? `You are Agent ${agentId} arguing ${stance} the debate topic. This is round ${round} of ${totalRounds}.

Your tasks:
1. Read "debate-topic" to recall the topic
2. Read "${opponentArgCrdtId}" to see your opponent's latest argument
3. Write a focused counter-argument (2-3 sentences) that directly addresses their point
4. Set "${crdtIdOwn}" to your argument text (client_id: ${clientId})
5. Add a JSON string {"round":${round},"text":"<your argument>"} to "${crdtIdHistoryOwn}" (client_id: ${clientId})
6. Increment "${crdtIdRoundOwn}" by 1 (client_id: ${clientId})`
    : `You are Agent ${agentId} arguing ${stance} the debate topic. This is round ${round} of ${totalRounds} — your opening statement.

Your tasks:
1. Read "debate-topic" to get the debate topic
2. Write a strong opening argument (2-3 sentences) for the ${stance} position
3. Set "${crdtIdOwn}" to your argument text (client_id: ${clientId})
4. Add a JSON string {"round":${round},"text":"<your argument>"} to "${crdtIdHistoryOwn}" (client_id: ${clientId})
5. Increment "${crdtIdRoundOwn}" by 1 (client_id: ${clientId})`;

  const messages: Anthropic.MessageParam[] = [{ role: "user", content: userPrompt }];

  let iteration = 0;
  while (iteration < 10) {
    iteration++;

    const response = await anthropic.messages.create({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 512,
      tools,
      messages,
    });

    const toolResults: Anthropic.ToolResultBlockParam[] = [];

    for (const block of response.content) {
      if (block.type === "text" && block.text.trim()) {
        console.log(`[Agent${agentId}] ${block.text.trim()}`);
      } else if (block.type === "tool_use") {
        const toolBlock = block as ToolUseBlock;
        console.log(`[Agent${agentId}] → ${toolBlock.name}(${JSON.stringify(toolBlock.input)})`);
        const result = await executeMeridianTool(agentConfig, toolBlock);
        console.log(`[Agent${agentId}] ← ${result}`);
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
}

async function main() {
  const baseUrl = (process.env.MERIDIAN_URL ?? "http://localhost:3000").replace(/^ws/, "http");
  const token = process.env.MERIDIAN_TOKEN;
  const apiKey = process.env.ANTHROPIC_API_KEY;
  const topic = process.env.DEBATE_TOPIC ?? "AI is net beneficial for humanity";
  const totalRounds = Number(process.env.DEBATE_ROUNDS ?? "3");

  if (!token) throw new Error("MERIDIAN_TOKEN is required");
  if (!apiKey) throw new Error("ANTHROPIC_API_KEY is required");

  const agentConfig: MeridianAgentConfig = { baseUrl, token, namespace: NAMESPACE };
  const anthropic = new Anthropic({ apiKey });

  console.log(`[debate] Topic: "${topic}"`);
  console.log(`[debate] Rounds: ${totalRounds}`);
  console.log(`[debate] Namespace: ${NAMESPACE}`);

  // Bootstrap — set topic and status
  await setLww(baseUrl, token, NAMESPACE, "debate-topic", 0, topic);
  await setLww(baseUrl, token, NAMESPACE, "debate-status", 0, "running");
  console.log(`[debate] State initialized — starting debate...`);

  const agent1: DebaterConfig = {
    agentId: 1, clientId: 1, stance: "FOR",
    crdtIdOwn: "debate-agent1-argument",
    crdtIdHistoryOwn: "debate-agent1-history",
    crdtIdRoundOwn: "debate-agent1-round",
    agentConfig, anthropic,
  };
  const agent2: DebaterConfig = {
    agentId: 2, clientId: 2, stance: "AGAINST",
    crdtIdOwn: "debate-agent2-argument",
    crdtIdHistoryOwn: "debate-agent2-history",
    crdtIdRoundOwn: "debate-agent2-round",
    agentConfig, anthropic,
  };

  for (let round = 1; round <= totalRounds; round++) {
    console.log(`\n${"=".repeat(60)}\nROUND ${round}/${totalRounds}\n${"=".repeat(60)}`);
    await runAgentRound(agent1, round, totalRounds, round > 1 ? "debate-agent2-argument" : null);
    await runAgentRound(agent2, round, totalRounds, "debate-agent1-argument");
  }

  await setLww(baseUrl, token, NAMESPACE, "debate-status", 0, "done");
  console.log(`\n[debate] Done. Open the UI → Debate tab to see the full transcript.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
