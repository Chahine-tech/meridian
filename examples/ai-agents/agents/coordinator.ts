import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import type { CrdtMapValue } from "meridian-sdk";
import Anthropic from "@anthropic-ai/sdk";

const NAMESPACE = "ai-agents";
const HEARTBEAT_INTERVAL_MS = 15_000;
const PRESENCE_TTL_MS = 60_000;

function getLww(map: CrdtMapValue, key: string): string {
  return (map[key] as { value?: unknown })?.value as string ?? "";
}

function allWorkersDone(map: CrdtMapValue, total: number): boolean {
  for (let i = 1; i <= total; i++) {
    const status = getLww(map, `agent:${i}:status`);
    if (status !== "done" && status !== "error") return false;
  }
  return true;
}

async function main() {
  const url = process.env.MERIDIAN_URL ?? "ws://localhost:3000";
  const token = process.env.MERIDIAN_TOKEN!;
  const totalWorkers = Number(process.env.WORKER_TOTAL ?? "3");

  console.log("[Coordinator] Starting...");

  const client = await Effect.runPromise(
    MeridianClient.create({ url, namespace: NAMESPACE, token })
  );
  await client.waitForConnected(5000);

  const agentsState = client.crdtmap("agents:state");
  const coordinatorState = client.crdtmap("coordinator:state");
  const presence = client.presence<{ name: string; role: string }>("agents:presence");

  presence.heartbeat({ name: "Coordinator", role: "coordinator" }, PRESENCE_TTL_MS);
  const heartbeatTimer = setInterval(() => {
    presence.heartbeat({ name: "Coordinator", role: "coordinator" }, PRESENCE_TTL_MS);
  }, HEARTBEAT_INTERVAL_MS);

  coordinatorState.lwwSet("status", "waiting");
  console.log(`[Coordinator] Waiting for ${totalWorkers} workers to finish...`);

  // Wait for all workers via onChange
  await new Promise<void>((resolve) => {
    let unsub: (() => void) | null = null;
    const check = (value: CrdtMapValue) => {
      if (allWorkersDone(value, totalWorkers)) {
        unsub?.();
        resolve();
      }
    };
    unsub = agentsState.onChange(check);
    check(agentsState.value()); // immediate check — workers may already be done
  });

  console.log("[Coordinator] All workers done. Aggregating summaries...");
  coordinatorState.lwwSet("status", "aggregating");

  // Collect summaries
  const currentMap = agentsState.value();
  const summaries: string[] = [];
  for (let i = 1; i <= totalWorkers; i++) {
    const summary = getLww(currentMap, `agent:${i}:summary`);
    if (summary) summaries.push(`Section ${i}: ${summary}`);
  }

  try {
    const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    const message = await anthropic.messages.create({
      model: "claude-haiku-4-5",
      max_tokens: 1024,
      messages: [
        {
          role: "user",
          content: [
            "Multiple agents have each summarized a different section of a document.",
            "Combine these section summaries into a single coherent summary of the whole document.",
            "Write 3-5 sentences that flow naturally and capture the key themes.",
            "",
            ...summaries,
          ].join("\n"),
        },
      ],
    });

    const finalSummary =
      message.content[0]?.type === "text" ? message.content[0].text : "";

    coordinatorState.lwwSet("final-summary", finalSummary);
    coordinatorState.lwwSet("status", "done");

    console.log("[Coordinator] Final summary written:");
    console.log(finalSummary);
  } catch (err) {
    console.error("[Coordinator] Aggregation error:", err);
    coordinatorState.lwwSet("status", "error");
  } finally {
    clearInterval(heartbeatTimer);
    presence.leave();
    await new Promise((r) => setTimeout(r, 500));
    client.close();
  }
}

main().catch((err) => {
  console.error("[coordinator] Fatal:", err.message ?? err);
  process.exit(1);
});
