import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import Anthropic from "@anthropic-ai/sdk";

const NAMESPACE = "ai-agents";
const HEARTBEAT_INTERVAL_MS = 15_000;
const PRESENCE_TTL_MS = 60_000;

async function main() {
  const url = process.env.MERIDIAN_URL ?? "ws://localhost:3000";
  const token = process.env.MERIDIAN_TOKEN!;
  const workerId = process.env.WORKER_ID!;
  const chunk = process.env.WORKER_CHUNK!;

  console.log(`[Worker-${workerId}] Starting...`);

  const client = await Effect.runPromise(
    MeridianClient.create({ url, namespace: NAMESPACE, token })
  );
  await client.waitForConnected(5000);

  const agentsState = client.crdtmap("agents:state");
  const tasksCompleted = client.gcounter("agents:tasks-completed");
  const presence = client.presence<{ name: string; role: string }>("agents:presence");

  // Announce presence
  presence.heartbeat({ name: `Worker-${workerId}`, role: "worker" }, PRESENCE_TTL_MS);
  const heartbeatTimer = setInterval(() => {
    presence.heartbeat({ name: `Worker-${workerId}`, role: "worker" }, PRESENCE_TTL_MS);
  }, HEARTBEAT_INTERVAL_MS);

  // Write initial state
  agentsState.lwwSet(`agent:${workerId}:status`, "processing");
  agentsState.lwwSet(`agent:${workerId}:chunk`, chunk);
  agentsState.lwwSet(`agent:${workerId}:model`, "claude-haiku-4-5");

  try {
    console.log(`[Worker-${workerId}] Summarizing chunk (${chunk.length} chars)...`);

    const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
    const message = await anthropic.messages.create({
      model: "claude-haiku-4-5",
      max_tokens: 512,
      messages: [
        {
          role: "user",
          content: `Summarize the following text in 2-3 concise sentences:\n\n${chunk}`,
        },
      ],
    });

    const summary =
      message.content[0]?.type === "text" ? message.content[0].text : "";

    agentsState.lwwSet(`agent:${workerId}:summary`, summary);
    agentsState.lwwSet(`agent:${workerId}:status`, "done");
    tasksCompleted.increment(1);

    console.log(`[Worker-${workerId}] Done. Summary: ${summary.slice(0, 100)}...`);
  } catch (err) {
    console.error(`[Worker-${workerId}] Error:`, err);
    agentsState.lwwSet(`agent:${workerId}:status`, "error");
  } finally {
    clearInterval(heartbeatTimer);
    presence.leave();
    await new Promise((r) => setTimeout(r, 500));
    client.close();
  }
}

main().catch((err) => {
  console.error("[worker] Fatal:", err.message ?? err);
  process.exit(1);
});
