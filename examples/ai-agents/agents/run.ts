import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";

const NAMESPACE = "ai-agents";
const WORKER_COUNT = 3;

const SAMPLE_DOCUMENT = `
Artificial intelligence has transformed nearly every industry over the past decade. From healthcare
to finance, AI systems are now capable of performing tasks that once required significant human
expertise. Machine learning algorithms can detect diseases in medical images with accuracy
rivaling experienced physicians, while natural language processing systems can understand and
generate human text with remarkable fluency.

The development of large language models has been particularly significant. These models, trained
on vast corpora of text data, have demonstrated emergent capabilities that surprised even their
creators. They can reason across domains, write code, compose music, and engage in nuanced
conversation. Companies like Anthropic, OpenAI, and Google have invested billions of dollars
into this research, racing to build ever more capable systems.

However, this rapid advancement has also raised profound questions about safety, alignment, and
societal impact. Researchers worry about systems that might pursue goals misaligned with human
values, or be used maliciously for disinformation, surveillance, or autonomous weapons. Governments
around the world are scrambling to develop regulatory frameworks that can keep pace with the
technology without stifling beneficial innovation.

The economic implications are equally complex. While AI promises tremendous productivity gains and
new capabilities, it also threatens to displace millions of workers in sectors ranging from
transportation to legal services to creative industries. Some economists predict a new era of
abundance; others warn of unprecedented inequality if the benefits accrue only to those who own
the technology.

Looking forward, the next decade will likely bring AI systems that are far more capable than today.
Multimodal models that seamlessly integrate vision, language, and action are already emerging.
Agentic systems that can autonomously plan and execute complex multi-step tasks are moving from
research labs into production. The question is no longer whether AI will transform civilization,
but how we choose to shape that transformation for the benefit of all humanity.
`.trim();

function loadDocument(): string {
  const fileFlag = process.argv.indexOf("--file");
  if (fileFlag !== -1) {
    const path = process.argv[fileFlag + 1];
    if (path) return Bun.file(path).text() as unknown as string;
  }
  return SAMPLE_DOCUMENT;
}

function splitIntoChunks(doc: string, n: number): string[] {
  const paragraphs = doc.split(/\n\n+/).filter(Boolean);
  const chunks: string[] = Array.from({ length: n }, () => "");
  paragraphs.forEach((p, i) => {
    chunks[i % n] += (chunks[i % n] ? "\n\n" : "") + p;
  });
  return chunks.filter(Boolean);
}

async function main() {
  const url = process.env.MERIDIAN_URL ?? "ws://localhost:3000";
  const adminToken = process.env.MERIDIAN_ADMIN_TOKEN;
  if (!adminToken) {
    throw new Error("MERIDIAN_ADMIN_TOKEN is required. Set it in your environment.");
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    throw new Error("ANTHROPIC_API_KEY is required. Set it in your environment.");
  }

  console.log(`[run] Connecting to Meridian at ${url}...`);
  const bootstrapClient = await Effect.runPromise(
    MeridianClient.create({ url, namespace: NAMESPACE, token: adminToken })
  );

  await bootstrapClient.waitForConnected(5000).catch(() => {
    throw new Error(`Meridian server not reachable at ${url}. Is it running?`);
  });
  console.log("[run] Connected.");

  const doc = await Promise.resolve(loadDocument());
  const chunks = splitIntoChunks(doc, WORKER_COUNT);
  console.log(`[run] Document split into ${chunks.length} chunks.`);

  // Issue tokens for each worker (client_id 1..N) + coordinator (N+1)
  const permissions = {
    read: ["*"],
    write: ["agents:state", "coordinator:state", "agents:tasks-completed", "agents:presence"],
    admin: false,
  };

  console.log("[run] Issuing tokens...");
  const workerTokens = await Promise.all(
    chunks.map((_, i) =>
      Effect.runPromise(
        bootstrapClient.http.issueToken(NAMESPACE, { client_id: i + 1, ttl_ms: 600_000, permissions })
      ).then((r) => r.token)
    )
  );
  const coordinatorToken = await Effect.runPromise(
    bootstrapClient.http.issueToken(NAMESPACE, { client_id: WORKER_COUNT + 1, ttl_ms: 600_000, permissions })
  ).then((r) => r.token);

  bootstrapClient.close();

  // Spawn workers
  chunks.forEach((chunk, i) => {
    const workerId = String(i + 1);
    console.log(`[run] Spawning Worker-${workerId}...`);
    const proc = Bun.spawn(["bun", "run", "agents/worker.ts"], {
      env: {
        ...process.env,
        MERIDIAN_URL: url,
        MERIDIAN_TOKEN: workerTokens[i],
        WORKER_ID: workerId,
        WORKER_CHUNK: chunk,
        WORKER_TOTAL: String(WORKER_COUNT),
      },
      stdout: "inherit",
      stderr: "inherit",
    });
    proc.exited.then((code: number) => {
      console.log(`[run] Worker-${workerId} exited with code ${code}`);
    });
  });

  // Spawn coordinator
  console.log("[run] Spawning Coordinator...");
  const coordProc = Bun.spawn(["bun", "run", "agents/coordinator.ts"], {
    env: {
      ...process.env,
      MERIDIAN_URL: url,
      MERIDIAN_TOKEN: coordinatorToken,
      WORKER_TOTAL: String(WORKER_COUNT),
    },
    stdout: "inherit",
    stderr: "inherit",
  });
  coordProc.exited.then((code) => {
    console.log(`[run] Coordinator exited with code ${code}`);
  });

  console.log("[run] All agents spawned. Open the UI at http://localhost:5175");
}

main().catch((err) => {
  console.error("[run] Fatal error:", err);
  process.exit(1);
});
