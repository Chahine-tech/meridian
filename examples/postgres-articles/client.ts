/**
 * postgres-articles — live subscriber
 *
 * Connects to a Meridian server backed by Postgres, subscribes to all
 * CRDT columns of the articles table, and prints every incoming delta
 * to the terminal in real time.
 *
 * Usage:
 *   TOKEN=<your-token> npx tsx client.ts
 *
 * Or with a custom server URL:
 *   MERIDIAN_URL=http://localhost:3000 TOKEN=<token> npx tsx client.ts
 */

import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";

const MERIDIAN_URL = process.env.MERIDIAN_URL ?? "http://localhost:3000";
const TOKEN = process.env.TOKEN;
const NAMESPACE = "articles";

// Article IDs to subscribe to — matches the seed.sql inserts.
const ARTICLE_IDS = ["rust-intro", "crdt-explained", "postgres-tips"];

if (!TOKEN) {
  console.error("TOKEN env var is required.");
  console.error("Generate one with:");
  console.error("  cargo run --bin meridian -- token --namespace articles");
  process.exit(1);
}

function log(articleId: string, col: string, value: unknown) {
  const ts = new Date().toISOString().slice(11, 23);
  console.log(`[${ts}] ${articleId}.${col} →`, value);
}

const program = Effect.gen(function* () {
  const client = yield* MeridianClient.create({
    url: MERIDIAN_URL,
    namespace: NAMESPACE,
    token: TOKEN!,
  });

  console.log(`Connected to ${MERIDIAN_URL} (namespace: ${NAMESPACE})`);
  console.log(`Subscribing to ${ARTICLE_IDS.length} articles…\n`);

  for (const id of ARTICLE_IDS) {
    // crdt_id format mirrors the trigger: "article:<id>:<col>"
    const prefix = `article:${id}`;

    // GCounter — view count
    const views = client.gcounter(`${prefix}:views`);
    views.onChange((v) => log(id, "views", v));

    // PNCounter — likes / dislikes
    const likes = client.pncounter(`${prefix}:likes`);
    likes.onChange((v) => log(id, "likes", v));

    // ORSet — tags
    const tags = client.orset<string>(`${prefix}:tags`);
    tags.onChange((v) => log(id, "tags", v));

    // LwwRegister — headline
    const headline = client.lwwregister<string>(`${prefix}:headline`);
    headline.onChange((v) => log(id, "headline", v));

    // RGA — collaborative body text (write via SDK only)
    const body = client.rga(`${prefix}:body`);
    body.onChange((v) => log(id, "body", v));
  }

  console.log("Listening for updates. Run simulate.sql in another terminal.\n");
  console.log("Press Ctrl-C to exit.\n");

  // Keep the process alive.
  yield* Effect.never;
});

Effect.runFork(program);
