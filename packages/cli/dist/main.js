#!/usr/bin/env node
import { Command, Options, Args } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Option } from "effect";
import { HttpClient } from "meridian-sdk";
const urlOption = Options.text("url").pipe(Options.withDescription("Meridian server base URL"), Options.withAlias("u"), Options.withDefault("http://localhost:3000"));
const tokenOption = Options.text("token").pipe(Options.withDescription("Bearer token for authentication"), Options.withAlias("t"));
const limitOption = Options.integer("limit").pipe(Options.withDescription("Number of history entries per page"), Options.withDefault(50));
const namespaceArg = Args.text({ name: "namespace" }).pipe(Args.withDescription("The Meridian namespace"));
const crdtIdArg = Args.text({ name: "crdt-id" }).pipe(Args.withDescription("The CRDT identifier"));
const inspectCommand = Command.make("inspect", { url: urlOption, token: tokenOption, namespace: namespaceArg, crdtId: crdtIdArg }, ({ url, token, namespace, crdtId }) => Effect.gen(function* () {
    const http = new HttpClient({ baseUrl: url, token });
    yield* Console.log(`Inspecting ${namespace}/${crdtId} at ${url}\n`);
    const result = yield* http.getCrdt(namespace, crdtId).pipe(Effect.mapError((e) => new Error(`Request failed: ${JSON.stringify(e)}`)));
    yield* Console.log(JSON.stringify(result, null, 2));
}).pipe(Effect.catchAll((e) => Console.error(String(e)))));
const fromSeqOption = Options.integer("from-seq").pipe(Options.withDescription("Start replaying from this WAL sequence number"), Options.withDefault(0));
const replayCommand = Command.make("replay", {
    url: urlOption,
    token: tokenOption,
    namespace: namespaceArg,
    crdtId: crdtIdArg,
    fromSeq: fromSeqOption,
    limit: limitOption,
}, ({ url, token, namespace, crdtId, fromSeq, limit }) => Effect.gen(function* () {
    const http = new HttpClient({ baseUrl: url, token });
    yield* Console.log(`Replaying WAL for ${namespace}/${crdtId} from seq=${fromSeq}\n`);
    yield* Console.log("seq        timestamp              op");
    yield* Console.log("─".repeat(72));
    let currentSeq = fromSeq;
    let hasMore = true;
    while (hasMore) {
        const response = yield* Effect.tryPromise({
            try: () => http.getHistory(namespace, crdtId, currentSeq, limit),
            catch: (e) => new Error(`Request failed: ${String(e)}`),
        });
        for (const entry of response.entries) {
            const ts = new Date(entry.timestamp_ms).toISOString();
            const op = JSON.stringify(entry.op);
            const opTrunc = op.length > 45 ? `${op.slice(0, 44)}…` : op;
            yield* Console.log(`#${String(entry.seq).padEnd(9)} ${ts}  ${opTrunc}`);
        }
        if (response.next_seq !== null && Option.isSome(Option.fromNullable(response.next_seq))) {
            currentSeq = response.next_seq;
        }
        else {
            hasMore = false;
        }
        if (response.entries.length === 0) {
            hasMore = false;
        }
    }
    yield* Console.log("\nDone.");
}).pipe(Effect.catchAll((e) => Console.error(String(e)))));
const meridianCommand = Command.make("meridian").pipe(Command.withDescription("Meridian CRDT CLI — inspect state and replay WAL history"), Command.withSubcommands([inspectCommand, replayCommand]));
const cli = Command.run(meridianCommand, {
    name: "meridian",
    version: "1.0.0",
});
cli(process.argv).pipe(Effect.provide(NodeContext.layer), NodeRuntime.runMain);
//# sourceMappingURL=main.js.map