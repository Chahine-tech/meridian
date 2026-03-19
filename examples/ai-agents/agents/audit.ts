/**
 * Audit trail — reads the WAL history for all agent CRDTs and prints
 * a chronological log of every op applied during the agent run.
 *
 * Usage:
 *   MERIDIAN_URL=http://localhost:3000 MERIDIAN_ADMIN_TOKEN=xxx bun run agents/audit.ts
 */

const baseUrl = (process.env.MERIDIAN_URL ?? "http://localhost:3000").replace(/^ws/, "http");
const token = process.env.MERIDIAN_ADMIN_TOKEN;
const namespace = "ai-agents";

if (!token) {
  console.error("MERIDIAN_ADMIN_TOKEN is required.");
  process.exit(1);
}

const CRDT_IDS = ["agents:state", "coordinator:state", "agents:tasks-completed"];

interface HistoryEntry {
  seq: number;
  timestamp_ms: number;
  op: unknown;
}

interface HistoryResponse {
  crdt_id: string;
  entries: HistoryEntry[];
  next_seq: number | null;
}

async function fetchHistory(crdtId: string): Promise<HistoryEntry[]> {
  const entries: HistoryEntry[] = [];
  let sinceSeq = 0;

  while (true) {
    const url = `${baseUrl}/v1/namespaces/${namespace}/crdts/${encodeURIComponent(crdtId)}/history?since_seq=${sinceSeq}&limit=500`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });

    if (!res.ok) {
      throw new Error(`GET ${url} → ${res.status} ${res.statusText}`);
    }

    const data = (await res.json()) as HistoryResponse;
    entries.push(...data.entries);

    if (data.next_seq === null) break;
    sinceSeq = data.next_seq;
  }

  return entries;
}

async function main() {
  console.log(`\n=== Meridian Audit Trail — namespace: ${namespace} ===\n`);

  const all: Array<HistoryEntry & { crdt_id: string }> = [];

  for (const crdtId of CRDT_IDS) {
    const entries = await fetchHistory(crdtId);
    for (const e of entries) all.push({ ...e, crdt_id: crdtId });
  }

  // Sort chronologically
  all.sort((a, b) => a.timestamp_ms - b.timestamp_ms || a.seq - b.seq);

  if (all.length === 0) {
    console.log("No history found. Run the agents first.");
    return;
  }

  for (const entry of all) {
    const ts = new Date(entry.timestamp_ms).toISOString();
    const op = JSON.stringify(entry.op, null, 0);
    console.log(`[seq=${entry.seq}] ${ts}  ${entry.crdt_id}`);
    console.log(`  op: ${op}\n`);
  }

  console.log(`=== Total: ${all.length} ops ===`);
}

main().catch((err: unknown) => {
  console.error("Error:", err instanceof Error ? err.message : err);
  process.exit(1);
});
