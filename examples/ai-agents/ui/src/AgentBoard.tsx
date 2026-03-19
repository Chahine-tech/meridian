import { useCRDTMap, usePresence, useGCounter } from "meridian-react";
import type { CrdtMapValue } from "meridian-sdk";

type AgentStatus = "idle" | "processing" | "done" | "error";

const STATUS_COLORS: Record<AgentStatus, string> = {
  idle: "#71717a",
  processing: "#eab308",
  done: "#22c55e",
  error: "#ef4444",
};

function getLww(map: CrdtMapValue, key: string): string {
  return (map[key] as { value?: unknown })?.value as string ?? "";
}

function StatusBadge({ status }: { status: string }) {
  const color = STATUS_COLORS[status as AgentStatus] ?? STATUS_COLORS.idle;
  const isPulsing = status === "processing" || status === "aggregating";
  return (
    <span style={{
      background: color,
      borderRadius: 4,
      padding: "2px 8px",
      fontSize: 11,
      fontWeight: 700,
      color: "#0f0f0f",
      animation: isPulsing ? "pulse 1.5s ease-in-out infinite" : "none",
    }}>
      {status || "idle"}
    </span>
  );
}

interface WorkerData {
  id: string;
  status: string;
  chunk: string;
  summary: string;
  model: string;
}

function WorkerCard({ worker }: { worker: WorkerData }) {
  return (
    <div style={{
      background: "#18181b",
      border: "1px solid #27272a",
      borderRadius: 8,
      padding: 16,
      display: "flex",
      flexDirection: "column",
      gap: 10,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <span style={{ fontWeight: 700, fontSize: 14 }}>Worker-{worker.id}</span>
        <StatusBadge status={worker.status} />
      </div>
      {worker.model && (
        <div style={{ fontSize: 11, color: "#71717a" }}>{worker.model}</div>
      )}
      {worker.chunk && (
        <details style={{ fontSize: 12, color: "#a1a1aa" }}>
          <summary style={{ cursor: "pointer", userSelect: "none", marginBottom: 4 }}>
            Assigned chunk ({worker.chunk.length} chars)
          </summary>
          <p style={{ whiteSpace: "pre-wrap", marginTop: 4, lineHeight: 1.5 }}>
            {worker.chunk.slice(0, 300)}{worker.chunk.length > 300 ? "…" : ""}
          </p>
        </details>
      )}
      {worker.summary ? (
        <p style={{ fontSize: 13, color: "#e2e8f0", lineHeight: 1.6, borderLeft: "2px solid #7c3aed", paddingLeft: 10 }}>
          {worker.summary}
        </p>
      ) : worker.status === "processing" ? (
        <p style={{ fontSize: 12, color: "#71717a", fontStyle: "italic" }}>Summarizing…</p>
      ) : null}
    </div>
  );
}

export function AgentBoard({ totalWorkers }: { totalWorkers: number }) {
  const { value: agentsState } = useCRDTMap("agents:state");
  const { value: coordinatorState } = useCRDTMap("coordinator:state");
  const { value: tasksCompleted } = useGCounter("agents:tasks-completed");
  const { online } = usePresence<{ name: string; role: string }>("agents:presence");

  const workers: WorkerData[] = Array.from({ length: totalWorkers }, (_, i) => {
    const id = String(i + 1);
    return {
      id,
      status: getLww(agentsState, `agent:${id}:status`) || "idle",
      chunk: getLww(agentsState, `agent:${id}:chunk`),
      summary: getLww(agentsState, `agent:${id}:summary`),
      model: getLww(agentsState, `agent:${id}:model`),
    };
  });

  const coordStatus = getLww(coordinatorState, "status") || "waiting";
  const finalSummary = getLww(coordinatorState, "final-summary");

  const progress = Math.min(tasksCompleted, totalWorkers);

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      gap: 24,
      padding: 24,
      maxWidth: 960,
      margin: "0 auto",
    }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1 style={{ fontSize: 20, fontWeight: 700 }}>Collaborative AI Agents</h1>
        <div style={{ display: "flex", flexDirection: "column", alignItems: "flex-end", gap: 4 }}>
          <div style={{ fontSize: 12, color: "#71717a" }}>
            {progress}/{totalWorkers} tasks completed
          </div>
          <div style={{
            width: 200,
            height: 6,
            background: "#27272a",
            borderRadius: 3,
            overflow: "hidden",
          }}>
            <div style={{
              width: `${(progress / totalWorkers) * 100}%`,
              height: "100%",
              background: "#7c3aed",
              borderRadius: 3,
              transition: "width 0.4s ease",
            }} />
          </div>
        </div>
      </div>

      {/* Online agents */}
      {online.length > 0 && (
        <div>
          <h2 style={{ fontSize: 13, color: "#71717a", marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.05em" }}>
            Online ({online.length})
          </h2>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {online.map((entry) => (
              <div key={entry.clientId} style={{
                padding: "4px 12px",
                background: "#18181b",
                border: "1px solid #27272a",
                borderRadius: 20,
                fontSize: 12,
                color: "#a1a1aa",
              }}>
                <span style={{ color: "#7c3aed", marginRight: 6 }}>●</span>
                {entry.data.name}
                <span style={{ color: "#52525b", marginLeft: 6 }}>({entry.data.role})</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Worker cards */}
      <div>
        <h2 style={{ fontSize: 13, color: "#71717a", marginBottom: 12, textTransform: "uppercase", letterSpacing: "0.05em" }}>
          Workers
        </h2>
        <div style={{
          display: "grid",
          gridTemplateColumns: `repeat(${totalWorkers}, 1fr)`,
          gap: 16,
        }}>
          {workers.map((w) => <WorkerCard key={w.id} worker={w} />)}
        </div>
      </div>

      {/* Coordinator panel */}
      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: 20,
      }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
          <h2 style={{ fontSize: 14, fontWeight: 700 }}>Coordinator</h2>
          <StatusBadge status={coordStatus} />
        </div>
        {finalSummary ? (
          <p style={{ fontSize: 14, color: "#e2e8f0", lineHeight: 1.7, borderLeft: "2px solid #22c55e", paddingLeft: 12 }}>
            {finalSummary}
          </p>
        ) : (
          <p style={{ fontSize: 13, color: "#52525b", fontStyle: "italic" }}>
            {coordStatus === "waiting"
              ? "Waiting for workers to finish…"
              : coordStatus === "aggregating"
              ? "Aggregating summaries…"
              : "No final summary yet."}
          </p>
        )}
      </div>
    </div>
  );
}
