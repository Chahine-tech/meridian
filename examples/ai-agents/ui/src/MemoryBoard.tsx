import { useGCounter, useLwwRegister, useORSet } from "meridian-react";

export function MemoryBoard() {
  const { value: runCount } = useGCounter("memory-run-count");
  const { value: lastRun } = useLwwRegister<Record<string, unknown>>("memory-last-run");
  const { elements: insights } = useORSet<string>("memory-insights");

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      gap: 24,
      padding: 24,
      maxWidth: 960,
      margin: "0 auto",
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h1 style={{ fontSize: 20, fontWeight: 700 }}>Agent Memory — Live State</h1>
        <span style={{ fontSize: 12, color: "#52525b" }}>
          namespace: <span style={{ color: "#7c3aed" }}>ai-agents</span>
        </span>
      </div>

      <p style={{ fontSize: 13, color: "#52525b", fontStyle: "italic", marginTop: -12 }}>
        Run <code style={{ color: "#a78bfa", background: "#1e1e2e", padding: "1px 6px", borderRadius: 3 }}>bun run agents/memory.ts</code> multiple times — each run reads and extends the memory from the previous one.
      </p>

      {/* Run counter */}
      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: 20,
        display: "flex",
        alignItems: "center",
        gap: 20,
      }}>
        <div style={{ fontSize: 48, fontWeight: 800, color: "#7c3aed", lineHeight: 1 }}>
          {runCount ?? 0}
        </div>
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: "#e2e8f0" }}>Total runs</div>
          <div style={{ fontSize: 12, color: "#52525b" }}>memory-run-count (GCounter)</div>
        </div>
      </div>

      {/* Last run */}
      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: 16,
      }}>
        <div style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 12 }}>
          Last run summary — memory-last-run (LWW Register)
        </div>
        {lastRun ? (
          <pre style={{ fontSize: 12, color: "#e2e8f0", margin: 0, whiteSpace: "pre-wrap", lineHeight: 1.6 }}>
            {JSON.stringify(lastRun, null, 2)}
          </pre>
        ) : (
          <p style={{ fontSize: 13, color: "#52525b", fontStyle: "italic", margin: 0 }}>
            No run yet — start the agent to populate memory.
          </p>
        )}
      </div>

      {/* Insights */}
      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: 16,
      }}>
        <div style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 12 }}>
          Accumulated insights — memory-insights (ORSet · {insights?.length ?? 0} entries)
        </div>
        {insights && insights.length > 0 ? (
          <ol style={{ margin: 0, padding: "0 0 0 18px", display: "flex", flexDirection: "column", gap: 8 }}>
            {insights.map((insight: string, i: number) => (
              <li key={i} style={{ fontSize: 13, color: "#e2e8f0", lineHeight: 1.5, borderLeft: "2px solid #7c3aed", paddingLeft: 10, listStyle: "none", marginLeft: -10 }}>
                {String(insight)}
              </li>
            ))}
          </ol>
        ) : (
          <p style={{ fontSize: 13, color: "#52525b", fontStyle: "italic", margin: 0 }}>
            No insights yet — each run adds one.
          </p>
        )}
      </div>
    </div>
  );
}
