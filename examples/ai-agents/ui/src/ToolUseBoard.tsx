import { useGCounter, useLwwRegister, useORSet } from "meridian-react";

function StatCard({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{
      background: "#18181b",
      border: "1px solid #27272a",
      borderRadius: 8,
      padding: 16,
      display: "flex",
      flexDirection: "column",
      gap: 8,
    }}>
      <div style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em" }}>
        {label}
      </div>
      <div style={{ fontSize: 14, color: "#e2e8f0", wordBreak: "break-word" }}>
        {children}
      </div>
    </div>
  );
}

export function ToolUseBoard() {
  const { value: views } = useGCounter("views");
  const { value: notes } = useLwwRegister<string>("notes");
  const { elements: tags } = useORSet<string>("tags");

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
        <h1 style={{ fontSize: 20, fontWeight: 700 }}>Claude Tool Use — Live State</h1>
        <span style={{ fontSize: 12, color: "#52525b" }}>
          namespace: <span style={{ color: "#7c3aed" }}>ai-agents</span>
        </span>
      </div>

      <p style={{ fontSize: 13, color: "#52525b", fontStyle: "italic", marginTop: -12 }}>
        Run <code style={{ color: "#a78bfa", background: "#1e1e2e", padding: "1px 6px", borderRadius: 3 }}>bun run agents/tool-use.ts</code> to see Claude interact with these CRDTs in real time.
      </p>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
        <StatCard label="views (GCounter)">
          <span style={{ fontSize: 28, fontWeight: 700, color: "#7c3aed" }}>
            {views ?? 0}
          </span>
        </StatCard>

        <StatCard label="notes (LWW Register)">
          {notes
            ? <span style={{ fontStyle: "normal" }}>{String(notes)}</span>
            : <span style={{ color: "#52525b", fontStyle: "italic" }}>empty</span>
          }
        </StatCard>

        <StatCard label="tags (ORSet)">
          {tags && tags.length > 0
            ? (
              <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                {tags.map((tag, i) => (
                  <span key={i} style={{
                    background: "#1e1e2e",
                    border: "1px solid #7c3aed",
                    borderRadius: 4,
                    padding: "2px 8px",
                    fontSize: 12,
                    color: "#a78bfa",
                  }}>
                    {String(tag)}
                  </span>
                ))}
              </div>
            )
            : <span style={{ color: "#52525b", fontStyle: "italic" }}>empty</span>
          }
        </StatCard>
      </div>

      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: 16,
      }}>
        <div style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 12 }}>
          Raw CRDT state
        </div>
        <pre style={{ fontSize: 12, color: "#a1a1aa", margin: 0, overflow: "auto" }}>
          {JSON.stringify({ views: views ?? 0, notes: notes ?? null, tags: tags ?? [] }, null, 2)}
        </pre>
      </div>
    </div>
  );
}
