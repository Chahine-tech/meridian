import { useLwwRegister, useGCounter, useORSet } from "meridian-react";

const TOTAL_ROUNDS = 3;

interface RoundEntry {
  round: number;
  text: string;
}

function parseHistory(elements: string[]): RoundEntry[] {
  return elements
    .map((el) => { try { return JSON.parse(el) as RoundEntry; } catch { return null; } })
    .filter((e): e is RoundEntry => e !== null && typeof e.round === "number")
    .sort((a, b) => a.round - b.round);
}

function StatusBadge({ status }: { status: string | null }) {
  const color = status === "running" ? "#eab308" : status === "done" ? "#22c55e" : "#52525b";
  return (
    <span style={{
      background: color,
      borderRadius: 4,
      padding: "2px 8px",
      fontSize: 11,
      fontWeight: 700,
      color: "#0f0f0f",
    }}>
      {status ?? "idle"}
    </span>
  );
}

function AgentColumn({
  label,
  stance,
  current,
  roundsDone,
  history,
  color,
}: {
  label: string;
  stance: string;
  current: string | null;
  roundsDone: number;
  history: RoundEntry[];
  color: string;
}) {
  return (
    <div style={{
      flex: 1,
      display: "flex",
      flexDirection: "column",
      gap: 12,
      minWidth: 0,
    }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <span style={{ fontWeight: 700, fontSize: 14 }}>{label}</span>
          <span style={{
            marginLeft: 8,
            fontSize: 11,
            fontWeight: 700,
            color,
            background: color + "22",
            borderRadius: 4,
            padding: "1px 6px",
          }}>
            {stance}
          </span>
        </div>
        <span style={{ fontSize: 11, color: "#71717a" }}>
          Round {roundsDone}/{TOTAL_ROUNDS}
        </span>
      </div>

      {/* Current argument */}
      <div style={{
        background: "#18181b",
        border: `1px solid ${color}44`,
        borderLeft: `3px solid ${color}`,
        borderRadius: 6,
        padding: 12,
        minHeight: 80,
      }}>
        <div style={{ fontSize: 10, color: "#52525b", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 6 }}>
          Current argument
        </div>
        {current
          ? <p style={{ fontSize: 13, color: "#e2e8f0", lineHeight: 1.6, margin: 0 }}>{current}</p>
          : <p style={{ fontSize: 12, color: "#52525b", fontStyle: "italic", margin: 0 }}>Waiting...</p>
        }
      </div>

      {/* History */}
      <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
        <div style={{ fontSize: 10, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          Transcript ({history.length} rounds)
        </div>
        {history.length > 0
          ? history.map((entry) => (
            <div key={entry.round} style={{
              background: "#18181b",
              border: "1px solid #27272a",
              borderRadius: 6,
              padding: 10,
            }}>
              <div style={{ fontSize: 10, color: color, fontWeight: 700, marginBottom: 4 }}>
                Round {entry.round}
              </div>
              <p style={{ fontSize: 12, color: "#a1a1aa", lineHeight: 1.5, margin: 0 }}>
                {entry.text}
              </p>
            </div>
          ))
          : <p style={{ fontSize: 12, color: "#52525b", fontStyle: "italic" }}>No arguments yet.</p>
        }
      </div>
    </div>
  );
}

export function DebateBoard() {
  const { value: topic } = useLwwRegister<string>("debate-topic");
  const { value: status } = useLwwRegister<string>("debate-status");
  const { value: agent1Arg } = useLwwRegister<string>("debate-agent1-argument");
  const { value: agent2Arg } = useLwwRegister<string>("debate-agent2-argument");
  const { value: agent1Round } = useGCounter("debate-agent1-round");
  const { value: agent2Round } = useGCounter("debate-agent2-round");
  const { elements: agent1History } = useORSet<string>("debate-agent1-history");
  const { elements: agent2History } = useORSet<string>("debate-agent2-history");

  const history1 = parseHistory(agent1History);
  const history2 = parseHistory(agent2History);

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      gap: 20,
      padding: 24,
      maxWidth: 1100,
      margin: "0 auto",
    }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
        <div>
          <h1 style={{ fontSize: 20, fontWeight: 700, margin: 0 }}>AI Debate — Live</h1>
          <p style={{ fontSize: 13, color: "#52525b", margin: "4px 0 0" }}>
            Run{" "}
            <code style={{ color: "#a78bfa", background: "#1e1e2e", padding: "1px 6px", borderRadius: 3 }}>
              bun run agents/debate.ts
            </code>{" "}
            to start a debate between two Claude agents.
          </p>
        </div>
        <StatusBadge status={status} />
      </div>

      {/* Topic */}
      <div style={{
        background: "#18181b",
        border: "1px solid #27272a",
        borderRadius: 8,
        padding: "12px 16px",
      }}>
        <span style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em" }}>
          Topic
        </span>
        <p style={{ fontSize: 15, fontWeight: 600, color: "#e2e8f0", margin: "4px 0 0" }}>
          {topic ?? <span style={{ color: "#52525b", fontStyle: "italic" }}>No debate started yet</span>}
        </p>
      </div>

      {/* Two-column debate */}
      <div style={{ display: "flex", gap: 20, alignItems: "flex-start" }}>
        <AgentColumn
          label="Agent 1"
          stance="FOR"
          current={agent1Arg}
          roundsDone={agent1Round ?? 0}
          history={history1}
          color="#7c3aed"
        />
        <div style={{ width: 1, background: "#27272a", alignSelf: "stretch" }} />
        <AgentColumn
          label="Agent 2"
          stance="AGAINST"
          current={agent2Arg}
          roundsDone={agent2Round ?? 0}
          history={history2}
          color="#dc2626"
        />
      </div>
    </div>
  );
}
