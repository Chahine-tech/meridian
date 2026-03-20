import { useState } from "react";
import { Effect } from "effect";
import { MeridianClient } from "meridian-sdk";
import { MeridianProvider } from "meridian-react";
import { AgentBoard } from "./AgentBoard.js";
import { ToolUseBoard } from "./ToolUseBoard.js";
import { MemoryBoard } from "./MemoryBoard.js";

interface ConnectForm {
  url: string;
  namespace: string;
  token: string;
}

function ConnectScreen({ onConnect }: { onConnect: (client: MeridianClient) => void }) {
  const [form, setForm] = useState<ConnectForm>({
    url: "ws://localhost:3000",
    namespace: "ai-agents",
    token: "",
  });
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError("");
    setLoading(true);

    const result = await Effect.runPromise(Effect.either(MeridianClient.create(form)));
    setLoading(false);

    if (result._tag === "Left") {
      setError(String(result.left));
      return;
    }

    onConnect(result.right);
  };

  return (
    <div style={{
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      height: "100%",
      background: "#0f0f0f",
    }}>
      <form onSubmit={handleSubmit} style={{
        display: "flex",
        flexDirection: "column",
        gap: 12,
        width: 320,
        padding: 24,
        background: "#18181b",
        borderRadius: 8,
        border: "1px solid #27272a",
      }}>
        <h1 style={{ fontSize: 16, fontWeight: 700, color: "#e2e8f0", marginBottom: 4 }}>
          Meridian — AI Agents
        </h1>
        {(["url", "namespace", "token"] as const).map((field) => (
          <div key={field} style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <label style={{ fontSize: 11, color: "#71717a", textTransform: "uppercase", letterSpacing: "0.05em" }}>
              {field}
            </label>
            <input
              type={field === "token" ? "password" : "text"}
              value={form[field]}
              onChange={(e) => setForm((f) => ({ ...f, [field]: e.target.value }))}
              required
              style={{
                padding: "6px 10px",
                background: "#0f0f0f",
                border: "1px solid #27272a",
                borderRadius: 4,
                color: "#e2e8f0",
                fontSize: 13,
                fontFamily: "monospace",
                outline: "none",
              }}
            />
          </div>
        ))}
        {error && <p style={{ color: "#f87171", fontSize: 12 }}>{error}</p>}
        <button
          type="submit"
          disabled={loading}
          style={{
            marginTop: 4,
            padding: "8px 0",
            background: "#7c3aed",
            color: "#fff",
            border: "none",
            borderRadius: 4,
            fontFamily: "monospace",
            fontWeight: 700,
            fontSize: 13,
            cursor: loading ? "not-allowed" : "pointer",
            opacity: loading ? 0.7 : 1,
          }}
        >
          {loading ? "Connecting…" : "Connect"}
        </button>
      </form>
    </div>
  );
}

type Tab = "agents" | "tool-use" | "memory";

const TAB_STYLE = (active: boolean): React.CSSProperties => ({
  padding: "6px 16px",
  background: active ? "#7c3aed" : "transparent",
  color: active ? "#fff" : "#71717a",
  border: "1px solid",
  borderColor: active ? "#7c3aed" : "#27272a",
  borderRadius: 4,
  fontFamily: "monospace",
  fontSize: 12,
  fontWeight: 700,
  cursor: "pointer",
});

export function App() {
  const [client, setClient] = useState<MeridianClient | null>(null);
  const [tab, setTab] = useState<Tab>("agents");

  if (!client) return <ConnectScreen onConnect={setClient} />;

  return (
    <MeridianProvider client={client}>
      <div style={{ background: "#0f0f0f", minHeight: "100%", color: "#e2e8f0", fontFamily: "monospace" }}>
        <div style={{ borderBottom: "1px solid #27272a", padding: "12px 24px", display: "flex", gap: 8 }}>
          <button style={TAB_STYLE(tab === "agents")} onClick={() => setTab("agents")}>
            Multi-Agent
          </button>
          <button style={TAB_STYLE(tab === "tool-use")} onClick={() => setTab("tool-use")}>
            Tool Use
          </button>
          <button style={TAB_STYLE(tab === "memory")} onClick={() => setTab("memory")}>
            Memory
          </button>
        </div>
        {tab === "agents" ? <AgentBoard totalWorkers={3} /> : tab === "tool-use" ? <ToolUseBoard /> : <MemoryBoard />}
      </div>
    </MeridianProvider>
  );
}
