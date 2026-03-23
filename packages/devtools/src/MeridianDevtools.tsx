import { useState, useEffect } from "react";
import type { ComponentType } from "react";
import type { MeridianClient, ClientSnapshot, CRDTSnapshotEntry, DeltaEvent } from "meridian-sdk";
import { useDevtoolsState } from "./useDevtoolsState.js";
import { useHistory } from "./useHistory.js";
import { styles, wsStateColor } from "./styles.js";

interface MeridianDevtoolsProps {
  client: MeridianClient;
  defaultOpen?: boolean;
}

type Tab = "crdts" | "events" | "history";

function CRDTRow({ entry, onSelect, selected }: { entry: CRDTSnapshotEntry; onSelect: () => void; selected: boolean }) {
  const valueDisplay = (() => {
    switch (entry.type) {
      case "gcounter":   return JSON.stringify({ value: entry.value, counts: entry.counts });
      case "pncounter":  return JSON.stringify({ value: entry.value });
      case "orset":      return JSON.stringify({ elements: entry.elements });
      case "lwwregister":return JSON.stringify({ value: entry.value, meta: entry.meta });
      case "presence":   return JSON.stringify({ online: entry.online });
      case "crdtmap":    return JSON.stringify({ value: entry.value });
    }
  })();

  return (
    <button
      type="button"
      onClick={onSelect}
      style={{ ...styles.crdtRow, cursor: "pointer", backgroundColor: selected ? "#1c1917" : "transparent", outline: "none", width: "100%", textAlign: "left", border: "none", padding: "4px 0" }}
    >
      <span style={styles.typeBadge(entry.type)}>{entry.type}</span>
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={styles.crdtId}>{entry.crdtId}</div>
        <div style={styles.crdtValue}>{valueDisplay}</div>
      </div>
    </button>
  );
}

function useLatencyStats(client: MeridianClient) {
  const [stats, setStats] = useState<{ p50: number; p99: number; count: number } | null>(null);
  useEffect(() => {
    const id = setInterval(() => {
      setStats(client.getLatencyStats());
    }, 1000);
    return () => { clearInterval(id); };
  }, [client]);
  return stats;
}

function ConnectionSection({ snapshot, client }: { snapshot: ClientSnapshot; client: MeridianClient }) {
  const latency = useLatencyStats(client);
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>Connection</div>
      <div style={styles.statusRow}>
        <span style={styles.badge(wsStateColor(snapshot.wsState))}>{snapshot.wsState}</span>
        <span style={{ color: "#a1a1aa" }}>ns: {snapshot.namespace}</span>
        <span style={{ color: "#a1a1aa" }}>id: {snapshot.clientId}</span>
      </div>
      {snapshot.pendingOpCount > 0 && (
        <div style={{ marginTop: 4, color: "#fbbf24", fontSize: 11 }}>
          ⏳ {snapshot.pendingOpCount} op{snapshot.pendingOpCount > 1 ? "s" : ""} pending
        </div>
      )}
      {latency !== null && (
        <div style={{ marginTop: 6, display: "flex", gap: 12 }}>
          <span style={{ fontSize: 11, color: "#a1a1aa" }}>
            Op latency — <span style={{ color: "#4ade80" }}>p50: {latency.p50.toFixed(1)}ms</span>
            {" · "}
            <span style={{ color: latency.p99 > 200 ? "#f87171" : "#facc15" }}>p99: {latency.p99.toFixed(1)}ms</span>
            {" · "}
            <span style={{ color: "#71717a" }}>{latency.count} samples</span>
          </span>
        </div>
      )}
    </div>
  );
}

function CRDTsSection({ crdts, selectedId, onSelect }: { crdts: CRDTSnapshotEntry[]; selectedId: string | null; onSelect: (id: string) => void }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>CRDTs ({crdts.length})</div>
      {crdts.length === 0 ? (
        <div style={{ color: "#52525b", fontStyle: "italic" }}>No active CRDTs</div>
      ) : (
        crdts.map((entry) => (
          <CRDTRow
            key={entry.crdtId}
            entry={entry}
            selected={entry.crdtId === selectedId}
            onSelect={() => onSelect(entry.crdtId)}
          />
        ))
      )}
    </div>
  );
}

function EventsSection({ events }: { events: DeltaEvent[] }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>Events ({events.length})</div>
      {events.length === 0 ? (
        <div style={{ color: "#52525b", fontStyle: "italic" }}>No events yet</div>
      ) : (
        [...events].reverse().map((e) => (
          <div key={`${e.at}-${e.crdtId}`} style={{ display: "flex", alignItems: "center", gap: 6, padding: "2px 0", borderBottom: "1px solid #18181b" }}>
            <span style={styles.typeBadge(e.type)}>{e.type}</span>
            <span style={{ color: "#a78bfa", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{e.crdtId}</span>
            <span style={{ color: "#52525b", fontSize: 10 }}>{new Date(e.at).toLocaleTimeString()}</span>
          </div>
        ))
      )}
    </div>
  );
}

function HistorySection({ client, selectedCrdtId, onSelectCrdt, crdts }: {
  client: MeridianClient;
  selectedCrdtId: string | null;
  onSelectCrdt: (id: string) => void;
  crdts: CRDTSnapshotEntry[];
}) {
  const { states, load, loadMore } = useHistory(client);
  const state = selectedCrdtId ? states[selectedCrdtId] : null;

  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>Time travel — WAL history</div>

      {/* CRDT picker */}
      <div style={{ display: "flex", gap: 4, flexWrap: "wrap", marginBottom: 8 }}>
        {crdts.map(c => (
          <button
            key={c.crdtId}
            type="button"
            onClick={() => { onSelectCrdt(c.crdtId); void load(c.crdtId); }}
            style={{
              background: selectedCrdtId === c.crdtId ? "#7c3aed" : "#27272a",
              color: "#e4e4e7",
              border: "none",
              borderRadius: 4,
              padding: "2px 7px",
              fontSize: 10,
              cursor: "pointer",
              fontFamily: "monospace",
            }}
          >
            {c.crdtId}
          </button>
        ))}
        {crdts.length === 0 && <span style={{ color: "#52525b", fontStyle: "italic" }}>No active CRDTs</span>}
      </div>

      {/* Entries */}
      {state?.error && <div style={{ color: "#f87171", fontSize: 11 }}>Error: {state.error}</div>}
      {state?.loading && <div style={{ color: "#71717a", fontSize: 11 }}>Loading…</div>}
      {state && !state.loading && state.entries.length === 0 && !state.error && (
        <div style={{ color: "#52525b", fontStyle: "italic" }}>No history entries</div>
      )}
      {state && state.entries.length > 0 && (
        <>
          {state.entries.map(entry => (
            <div key={entry.seq} style={{ padding: "4px 0", borderBottom: "1px solid #18181b" }}>
              <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                <span style={{ color: "#71717a", fontSize: 10 }}>#{entry.seq}</span>
                <span style={{ color: "#52525b", fontSize: 10 }}>{new Date(entry.timestamp_ms).toLocaleTimeString()}</span>
              </div>
              <div style={{ color: "#86efac", fontSize: 10, wordBreak: "break-all", whiteSpace: "pre-wrap", marginTop: 2 }}>
                {JSON.stringify(entry.op, null, 2)}
              </div>
            </div>
          ))}
          {state.nextSeq != null && (
            <button
              type="button"
              onClick={() => { if (selectedCrdtId) loadMore(selectedCrdtId); }}
              style={{ marginTop: 6, background: "#27272a", color: "#a1a1aa", border: "none", borderRadius: 4, padding: "3px 10px", fontSize: 10, cursor: "pointer" }}
            >
              Load more
            </button>
          )}
        </>
      )}
    </div>
  );
}

function TabBar({ active, onChange }: { active: Tab; onChange: (t: Tab) => void }) {
  const tab = (t: Tab, label: string) => (
    <button
      key={t}
      type="button"
      onClick={() => onChange(t)}
      style={{
        flex: 1,
        background: active === t ? "#27272a" : "transparent",
        color: active === t ? "#e4e4e7" : "#71717a",
        border: "none",
        borderBottom: active === t ? "2px solid #7c3aed" : "2px solid transparent",
        padding: "4px 0",
        fontSize: 10,
        fontWeight: 700,
        fontFamily: "monospace",
        cursor: "pointer",
        letterSpacing: "0.05em",
        textTransform: "uppercase",
      }}
    >
      {label}
    </button>
  );
  return (
    <div style={{ display: "flex", borderBottom: "1px solid #27272a", backgroundColor: "#18181b" }}>
      {tab("crdts", "CRDTs")}
      {tab("events", "Events")}
      {tab("history", "History")}
    </div>
  );
}

function MeridianDevtoolsImpl({ client, defaultOpen = false }: MeridianDevtoolsProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  const [tab, setTab] = useState<Tab>("crdts");
  const [selectedCrdtId, setSelectedCrdtId] = useState<string | null>(null);
  const [snapshot, events, refresh] = useDevtoolsState(client);

  return (
    <>
      {isOpen && (
        <div style={styles.panel}>
          <div style={styles.header}>
            <span>M</span>
            <span>Meridian Devtools</span>
            <button
              type="button"
              onClick={refresh}
              title="Refresh snapshot"
              style={{ marginLeft: "auto", background: "none", border: "none", color: "#a1a1aa", cursor: "pointer", fontSize: 14, padding: "0 4px" }}
            >↻</button>
          </div>
          <ConnectionSection snapshot={snapshot} client={client} />
          <TabBar active={tab} onChange={setTab} />
          <div style={styles.body}>
            {tab === "crdts" && (
              <CRDTsSection
                crdts={snapshot.crdts}
                selectedId={selectedCrdtId}
                onSelect={setSelectedCrdtId}
              />
            )}
            {tab === "events" && <EventsSection events={events} />}
            {tab === "history" && (
              <HistorySection
                client={client}
                selectedCrdtId={selectedCrdtId}
                onSelectCrdt={setSelectedCrdtId}
                crdts={snapshot.crdts}
              />
            )}
          </div>
        </div>
      )}
      <button
        type="button"
        style={styles.toggleButton}
        onClick={() => { setIsOpen((v) => !v); }}
        title="Toggle Meridian Devtools"
      >
        M
      </button>
    </>
  );
}

// Bundlers (Vite, webpack, Next.js) replace process.env.NODE_ENV at build time.
// In production builds the entire MeridianDevtoolsImpl is tree-shaken away.
declare const process: { env: Record<string, string | undefined> } | undefined;
const isProduction =
  typeof process !== "undefined" && process.env.NODE_ENV === "production";

export const MeridianDevtools: ComponentType<MeridianDevtoolsProps> =
  isProduction ? () => null : MeridianDevtoolsImpl;
