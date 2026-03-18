import { useState } from "react";
import type { ComponentType } from "react";
import type { MeridianClient, ClientSnapshot, CRDTSnapshotEntry } from "meridian-sdk";
import { useDevtoolsState } from "./useDevtoolsState.js";
import { styles, wsStateColor } from "./styles.js";

interface MeridianDevtoolsProps {
  client: MeridianClient;
  defaultOpen?: boolean;
}

function CRDTRow({ entry }: { entry: CRDTSnapshotEntry }) {
  const valueDisplay = (() => {
    switch (entry.type) {
      case "gcounter":
        return JSON.stringify({ value: entry.value, counts: entry.counts });
      case "pncounter":
        return JSON.stringify({ value: entry.value });
      case "orset":
        return JSON.stringify({ elements: entry.elements });
      case "lwwregister":
        return JSON.stringify({ value: entry.value, meta: entry.meta });
      case "presence":
        return JSON.stringify({ online: entry.online });
      case "crdtmap":
        return JSON.stringify({ value: entry.value });
    }
  })();

  return (
    <div style={styles.crdtRow}>
      <span style={styles.typeBadge(entry.type)}>{entry.type}</span>
      <div style={{ minWidth: 0, flex: 1 }}>
        <div style={styles.crdtId}>{entry.crdtId}</div>
        <div style={styles.crdtValue}>{valueDisplay}</div>
      </div>
    </div>
  );
}

function ConnectionSection({ snapshot }: { snapshot: ClientSnapshot }) {
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
    </div>
  );
}

function CRDTsSection({ crdts }: { crdts: CRDTSnapshotEntry[] }) {
  return (
    <div style={styles.section}>
      <div style={styles.sectionTitle}>CRDTs ({crdts.length})</div>
      {crdts.length === 0 ? (
        <div style={{ color: "#52525b", fontStyle: "italic" }}>No active CRDTs</div>
      ) : (
        crdts.map((entry) => <CRDTRow key={entry.crdtId} entry={entry} />)
      )}
    </div>
  );
}

function MeridianDevtoolsImpl({ client, defaultOpen = false }: MeridianDevtoolsProps) {
  const [isOpen, setIsOpen] = useState(defaultOpen);
  const [snapshot, refresh] = useDevtoolsState(client);

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
          <div style={styles.body}>
            <ConnectionSection snapshot={snapshot} />
            <CRDTsSection crdts={snapshot.crdts} />
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
  typeof process !== "undefined" && process.env["NODE_ENV"] === "production";

export const MeridianDevtools: ComponentType<MeridianDevtoolsProps> =
  isProduction ? () => null : MeridianDevtoolsImpl;
