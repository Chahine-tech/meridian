import type { CSSProperties } from "react";

export const Z_INDEX = 99999;

const typeColor = (type: string): string => {
  switch (type) {
    case "gcounter": return "#1d4ed8";
    case "pncounter": return "#0f766e";
    case "orset": return "#b45309";
    case "lwwregister": return "#7c3aed";
    case "presence": return "#be185d";
    case "crdtmap": return "#4d7c0f";
    case "rga": return "#c2410c";
    case "tree": return "#0369a1";
    default: return "#52525b";
  }
};

export const wsStateColor = (state: string): string => {
  switch (state) {
    case "CONNECTED": return "#16a34a";
    case "CONNECTING": return "#ca8a04";
    case "DISCONNECTED": return "#dc2626";
    case "CLOSING": return "#9a3412";
    default: return "#52525b";
  }
};

export const styles = {
  toggleButton: {
    position: "fixed",
    bottom: 16,
    right: 16,
    zIndex: Z_INDEX,
    width: 36,
    height: 36,
    borderRadius: "50%",
    backgroundColor: "#7c3aed",
    color: "#fff",
    border: "none",
    cursor: "pointer",
    fontSize: 14,
    fontWeight: 700,
    fontFamily: "monospace",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    boxShadow: "0 2px 8px rgba(0,0,0,0.4)",
    lineHeight: 1,
  } satisfies CSSProperties,

  panel: {
    position: "fixed",
    bottom: 60,
    right: 16,
    zIndex: Z_INDEX,
    width: 360,
    maxHeight: 480,
    backgroundColor: "#0f0f0f",
    color: "#e2e8f0",
    borderRadius: 8,
    boxShadow: "0 4px 24px rgba(0,0,0,0.6)",
    fontFamily: "monospace",
    fontSize: 12,
    overflow: "hidden",
    display: "flex",
    flexDirection: "column",
    border: "1px solid #27272a",
  } satisfies CSSProperties,

  header: {
    padding: "8px 12px",
    backgroundColor: "#18181b",
    borderBottom: "1px solid #27272a",
    display: "flex",
    alignItems: "center",
    gap: 8,
    fontWeight: 700,
    fontSize: 11,
    letterSpacing: "0.05em",
    textTransform: "uppercase" as const,
    color: "#a1a1aa",
  } satisfies CSSProperties,

  body: {
    overflowY: "auto" as const,
    flex: 1,
  } satisfies CSSProperties,

  section: {
    padding: "8px 12px",
    borderBottom: "1px solid #18181b",
  } satisfies CSSProperties,

  sectionTitle: {
    fontSize: 10,
    fontWeight: 700,
    color: "#71717a",
    textTransform: "uppercase" as const,
    letterSpacing: "0.08em",
    marginBottom: 6,
  } satisfies CSSProperties,

  statusRow: {
    display: "flex",
    alignItems: "center",
    gap: 8,
  } satisfies CSSProperties,

  crdtRow: {
    display: "flex",
    alignItems: "flex-start",
    gap: 8,
    padding: "4px 0",
    borderBottom: "1px solid #18181b",
  } satisfies CSSProperties,

  crdtId: {
    color: "#a78bfa",
    fontWeight: 600,
    minWidth: 0,
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap" as const,
  } satisfies CSSProperties,

  crdtValue: {
    color: "#86efac",
    wordBreak: "break-all" as const,
    whiteSpace: "pre-wrap" as const,
    fontSize: 11,
    marginTop: 2,
  } satisfies CSSProperties,

  badge: (color: string): CSSProperties => ({
    display: "inline-block",
    padding: "1px 6px",
    borderRadius: 4,
    backgroundColor: color,
    color: "#fff",
    fontSize: 10,
    fontWeight: 700,
    textTransform: "uppercase",
    letterSpacing: "0.04em",
  }),

  typeBadge: (type: string): CSSProperties => ({
    flexShrink: 0,
    display: "inline-block",
    padding: "1px 5px",
    borderRadius: 3,
    backgroundColor: typeColor(type),
    color: "#fff",
    fontSize: 9,
    fontWeight: 700,
    textTransform: "uppercase",
    letterSpacing: "0.04em",
    marginTop: 1,
  }),
} as const;
