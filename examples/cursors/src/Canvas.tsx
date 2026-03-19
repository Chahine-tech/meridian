import { useCallback, useEffect, useRef } from "react";
import { Schema } from "effect";
import { useAwareness, useMeridianClient, usePresence } from "meridian-react";
import { colorForClient } from "./colors.js";

const CursorSchema = Schema.Struct({
  x: Schema.Number,
  y: Schema.Number,
  name: Schema.String,
});

type CursorData = typeof CursorSchema.Type;

function RemoteCursor({ x, y, name, clientId }: CursorData & { clientId: number }) {
  const color = colorForClient(clientId);
  return (
    <div style={{
      position: "absolute",
      left: x,
      top: y,
      pointerEvents: "none",
      transform: "translate(-2px, -2px)",
    }}>
      {/* SVG cursor arrow */}
      <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
        <path d="M3 2L17 10L10 12L7 18L3 2Z" fill={color} stroke="#0f0f0f" strokeWidth="1.5" />
      </svg>
      <span style={{
        position: "absolute",
        left: 18,
        top: 2,
        background: color,
        color: "#0f0f0f",
        fontSize: 11,
        fontWeight: 700,
        padding: "1px 6px",
        borderRadius: 4,
        whiteSpace: "nowrap",
        fontFamily: "monospace",
      }}>
        {name}
      </span>
    </div>
  );
}

function VisitorCount({ count }: { count: number }) {
  return (
    <div style={{
      position: "fixed",
      top: 16,
      left: 16,
      background: "#18181b",
      border: "1px solid #27272a",
      borderRadius: 6,
      padding: "6px 12px",
      fontSize: 12,
      fontFamily: "monospace",
      color: "#a1a1aa",
    }}>
      {count} visitor{count !== 1 ? "s" : ""} connected
    </div>
  );
}

export function Canvas() {
  const client = useMeridianClient();
  const containerRef = useRef<HTMLDivElement>(null);

  const myName = `Client #${client.clientId}`;
  const myColor = colorForClient(client.clientId);

  const { peers, update, clear } = useAwareness<CursorData>("cursors", CursorSchema);
  const { online } = usePresence("visitors", { data: {}, ttlMs: 15_000 });

  // Track last known position so we can re-broadcast periodically.
  // Awareness is stateless — new peers won't see our cursor until we move again.
  const lastPosRef = useRef<CursorData | null>(null);

  const handleMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const pos = { x: e.clientX - rect.left, y: e.clientY - rect.top, name: myName };
    lastPosRef.current = pos;
    update(pos);
  }, [update, myName]);

  // Clear our cursor when the mouse leaves the canvas
  const handleMouseLeave = useCallback(() => {
    lastPosRef.current = null;
    clear();
  }, [clear]);

  // Re-broadcast our position every 3 s so late-joining peers see us immediately.
  useEffect(() => {
    const id = setInterval(() => {
      if (lastPosRef.current) update(lastPosRef.current);
    }, 3_000);
    return () => clearInterval(id);
  }, [update]);

  return (
    <div
      ref={containerRef}
      onMouseMove={handleMouseMove}
      onMouseLeave={handleMouseLeave}
      style={{
        width: "100%",
        height: "100%",
        position: "relative",
        cursor: "none",
        background: "#0f0f0f",
        backgroundImage: "radial-gradient(circle, #27272a 1px, transparent 1px)",
        backgroundSize: "32px 32px",
      }}
    >
      {/* My cursor label */}
      <div style={{
        position: "fixed",
        top: 16,
        right: 60,
        background: myColor,
        color: "#0f0f0f",
        fontSize: 11,
        fontWeight: 700,
        padding: "2px 8px",
        borderRadius: 4,
        fontFamily: "monospace",
      }}>
        {myName} (you)
      </div>

      <VisitorCount count={online.length} />

      {/* Hint */}
      <div style={{
        position: "absolute",
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
        color: "#27272a",
        fontSize: 13,
        fontFamily: "monospace",
        textAlign: "center",
        pointerEvents: "none",
        userSelect: "none",
      }}>
        Move your cursor · Open in another tab to see live sync
      </div>

      {/* Remote cursors */}
      {peers.map((entry) => (
        <RemoteCursor
          key={entry.clientId}
          clientId={entry.clientId}
          x={entry.data.x}
          y={entry.data.y}
          name={entry.data.name}
        />
      ))}
    </div>
  );
}
