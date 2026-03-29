import { useState, useEffect } from "react";
import type { QuerySpec, LiveQueryResult } from "meridian-sdk";
import { useMeridianClient } from "../context.js";

/**
 * Subscribe to a live cross-CRDT query over WebSocket.
 *
 * The server pushes a new result whenever a matching CRDT changes. The hook
 * reflects the most recent result in `data` and `loading: true` until the
 * first result arrives.
 *
 * Stabilize the spec object with `useMemo` to avoid re-subscribing on every render.
 *
 * @example
 * ```tsx
 * const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
 * const { data, loading } = useLiveQuery(spec);
 * if (!loading) console.log("live total:", data?.value);
 * ```
 */
export const useLiveQuery = (
  spec: QuerySpec,
): { data: LiveQueryResult | null; loading: boolean; error: Error | null } => {
  const client = useMeridianClient();
  const [data, setData] = useState<LiveQueryResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    setLoading(true);
    setError(null);

    let handle: ReturnType<typeof client.liveQuery> | null = null;
    try {
      handle = client.liveQuery(spec);
      handle.onResult((result: LiveQueryResult) => {
        setData(result);
        setLoading(false);
      });
    } catch (e: unknown) {
      setError(e instanceof Error ? e : new Error(String(e)));
      setLoading(false);
    }

    return () => {
      handle?.close();
    };
    // Re-subscribe when spec fields change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, spec]);

  return { data, loading, error };
};
