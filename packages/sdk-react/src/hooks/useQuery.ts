import { useState, useEffect } from "react";
import type { QuerySpec, QueryResult } from "meridian-sdk";
import { useMeridianClient } from "../context.js";

/**
 * Execute a one-shot cross-CRDT query against the namespace.
 *
 * The query re-runs whenever `spec.from`, `spec.type`, or `spec.aggregate` changes.
 * Stabilize `spec` with `useMemo` to avoid unnecessary re-fetches.
 *
 * @example
 * ```tsx
 * const spec = useMemo(() => ({ from: "gc:views-*", aggregate: "sum" as const }), []);
 * const { data, loading } = useQuery(spec);
 * if (!loading) console.log(data?.value);
 * ```
 */
export const useQuery = (
  spec: QuerySpec,
): { data: QueryResult | null; loading: boolean; error: Error | null } => {
  const client = useMeridianClient();
  const [data, setData] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    client
      .query(spec)
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setError(e instanceof Error ? e : new Error(String(e)));
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
    // spec is not stable across renders — depend on its primitive fields explicitly.
    // Callers should stabilize the spec object with useMemo.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [client, spec.from, spec.type, spec.aggregate]);

  return { data, loading, error };
};
