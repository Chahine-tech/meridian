import { useState, useCallback } from "react";
import type { MeridianClient, HistoryEntry, HistoryResponse } from "meridian-sdk";

interface HistoryState {
  entries: HistoryEntry[];
  nextSeq: number | null;
  loading: boolean;
  error: string | null;
}

export type { HistoryEntry };

export function useHistory(client: MeridianClient) {
  const [states, setStates] = useState<Record<string, HistoryState>>({});

  const load = useCallback(async (crdtId: string, fromSeq = 0) => {
    setStates(prev => ({
      ...prev,
      [crdtId]: {
        entries: fromSeq === 0 ? [] : (prev[crdtId]?.entries ?? []),
        nextSeq: null,
        loading: true,
        error: null,
      },
    }));

    try {
      const data: HistoryResponse = await client.http.getHistory(client.namespace, crdtId, fromSeq);
      setStates(prev => ({
        ...prev,
        [crdtId]: {
          entries: fromSeq === 0 ? data.entries : [...(prev[crdtId]?.entries ?? []), ...data.entries],
          nextSeq: data.next_seq,
          loading: false,
          error: null,
        },
      }));
    } catch (e) {
      setStates(prev => ({
        ...prev,
        [crdtId]: {
          entries: prev[crdtId]?.entries ?? [],
          nextSeq: prev[crdtId]?.nextSeq ?? null,
          loading: false,
          error: String(e),
        },
      }));
    }
  }, [client]);

  const loadMore = useCallback((crdtId: string) => {
    const nextSeq = states[crdtId]?.nextSeq;
    if (nextSeq != null) void load(crdtId, nextSeq);
  }, [states, load]);

  return { states, load, loadMore };
}
