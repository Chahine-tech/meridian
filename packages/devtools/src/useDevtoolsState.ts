import { useState, useEffect, useCallback } from "react";
import type { MeridianClient, ClientSnapshot, DeltaEvent } from "meridian-sdk";

const MAX_EVENTS = 20;

export function useDevtoolsState(client: MeridianClient): [ClientSnapshot, DeltaEvent[], () => void] {
  const [, setTick] = useState(0);
  const [events, setEvents] = useState<DeltaEvent[]>([]);

  const refresh = useCallback(() => { setTick((n) => n + 1); }, []);

  useEffect(() => {
    const unsubChange = client.onAnyChange(refresh);
    const unsubDelta = client.onDelta((event) => {
      setEvents((prev) => [...prev.slice(-(MAX_EVENTS - 1)), event]);
    });
    return () => { unsubChange(); unsubDelta(); };
  }, [client, refresh]);

  return [client.snapshot(), events, refresh];
}
