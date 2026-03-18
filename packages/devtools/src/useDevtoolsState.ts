import { useState, useEffect, useCallback } from "react";
import type { MeridianClient, ClientSnapshot } from "meridian-sdk";

export function useDevtoolsState(client: MeridianClient): [ClientSnapshot, () => void] {
  const [, setTick] = useState(0);
  const refresh = useCallback(() => { setTick((n) => n + 1); }, []);

  useEffect(() => {
    const unsub = client.onAnyChange(refresh);
    return () => { unsub(); };
  }, [client, refresh]);

  return [client.snapshot(), refresh];
}
