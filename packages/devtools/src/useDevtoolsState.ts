import { useState, useEffect } from "react";
import type { MeridianClient, ClientSnapshot } from "meridian-sdk";

export function useDevtoolsState(client: MeridianClient): ClientSnapshot {
  const [, setTick] = useState(0);

  useEffect(() => {
    const bump = () => { setTick((n) => n + 1); };
    const unsub = client.onAnyChange(bump);
    return () => { unsub(); };
  }, [client]);

  return client.snapshot();
}
