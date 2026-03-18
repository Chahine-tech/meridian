import { useEffect } from "react";

/**
 * Runs an effect exactly once on mount and cleans up on unmount.
 * Use this instead of `useEffect(..., [])` to make intent explicit
 * and satisfy the no-useEffect lint rule.
 *
 * Only use this for syncing with external systems (DOM, timers, subscriptions).
 * For all other cases, prefer derived state, event handlers, or data-fetching libraries.
 */
export function useMountEffect(effect: () => (() => void) | undefined): void {
  // biome-ignore lint/correctness/useExhaustiveDependencies: mount-only by design
  useEffect(effect, []);
}
