/**
 * Conflict events emitted by TreeHandle when the server delta reveals a
 * resolution that differs from what the local client expected.
 *
 * These are purely informational — the CRDT has already converged. Use them
 * for audit logs, "track changes" UIs, or debugging concurrent edit scenarios.
 */

/**
 * A concurrent MoveNode op caused a cycle (e.g. moving a node under its own
 * descendant). The server silently discarded the move; the node stayed in
 * place. This event tells you which move was dropped and where it tried to go.
 */
export interface MoveDiscardedEvent {
  readonly kind: "move_discarded";
  /** The node that was supposed to move. */
  readonly nodeId: string;
  /** Where the move tried to put it (null = root level). */
  readonly attemptedParentId: string | null;
  readonly attemptedPosition: string;
  /** Where the node actually stayed. */
  readonly actualParentId: string | null;
  readonly actualPosition: string;
  readonly at: number; // wall clock ms
}

/**
 * Two concurrent MoveNode ops targeted the same node. The server picked a
 * winner (highest HLC op_id); the loser's destination was overridden.
 */
export interface MoveReorderedEvent {
  readonly kind: "move_reordered";
  readonly nodeId: string;
  /** Where the local client moved it (optimistic). */
  readonly localParentId: string | null;
  readonly localPosition: string;
  /** Where the server placed it after conflict resolution. */
  readonly serverParentId: string | null;
  readonly serverPosition: string;
  readonly at: number;
}

/**
 * Two concurrent UpdateNode ops targeted the same node. LWW resolved it;
 * the value with the older `updated_at` HLC was discarded.
 */
export interface LwwOverwriteEvent {
  readonly kind: "lww_overwrite";
  readonly nodeId: string;
  /** The value that was overwritten (loser). */
  readonly discardedValue: string;
  /** The value that won. */
  readonly winnerValue: string;
  readonly at: number;
}

export type ConflictEvent =
  | MoveDiscardedEvent
  | MoveReorderedEvent
  | LwwOverwriteEvent;
