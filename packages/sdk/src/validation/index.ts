/**
 * Runtime validation for CRDT string values.
 *
 * Values stored in TreeCRDT and RGA are always strings on the wire. This
 * layer lets you attach a validator to a handle so that ops are checked
 * before being sent. Invalid values throw a `CrdtValidationError`.
 *
 * The validator interface is intentionally minimal — plug in Zod, Valibot,
 * ArkType, or a plain function without any SDK dependency on those libraries.
 *
 * @example Using Zod:
 * ```ts
 * import { z } from "zod";
 * import { zodValidator } from "meridian-sdk";
 *
 * const schema = z.object({ title: z.string(), done: z.boolean() });
 * const tree = client.tree("tasks", { validator: zodValidator(schema) });
 *
 * // Validated — value is parsed JSON then checked against schema
 * tree.addNode(null, "a0", JSON.stringify({ title: "Task 1", done: false }));
 *
 * // Throws CrdtValidationError
 * tree.addNode(null, "a0", JSON.stringify({ title: 42 }));
 * ```
 *
 * @example Using a plain function:
 * ```ts
 * import { fnValidator } from "meridian-sdk";
 *
 * const tree = client.tree("tasks", {
 *   validator: fnValidator((v) => typeof v === "string" && v.length > 0),
 * });
 * ```
 */

/** Thrown when a CRDT value fails validation before being sent. */
export class CrdtValidationError extends Error {
  override readonly name = "CrdtValidationError";
  constructor(
    /** The raw string value that failed. */
    public readonly value: string,
    /** Human-readable reason from the validator. */
    public readonly reason: string,
  ) {
    super(`CrdtValidationError: ${reason} (value: ${JSON.stringify(value)})`);
  }
}

/**
 * A validator that can be attached to a CRDT handle.
 *
 * `validate` receives the raw string value as stored on the wire. It should
 * throw or return `{ ok: false, error: string }` for invalid values, and
 * return `{ ok: true }` or `undefined` for valid ones.
 */
export interface CrdtValidator {
  /**
   * Validate `value`. Throw, or return `{ ok: false, error }` to reject.
   * Return `{ ok: true }` or `undefined` to accept.
   */
  validate(value: string): { ok: false; error: string } | { ok: true } | undefined | void;
}

/**
 * Run a validator against a value. Throws `CrdtValidationError` on failure.
 * No-ops if `validator` is undefined.
 */
export function runValidator(validator: CrdtValidator | undefined, value: string): void {
  if (validator === undefined) return;
  let result: ReturnType<CrdtValidator["validate"]>;
  try {
    result = validator.validate(value);
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    throw new CrdtValidationError(value, reason);
  }
  if (result !== undefined && result !== null && !result.ok) {
    throw new CrdtValidationError(value, result.error);
  }
}

/**
 * Creates a `CrdtValidator` from any Zod schema (or any object with a
 * `safeParse` method returning `{ success, error }`).
 *
 * The string value is JSON-parsed before being passed to the schema.
 * If JSON parsing fails, the raw string is passed as-is.
 *
 * @example
 * ```ts
 * import { z } from "zod";
 * const validator = zodValidator(z.object({ title: z.string() }));
 * ```
 */
export function zodValidator(schema: {
  safeParse(value: unknown): { success: true } | { success: false; error: { message: string } };
}): CrdtValidator {
  return {
    validate(raw: string) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(raw);
      } catch {
        parsed = raw;
      }
      const result = schema.safeParse(parsed);
      if (!result.success) {
        return { ok: false, error: result.error.message };
      }
    },
  };
}

/**
 * Creates a `CrdtValidator` from a plain predicate function.
 *
 * The function receives the raw string value. Return `true` to accept,
 * `false` or a string (used as the error message) to reject.
 *
 * @example
 * ```ts
 * const validator = fnValidator((v) => v.length > 0 || "Value must not be empty");
 * ```
 */
export function fnValidator(
  fn: (value: string) => boolean | string,
): CrdtValidator {
  return {
    validate(raw: string) {
      const result = fn(raw);
      if (result === true) return { ok: true };
      if (result === false) return { ok: false, error: "Validation failed" };
      return { ok: false, error: result };
    },
  };
}
