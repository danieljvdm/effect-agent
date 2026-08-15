import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Reasoning effort, stored as a POSITION on [0, 1] rather than a rung name.
// A rung name is only meaningful inside the provider that published it: the
// same word can be one provider's floor and another's midpoint, and a stored
// name silently changes meaning when the model under the setting changes. A
// position has no such problem: 0 is whatever the provider calls its cheapest
// offered rung and 1 its most expensive, and resolution is a lookup into that
// provider's own ladder — the result is always a rung the provider offers.
// ---------------------------------------------------------------------------

/** A point on the effort axis: 0 = cheapest offered rung, 1 = most expensive. */
export type EffortPosition = number;

/**
 * Names accepted on user-facing surfaces (the action input, the CLI flag),
 * mapped to fixed points on the axis. These same names anchor every offered
 * rung during resolution, so a named input always lands on its same-named
 * rung when the provider offers it — `high` never resolves to `medium` just
 * because a ladder is short.
 */
export const EFFORT_ALIASES = {
  low: 0,
  medium: 0.25,
  high: 0.5,
  xhigh: 0.75,
  max: 1,
} as const satisfies Readonly<Record<string, EffortPosition>>;

/** A rung name every provider ladder must draw from. */
export type EffortAliasName = keyof typeof EFFORT_ALIASES;

const aliasPosition: Readonly<Record<string, EffortPosition | undefined>> = EFFORT_ALIASES;

/** An effort input that is neither a known name nor a number on [0, 1]. */
export class InvalidEffortInput extends Schema.TaggedError<InvalidEffortInput>()(
  "InvalidEffortInput",
  {
    input: Schema.String,
  },
) {
  override get message() {
    return (
      `Invalid effort '${this.input}': expected one of ` +
      `${Object.keys(EFFORT_ALIASES).join(", ")} or a number between 0 and 1.`
    );
  }
}

export const isEffortPosition = (value: number): boolean =>
  Number.isFinite(value) && value >= 0 && value <= 1;

/**
 * Parse a user-supplied effort into a position: a name (`high`) or a bare
 * number (`0.75`). Returns undefined for anything else so the caller can fail
 * typed — a typo must stay visible, never silently become a level.
 */
export const parseEffortPosition = (raw: string): EffortPosition | undefined => {
  const normalized = raw.trim().toLowerCase();
  const named = aliasPosition[normalized];
  if (named !== undefined) return named;
  if (normalized === "") return undefined;
  const numeric = Number(normalized);
  return isEffortPosition(numeric) ? numeric : undefined;
};

/**
 * Land a position on one provider's offered ladder: the highest offered rung
 * whose canonical alias position is at or below the requested position.
 * Anchoring on the alias positions (instead of scaling by ladder index) keeps
 * two properties at once: a named input lands on its same-named rung whenever
 * the provider offers it, and anything between rungs rounds DOWN so
 * resolution never costs more than was asked for.
 */
export const resolveEffortRung = <const Rung extends EffortAliasName>(
  position: EffortPosition,
  rungs: readonly [Rung, ...ReadonlyArray<Rung>],
): Rung => {
  const clamped = Math.min(1, Math.max(0, position));
  let selected = rungs[0];
  for (const rung of rungs) {
    if (EFFORT_ALIASES[rung] <= clamped) selected = rung;
  }
  return selected;
};
