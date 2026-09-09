import type { DemoOperationalEvent } from "./operational-contracts";

type EventBatch<A> =
  | { readonly _tag: "Event"; readonly event: A }
  | { readonly _tag: "Deltas"; readonly events: ReadonlyArray<A> };

/**
 * Coalesces only adjacent deltas already delivered together. Semantic events
 * remain individual boundaries; no event is dropped or delayed for another pull.
 */
export function* eventBatches<A extends DemoOperationalEvent>(
  events: ReadonlyArray<A>,
): Generator<EventBatch<A>> {
  let deltas: Array<A> = [];

  for (const event of events) {
    if (event._tag === "TextDelta" || event._tag === "ReasoningDelta") {
      deltas.push(event);
      if (deltas.length === 64) {
        yield { _tag: "Deltas", events: deltas };
        deltas = [];
      }
    } else {
      if (deltas.length > 0) {
        yield { _tag: "Deltas", events: deltas };
        deltas = [];
      }
      yield { _tag: "Event", event };
    }
  }

  if (deltas.length > 0) yield { _tag: "Deltas", events: deltas };
}
