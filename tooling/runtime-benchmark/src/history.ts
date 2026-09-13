import type { Layer } from "effect";
import * as ThreadHistory from "effect-agent/thread-history";

// The comparison fixture also runs against releases before in-memory history became the default.
const selectHistory = (module: {
  readonly layer?: Layer.Layer<ThreadHistory.ThreadHistory>;
  readonly ThreadHistory: {
    readonly layer?: Layer.Layer<ThreadHistory.ThreadHistory>;
    readonly layerTransient?: Layer.Layer<ThreadHistory.ThreadHistory>;
  };
}) => {
  const layer = module.layer ?? module.ThreadHistory.layer ?? module.ThreadHistory.layerTransient;

  if (layer === undefined) throw new Error("Compared release has no supported history Layer");

  return {
    layer,
    retainsHistory: module.layer !== undefined || module.ThreadHistory.layer !== undefined,
  };
};

export const { layer: BenchmarkHistoryLive, retainsHistory: BenchmarkRetainsHistory } =
  selectHistory(ThreadHistory);
