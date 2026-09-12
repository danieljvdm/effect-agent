import { Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { PlannerError } from "../domain.ts";
import { ScoutFindings } from "./contracts.ts";

// Accept a structurally valid model draft before checking publication limits. Provider-side
// parameter decoding otherwise aborts the run before a tool can return corrective feedback.
// The model response budget bounds draft generation; only ScoutFindings can leave the scout.
export const ResearchDraft = Schema.Struct({
  summary: Schema.String,
  sources: Schema.Array(
    Schema.Struct({
      title: Schema.String,
      url: Schema.String,
      notes: Schema.String,
      photos: Schema.Array(Schema.Struct({ url: Schema.String, caption: Schema.String })),
    }),
  ),
});

export const CheckedFinishResearch = Tool.make("finish_research", {
  description:
    "Finish with concise sourced findings: summary at most 4000 characters, at most 6 sources, source titles at most 240 characters, notes at most 1200 characters, and at most 4 photos per source. Keep the complete JSON under 8 KiB; use fewer sources/photos if needed. Use real public HTTPS URLs and only photos from inspected pages. Preserve uncertainty about prices, availability and dates. If rejected, shorten or correct this same draft and call finish_research again; do not repeat the research.",
  parameters: ResearchDraft,
  success: ScoutFindings,
  failure: PlannerError,
  failureMode: "return",
}).annotate(Tool.Readonly, true);

export const CheckedFinishResearchLive = Toolkit.make(CheckedFinishResearch).toLayer({
  finish_research: (draft) =>
    Schema.decodeEffect(ScoutFindings)(draft).pipe(
      Effect.mapError(
        () =>
          new PlannerError({
            code: "invalid",
            message:
              `Findings were not accepted. This draft has ${draft.summary.length} summary characters, ${draft.sources.length} sources, and ${new TextEncoder().encode(JSON.stringify(draft)).byteLength} JSON bytes. ` +
              "Correct the draft and call finish_research again: summary <= 4000 characters; <= 6 sources; source titles and photo captions must be 1-240 characters; notes <= 1200 characters; <= 4 photos per source; URLs must be public HTTPS and <= 2048 characters; complete JSON <= 8192 bytes. Preserve key findings and uncertainty; omit redundant detail and photos before removing useful evidence. Do not redo searches.",
          }),
      ),
    ),
});
