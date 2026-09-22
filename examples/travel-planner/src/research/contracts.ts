import { Schema } from "effect";
import { ThreadId } from "effect-agent/identifiers";

import { AdmittedPlannerSettings, ShortText, Text } from "../domain.ts";
import { TravelPhoto, TravelUrl } from "../travel-content.ts";

export const researchCoordinatorId = "travel-planner-v16";

export const ScoutRequest = Schema.Struct({ title: ShortText, message: Text });

export const ScoutInput = Schema.Struct({
  ...ScoutRequest.fields,
  sourceThreadId: ThreadId,
  settings: AdmittedPlannerSettings,
});

/** Bounded source evidence, never authority or private child history. */
export const ScoutFindings = Schema.Struct({
  summary: Text,
  sources: Schema.Array(
    Schema.Struct({
      title: ShortText,
      url: TravelUrl,
      notes: Schema.String.check(Schema.isMaxLength(1_200)),
      photos: Schema.Array(TravelPhoto).check(Schema.isMaxLength(4)),
    }),
  ).check(Schema.isMaxLength(6)),
}).check(
  Schema.makeFilter(
    (value) => new TextEncoder().encode(JSON.stringify(value)).byteLength <= 8 * 1_024,
    { title: "research findings of at most 8 KiB" },
  ),
);

/** A deliberately authored, sourced milestone, never a partial model response. */
export const ScoutProgress = Schema.Struct({
  summary: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(900)),
  sources: Schema.Array(TravelUrl).check(Schema.isMinLength(1), Schema.isMaxLength(3)),
});
