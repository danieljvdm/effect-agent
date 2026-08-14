import { PrReview } from "@effect-agent/pr-review";
import { Effect, Schema } from "effect";
import { ToolExecutionClass } from "effect-agent";
import { Tool, Toolkit, type LanguageModel, type Model } from "effect/unstable/ai";

// ---------------------------------------------------------------------------
// A customized reviewer built on @effect-agent/pr-review — the consumer path
// this example exists to exercise. Three adaptations, none of which touch the
// package's fail-closed publication path:
//
//   1. domain guidance injected into the reviewer's instructions;
//   2. one extra READ-ONLY tool serving this repository's review conventions;
//   3. lockfiles removed from the review surface.
// ---------------------------------------------------------------------------

export const REVIEW_CONVENTIONS = [
  "Effect-repository review conventions:",
  "- Public asynchronous operations return Effect or Stream, never naked Promises.",
  "- Expected failures stay typed in E; do not widen errors to unknown or Error.",
  "- Schema is the canonical source for persisted and transported values.",
  "- No code may claim exactly-once external side-effect execution.",
].join("\n");

export class ConventionsQuery extends Schema.Class<ConventionsQuery>(
  "@effect-agent/example-pr-review/ConventionsQuery",
)({
  /** Explicit constant keeps the zero-choice operation compatible with strict provider schemas. */
  scope: Schema.Literal("all"),
}) {}

export class ConventionsView extends Schema.Class<ConventionsView>(
  "@effect-agent/example-pr-review/ConventionsView",
)({
  conventions: Schema.NonEmptyString,
}) {}

/** The extra tool: read-only by annotation, so the factory accepts it. */
export const ReadReviewConventions = Tool.make("read_review_conventions", {
  description: "Read this repository's code-review conventions.",
  parameters: ConventionsQuery,
  success: ConventionsView,
}).annotate(ToolExecutionClass, "readonly");

export const ReadReviewConventionsLayer = Toolkit.make(ReadReviewConventions).toLayer({
  read_review_conventions: () =>
    Effect.succeed(ConventionsView.make({ conventions: REVIEW_CONVENTIONS })),
});

/**
 * Build the customized reviewer for any caller-selected Model (D-027). The
 * run's requirements keep the extra tool's handler Layer visible, so a caller
 * who forgets `ReadReviewConventionsLayer` gets a compile error, not a
 * runtime surprise.
 */
export const makeExampleReviewer = <Provider, ModelProvides, ModelRequires>(
  model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>,
) =>
  PrReview.make({
    model,
    guidance: (mission) => [
      `Consult read_review_conventions before judging style or API-shape questions in the ${mission.changedFileCount} changed file(s).`,
      "This repository is an Effect codebase: flag any public API returning a naked Promise as important.",
    ],
    ignore: ["**/*.lock", "**/bun.lock"],
    maxFindings: 10,
    extraTools: [ReadReviewConventions],
  });
