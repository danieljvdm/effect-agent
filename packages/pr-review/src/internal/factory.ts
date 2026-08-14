import { Effect, Layer } from "effect";
import {
  Agent,
  AgentPolicy,
  getToolExecutionClass,
  IdGenerator,
  SubagentReservationsMemoryLive,
  type AgentPolicyInput,
  type UsageBudgetLimits,
} from "effect-agent";
import { Toolkit, type LanguageModel, type Model, type Tool } from "effect/unstable/ai";

import {
  fanOutHandlersLayerFor,
  FanOutCoordinatorToolkitLayer,
  FileReviewToolkitLayer,
  makeFanOutReviewSuite,
} from "./fan-out.ts";
import { ignoringPullRequestSourceLayer } from "./ignore.ts";
import {
  clampMaxFindings,
  CodeReview,
  defaultReviewPolicy,
  ListChangedFiles,
  makeReviewInstructions,
  ReadFile,
  ReadFileDiff,
  ReviewMission,
  ReviewToolkitLayer,
  type ReviewGuidance,
} from "./review-agent.ts";
import { executeReview, fanOutReviewBudgetLimits, reviewBudgetLimits } from "./run.ts";

// ---------------------------------------------------------------------------
// The configuration factory: one call turns a Model and optional adaptation
// knobs into a bound, runnable reviewer. Every knob widens what goes INTO the
// review — guidance, extra read-only tools, execution bounds, ignore globs —
// and none weakens what leaves it: anchor validation, the findings bound, and
// publication-after-settlement are applied by the run path unconditionally.
// ---------------------------------------------------------------------------

/** Options shared by both reviewer shapes. */
export interface PrReviewSharedOptions {
  /**
   * Host-side and instruction-level findings bound, clamped to the CodeReview
   * schema cap of 20.
   */
  readonly maxFindings?: number | undefined;
  /**
   * Glob patterns (`**` crosses directories, `*`/`?` stay in one segment)
   * removed from the reviewer's observation surface entirely.
   */
  readonly ignore?: ReadonlyArray<string> | undefined;
  /** Map the model's verdict onto APPROVE/REQUEST_CHANGES instead of COMMENT. */
  readonly applyVerdict?: boolean | undefined;
  /** Run-level usage bounds; defaults to the shape's packaged limits. */
  readonly budget?: UsageBudgetLimits | undefined;
}

/** Options accepted by `PrReview.make` (the flat reviewer). */
export interface PrReviewOptions<
  Provider,
  ModelProvides,
  ModelRequires,
  Extra extends ReadonlyArray<Tool.Any>,
> extends PrReviewSharedOptions {
  /** The Effect AI Model to bind; its Layer requirements stay visible in `R`. */
  readonly model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>;
  /** Domain guidance injected between the mission framing and the procedure. */
  readonly guidance?: ReviewGuidance | undefined;
  /** Full override of the flat reviewer's execution bounds. */
  readonly policy?: AgentPolicyInput | undefined;
  /**
   * Additional tools merged into the reviewer's toolkit. Every extra tool
   * must be annotated `ToolExecutionClass: "readonly"` — construction fails
   * otherwise — and its handler Layer is the caller's to provide, so the new
   * dependency stays visible in the run's `R`.
   */
  readonly extraTools?: Extra | undefined;
}

/** How one run should publish. */
export interface RunReviewOptions {
  /** Post the review to GitHub; `false` (default) stops after planning. */
  readonly post?: boolean | undefined;
}

const EMPTY_TOOLS: ReadonlyArray<Tool.Any> = [];

const requireReadonly = (tools: ReadonlyArray<Tool.Any>): void => {
  for (const tool of tools) {
    const executionClass = getToolExecutionClass(tool);
    if (executionClass !== "readonly") {
      throw new Error(
        `PrReview.make: extra tool '${tool.name}' declares execution class '${executionClass}'. ` +
          `The packaged reviewer's tool surface is read-only; annotate the tool with ` +
          `ToolExecutionClass "readonly" or run it outside the reviewer.`,
      );
    }
  }
};

const provideIgnore = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  ignore: ReadonlyArray<string> | undefined,
) =>
  ignore !== undefined && ignore.length > 0
    ? effect.pipe(Effect.provide(ignoringPullRequestSourceLayer(ignore)))
    : effect;

/**
 * Build the flat reviewer: one bounded read-only agent over the whole
 * changeset. Returns the model-agnostic definition, the explicit binding, and
 * a `run` whose error and requirement channels stay fully inferred — the
 * pull-request source, the publisher, extra tool handlers, and the Model
 * Layer's requirements all remain visible to the caller.
 */
const make = <
  Provider,
  ModelProvides,
  ModelRequires,
  const Extra extends ReadonlyArray<Tool.Any> = readonly [],
>(
  options: PrReviewOptions<Provider, ModelProvides, ModelRequires, Extra>,
) => {
  // Safe when `extraTools` is omitted: the generic default fixes Extra to the
  // empty tuple, which is exactly what the fallback value is.
  const extraTools = options.extraTools ?? (EMPTY_TOOLS as Extra);
  requireReadonly(extraTools);

  const definition = Agent.define("pr-reviewer", {
    input: ReviewMission,
    output: CodeReview,
    instructions: makeReviewInstructions({
      guidance: options.guidance,
      maxFindings: options.maxFindings,
    }),
    toolkit: Toolkit.make(ListChangedFiles, ReadFileDiff, ReadFile, ...extraTools),
    policy: options.policy === undefined ? defaultReviewPolicy : AgentPolicy.make(options.policy),
    description:
      "Review one pull request read-only: list the changeset, read annotated diffs and head-file context, and return a structured, line-anchored code review.",
    metadata: { deploymentClass: "E", surface: "read-only" },
  });
  // `Agent.withModel` types the model through a conditional that stays
  // deferred inside this generic body, so the binding is built structurally —
  // the identical frozen `{ definition, model }` pair the runtime accepts.
  const binding = Object.freeze({ definition, model: options.model });

  const run = (runOptions: RunReviewOptions = {}) =>
    provideIgnore(
      executeReview(binding, {
        post: runOptions.post ?? false,
        applyVerdict: options.applyVerdict ?? false,
        limits: options.budget ?? reviewBudgetLimits,
        maxFindings: clampMaxFindings(options.maxFindings),
      }).pipe(Effect.provide(Layer.mergeAll(ReviewToolkitLayer, IdGenerator.layer)), Effect.scoped),
      options.ignore,
    );

  return { definition, binding, run } as const;
};

/** Options accepted by `PrReview.makeFanOut` (the delegating reviewer). */
export interface PrReviewFanOutOptions<
  Provider,
  ModelProvides,
  ModelRequires,
> extends PrReviewSharedOptions {
  /** The Effect AI Model bound to both the coordinator and its children. */
  readonly model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>;
  /**
   * Static guidance injected into every child reviewer's instructions. The
   * coordinator's mission never crosses the delegation boundary, so
   * mission-dependent guidance cannot exist for children.
   */
  readonly guidance?: string | ReadonlyArray<string> | undefined;
}

/**
 * Build the fan-out reviewer: a coordinator that delegates bounded per-unit
 * file reviews to attached ephemeral children and merges their findings under
 * the same output contract and the same fail-closed publication path as the
 * flat reviewer. Child and coordinator execution bounds are packaged and not
 * configurable here — the delegation reservation mirrors the child policy,
 * and letting the two drift apart is a published-API hazard.
 */
const makeFanOut = <Provider, ModelProvides, ModelRequires>(
  options: PrReviewFanOutOptions<Provider, ModelProvides, ModelRequires>,
) => {
  const suite = makeFanOutReviewSuite({ guidance: options.guidance });
  // Structural bindings for the same reason as in `make` above.
  const binding = Object.freeze({ definition: suite.parent, model: options.model });
  const childBinding = Object.freeze({ definition: suite.child, model: options.model });
  const delegationLayer = fanOutHandlersLayerFor(suite.delegation)(childBinding).pipe(
    Layer.provide(
      Layer.mergeAll(FileReviewToolkitLayer, SubagentReservationsMemoryLive, IdGenerator.layer),
    ),
  );

  const run = (runOptions: RunReviewOptions = {}) =>
    provideIgnore(
      executeReview(binding, {
        post: runOptions.post ?? false,
        applyVerdict: options.applyVerdict ?? false,
        limits: options.budget ?? fanOutReviewBudgetLimits,
        maxFindings: clampMaxFindings(options.maxFindings),
      }).pipe(
        Effect.provide(
          Layer.mergeAll(FanOutCoordinatorToolkitLayer, delegationLayer, IdGenerator.layer),
        ),
        Effect.scoped,
      ),
      options.ignore,
    );

  return { definition: suite.parent, binding, childBinding, run } as const;
};

/**
 * The packaged pull-request reviewer factory.
 *
 * - `make` — one flat reviewer over the whole changeset.
 * - `makeFanOut` — a coordinator delegating bounded per-unit child reviews.
 */
export const PrReview = { make, makeFanOut } as const;
