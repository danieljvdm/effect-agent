import { Effect, Layer, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  getToolExecutionClass,
  IdGenerator,
  type AgentPolicyInput,
  type UsageBudgetLimits,
} from "effect-agent";
import { Toolkit, type LanguageModel, type Model, type Tool } from "effect/unstable/ai";

import type { ChangedFile } from "./diff.ts";
import { makeFileReviewerDefinition } from "./fan-out.ts";
import { computeChangesetFingerprint, computeProfileFingerprint } from "./fingerprint.ts";
import { compileIgnoreGlobs, ignoringPullRequestSourceLayer } from "./ignore.ts";
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
  resolveGuidance as resolveReviewGuidance,
  type ReviewGuidance,
} from "./review-agent.ts";
import { buildProfileMission } from "./review-state.ts";
import {
  buildReviewMission,
  executeFanOutReview,
  executeReview,
  fanOutReviewBudgetLimits,
  reviewBudgetLimits,
} from "./run.ts";
import { PullRequestSource } from "./source.ts";

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
  /**
   * Human-readable descriptor of the bound model (provider, model id, effort,
   * and request profile such as service tier)
   * rendered into the review footer and included in the fingerprint
   * signature, so changing the binding re-reviews instead of skipping.
   */
  readonly modelLabel?: string | undefined;
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
  /** Workflow-run URL rendered into the review footer. */
  readonly runUrl?: string | undefined;
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
 * The changeset fingerprint of what this reviewer WOULD review right now:
 * the ignore-filtered changeset hashed with the prompt signature. Identical
 * fingerprints mean an identical review input surface — the basis for
 * skipping re-reviews after content-free head changes (base auto-merges,
 * equivalent rebases).
 */
const makeFingerprint = (
  signature: (mission: ReturnType<typeof buildReviewMission>) => string,
  ignore: ReadonlyArray<string> | undefined,
) =>
  provideIgnore(
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const metadata = yield* source.metadata;
      const files = yield* source.changedFiles;
      return yield* computeChangesetFingerprint(
        files,
        signature(buildReviewMission(metadata, files)),
      );
    }),
    ignore,
  );

const makeProfileFingerprint = (
  signature: (mission: ReviewMission) => string,
  ignore: ReadonlyArray<string> | undefined,
) =>
  provideIgnore(
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const metadata = yield* source.metadata;
      const files = yield* source.anchorFiles;
      return yield* computeProfileFingerprint(signature(buildProfileMission(metadata, files)));
    }),
    ignore,
  );

const makeReviewSnapshot = (ignore: ReadonlyArray<string> | undefined) =>
  provideIgnore(
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      return {
        metadata: yield* source.metadata,
        files: yield* source.anchorFiles,
      };
    }),
    ignore,
  );

/**
 * Build the flat reviewer: one bounded read-only agent over the whole
 * changeset. Returns the model-agnostic definition, the explicit binding, and
 * a `run` whose error and requirement channels stay fully inferred — the
 * pull-request source, the publisher, `Crypto.Crypto`, extra tool handlers,
 * and the Model Layer's requirements all remain visible to the caller.
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

  // Everything that shapes this reviewer's output: the rendered instructions
  // (mission, guidance, findings bound, contract) plus the verdict mapping.
  const signature = (mission: ReviewMission): string =>
    [
      definition.instructions(mission),
      `applyVerdict=${String(options.applyVerdict ?? false)}`,
      ...(options.modelLabel === undefined ? [] : [`model=${options.modelLabel}`]),
    ].join("\u0000");
  const profileSignature = (mission: ReviewMission): string =>
    [
      "pr-review-profile-v1-flat",
      JSON.stringify(resolveReviewGuidance(options.guidance, mission)),
      JSON.stringify(options.policy ?? {}),
      JSON.stringify(extraTools.map((tool) => tool.name)),
      JSON.stringify(options.ignore ?? []),
      `maxFindings=${clampMaxFindings(options.maxFindings)}`,
      `applyVerdict=${String(options.applyVerdict ?? false)}`,
      ...(options.modelLabel === undefined ? [] : [`model=${options.modelLabel}`]),
    ].join("\u0000");

  const run = (runOptions: RunReviewOptions = {}) =>
    provideIgnore(
      executeReview(binding, {
        post: runOptions.post ?? false,
        applyVerdict: options.applyVerdict ?? false,
        limits: options.budget ?? reviewBudgetLimits,
        maxFindings: clampMaxFindings(options.maxFindings),
        signature,
        modelLabel: options.modelLabel,
        runUrl: runOptions.runUrl,
      }).pipe(Effect.provide(Layer.mergeAll(ReviewToolkitLayer, IdGenerator.layer)), Effect.scoped),
      options.ignore,
    );

  return {
    definition,
    binding,
    run,
    fingerprint: makeFingerprint(signature, options.ignore),
    profileFingerprint: makeProfileFingerprint(profileSignature, options.ignore),
    snapshot: makeReviewSnapshot(options.ignore),
    filterFiles: (files: ReadonlyArray<ChangedFile>) => {
      const ignored = compileIgnoreGlobs(options.ignore ?? []);
      return files.filter((file) => !ignored(file.path));
    },
  } as const;
};

/** Options accepted by `PrReview.makeFanOut` (the host-scheduled pipeline). */
export interface PrReviewFanOutOptions<
  Provider,
  ModelProvides,
  ModelRequires,
> extends PrReviewSharedOptions {
  /** The Effect AI Model bound to every child review pass. */
  readonly model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>;
  /**
   * Static guidance injected into every child reviewer's instructions. The
   * pipeline schedules children from the deterministic plan, so
   * mission-dependent guidance cannot exist for children.
   */
  readonly guidance?: string | ReadonlyArray<string> | undefined;
}

/**
 * Build the fan-out reviewer: host code schedules host-planned general and
 * specialist discovery plus independent candidate verification directly as
 * bounded child runs — there is no coordinator model and no delegation tool,
 * so review assurance cannot fail on scheduling compliance. Host code
 * composes publication only from exactly confirmed candidates. Child
 * execution bounds are packaged and not configurable here.
 */
const makeFanOut = <Provider, ModelProvides, ModelRequires>(
  options: PrReviewFanOutOptions<Provider, ModelProvides, ModelRequires>,
) => {
  const child = makeFileReviewerDefinition({ guidance: options.guidance });
  // Structural binding for the same reason as in `make` above.
  const childBinding = Object.freeze({ definition: child, model: options.model });

  // Everything that shapes this pipeline's output: the child instructions
  // (guidance included) plus the host knobs the children do not carry.
  const guidanceLines =
    options.guidance === undefined
      ? []
      : typeof options.guidance === "string"
        ? [options.guidance]
        : options.guidance;
  const signature = (mission: ReviewMission): string =>
    [
      "pr-review-fan-out-host-scheduled-v1",
      // Children never see the mission, but the documented skip-unchanged
      // contract includes pull-request framing, so the fingerprint keeps it.
      JSON.stringify(Schema.encodeSync(ReviewMission)(mission)),
      `childGuidance=${JSON.stringify(guidanceLines)}`,
      `maxFindings=${clampMaxFindings(options.maxFindings)}`,
      `applyVerdict=${String(options.applyVerdict ?? false)}`,
      ...(options.modelLabel === undefined ? [] : [`model=${options.modelLabel}`]),
    ].join(" ");
  const profileSignature = (_mission: ReviewMission): string =>
    [
      // v4 invalidates continuity produced by the coordinator-scheduled
      // pipeline; the host now schedules every pass and retries failures.
      "pr-review-profile-v4-host-scheduled",
      JSON.stringify(guidanceLines),
      JSON.stringify(options.ignore ?? []),
      `maxFindings=${clampMaxFindings(options.maxFindings)}`,
      `applyVerdict=${String(options.applyVerdict ?? false)}`,
      ...(options.modelLabel === undefined ? [] : [`model=${options.modelLabel}`]),
    ].join(" ");

  const run = (runOptions: RunReviewOptions = {}) =>
    provideIgnore(
      executeFanOutReview(childBinding, {
        post: runOptions.post ?? false,
        applyVerdict: options.applyVerdict ?? false,
        limits: options.budget ?? fanOutReviewBudgetLimits,
        maxFindings: clampMaxFindings(options.maxFindings),
        signature,
        modelLabel: options.modelLabel,
        runUrl: runOptions.runUrl,
      }).pipe(Effect.provide(IdGenerator.layer), Effect.scoped),
      options.ignore,
    );

  return {
    definition: child,
    childBinding,
    run,
    fingerprint: makeFingerprint(signature, options.ignore),
    profileFingerprint: makeProfileFingerprint(profileSignature, options.ignore),
    snapshot: makeReviewSnapshot(options.ignore),
    filterFiles: (files: ReadonlyArray<ChangedFile>) => {
      const ignored = compileIgnoreGlobs(options.ignore ?? []);
      return files.filter((file) => !ignored(file.path));
    },
  } as const;
};

/**
 * The packaged pull-request reviewer factory.
 *
 * - `make` — one flat reviewer over the whole changeset.
 * - `makeFanOut` — a coordinator delegating bounded per-unit child reviews.
 */
export const PrReview = { make, makeFanOut } as const;
