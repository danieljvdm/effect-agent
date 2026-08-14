import {
  Subagent,
  SubagentPolicy,
  SubagentReservationsMemoryLive,
  SubagentRuntime,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  RunId,
  SubmissionId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import { DurableStep, DurableStepError, ToolExecutionClass } from "@effect-agent/engine";
import {
  AbortCommand,
  AgentBindingResolver,
  ApprovalDecisionCommand,
  ConversationExportRequest,
  ConversationStore,
  DefinitionDigests,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointLocation,
  DurableRuntimeFailpointTestControl,
  DurableWorkerBinding,
  IdempotencyKey,
  ObligationThresholds,
  Principal,
  ResolutionAbortSubmission,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  SubmissionLedger,
  SubmissionLookupById,
  UnknownResolutionCommand,
  childConversationIdFor,
  verifyConversationInvariants,
  type CanonicalRecordEnvelope,
  type BatchId,
  type ProducerId,
  type Receipt,
  type ResolvedBinding,
  type Settlement,
  type SubmissionSnapshot,
  type UnknownResolution,
} from "@effect-agent/session";
import { Cause, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { FastCheck } from "effect/testing";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";

/**
 * P7 WP4 chaos machinery (plan §5): a Schema-first `ChaosPlan`, a seeded generator over
 * `effect/testing/FastCheck` (already inside the pinned Effect — no new dependency), and a
 * deterministic runner that drives the durable coordinator over whatever adapter pair the test
 * provides. Every plan ends in the SAME claims the crash matrices make:
 *
 * 1. `verifyConversationInvariants` in convergence mode over every touched Conversation (the
 *    shared WP1 checker — one set of claims for admin verify, certification, chaos, and soak);
 * 2. `scanObligations` returning ZERO entries (everything settled; nothing invisibly stuck);
 * 3. supplier non-fabrication wherever the deterministic desk was in play (durability §10: no
 *    canonical Tool success exists that the external store did not actually produce).
 *
 * Replay contract: the memory/SQLite chaos tests derive every plan from one root seed
 * (`CHAOS_SEED` env override; see `chaosSeedFromEnv`) and print that seed plus the failing
 * plan's own seed in the failure output, so any red run is replayable byte-for-byte.
 */

// ---------------------------------------------------------------------------
// ChaosPlan schema
// ---------------------------------------------------------------------------

/** The six durable scenario flavors a chaos lane can exercise (plan §5). */
export const ChaosScenarioKind = Schema.Literals([
  "plain",
  "uncertain-tool",
  "durable-steps",
  "approval",
  "join",
  "delegation",
]);
export type ChaosScenarioKind = typeof ChaosScenarioKind.Type;

const LaneIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(7));

/** One Submission of a plan: which lane it queues into and that lane's scenario flavor. */
export class ChaosSubmissionSpec extends Schema.Class<ChaosSubmissionSpec>(
  "@effect-agent/testing/ChaosSubmissionSpec",
)({
  lane: LaneIndex,
  /** The lane's flavor; the FIRST spec of a lane fixes the lane's agent. */
  kind: ChaosScenarioKind,
}) {}

/** How the runner resolves a durable Unknown Outcome it encounters (DUR-017 driver). */
export const ChaosResolutionKind = Schema.Literals([
  /** The call provably never started: the batch resumes and executes it. */
  "never-happened",
  /**
   * Recovered supplier truth: resolve with the EXACT value the desk produced. Falls back to
   * `never-happened` when the desk holds no value for the call, so the runner never fabricates.
   */
  "completed-from-supplier",
  /** Unresolvable: route into the abort path (settles aborted, audit retained). */
  "abort-submission",
]);
export type ChaosResolutionKind = typeof ChaosResolutionKind.Type;

export const ChaosApprovalDecision = Schema.Literals(["approved", "denied"]);
export type ChaosApprovalDecision = typeof ChaosApprovalDecision.Type;

const BoundedAdapterArm = Schema.String.check(Schema.isMaxLength(128));

/**
 * One seeded chaos plan (plan §5): the full fault schedule is data, so a failing run replays
 * from the plan alone. `failpointArms` are coordinator locations; `adapterArms` are
 * adapter-owned location names the adapter test validates (the memory runner has none).
 */
export class ChaosPlan extends Schema.Class<ChaosPlan>("@effect-agent/testing/ChaosPlan")({
  /** Identifies this plan in failure output; derived from the root seed plus the plan index. */
  seed: Schema.Int,
  /** Lane count; submissions address lanes `0..lanes-1`. */
  lanes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1), Schema.isLessThanOrEqualTo(8)),
  submissions: Schema.NonEmptyArray(ChaosSubmissionSpec),
  /** Coordinator failpoint arms, consumed one per round (each fails every hit that round). */
  failpointArms: Schema.Array(DurableRuntimeFailpointLocation),
  /** Adapter-owned failpoint arms (e.g. SQLite `ledger:*`/`append:*` locations). */
  adapterArms: Schema.Array(BoundedAdapterArm),
  /** Flattened submission indices to abort mid-plan (modulo the submission count). */
  abortInjections: Schema.Array(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  /** Resolution choices for Unknown Outcomes, indexed deterministically per open call. */
  resolutionInjections: Schema.Array(ChaosResolutionKind),
  /** Approval decisions for suspended approval lanes, indexed deterministically per call. */
  approvalDecisions: Schema.Array(ChaosApprovalDecision),
}) {}

/** Per-lane verification result inside a plan report. */
export class ChaosLaneReport extends Schema.Class<ChaosLaneReport>(
  "@effect-agent/testing/ChaosLaneReport",
)({
  conversationId: ConversationId,
  kind: ChaosScenarioKind,
  submissionCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Verdict of `verifyConversationInvariants` in convergence mode. */
  verified: Schema.Boolean,
}) {}

/** The Schema-first outcome of one executed chaos plan. */
export class ChaosPlanReport extends Schema.Class<ChaosPlanReport>(
  "@effect-agent/testing/ChaosPlanReport",
)({
  seed: Schema.Int,
  rounds: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  lanes: Schema.Array(ChaosLaneReport),
  /** `scanObligations` entries after convergence — MUST be zero. */
  openObligations: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** Typed convergence/verification failure of one chaos plan (never a bare defect). */
export class ChaosConvergenceFailure extends Schema.TaggedErrorClass<ChaosConvergenceFailure>()(
  "ChaosConvergenceFailure",
  {
    seed: Schema.Int,
    message: Schema.String.check(Schema.isMaxLength(16_384)),
  },
) {}

// ---------------------------------------------------------------------------
// Seeded generation
// ---------------------------------------------------------------------------

/** Default root seed for chaos suites; override with the `CHAOS_SEED` environment variable. */
export const DEFAULT_CHAOS_SEED = 20260813;

/** The root seed for this run: `CHAOS_SEED` when set to an integer, the default otherwise. */
export const chaosSeedFromEnv = (env: Record<string, string | undefined>): number => {
  const raw = env["CHAOS_SEED"];
  if (raw === undefined || raw === "") return DEFAULT_CHAOS_SEED;
  const parsed = Number.parseInt(raw, 10);
  return Number.isSafeInteger(parsed) ? parsed : DEFAULT_CHAOS_SEED;
};

export interface ChaosGeneratorOptions {
  /** Root seed (print this in failure output for replay). */
  readonly seed: number;
  /** How many plans to derive. */
  readonly count: number;
  /** Adapter failpoint location names available to arm (empty for memory). */
  readonly adapterArms?: ReadonlyArray<string> | undefined;
}

interface GeneratedLane {
  readonly kind: ChaosScenarioKind;
  readonly depth: number;
}

const laneArbitrary: FastCheck.Arbitrary<GeneratedLane> = FastCheck.constantFrom<ChaosScenarioKind>(
  "plain",
  "uncertain-tool",
  "durable-steps",
  "approval",
  "join",
  "delegation",
).chain(
  (kind): FastCheck.Arbitrary<GeneratedLane> =>
    kind === "join"
      ? FastCheck.integer({ min: 2, max: 3 }).map((depth): GeneratedLane => ({ kind, depth }))
      : kind === "plain"
        ? FastCheck.integer({ min: 1, max: 2 }).map((depth): GeneratedLane => ({ kind, depth }))
        : FastCheck.constant<GeneratedLane>({ kind, depth: 1 }),
);

interface ChaosPlanShape {
  readonly lanes: number;
  readonly submissions: readonly [ChaosSubmissionSpec, ...Array<ChaosSubmissionSpec>];
  readonly failpointArms: ReadonlyArray<DurableRuntimeFailpointLocation>;
  readonly adapterArms: ReadonlyArray<string>;
  readonly abortInjections: ReadonlyArray<number>;
  readonly resolutionInjections: ReadonlyArray<ChaosResolutionKind>;
  readonly approvalDecisions: ReadonlyArray<ChaosApprovalDecision>;
}

const planShapeArbitrary = (
  adapterArms: ReadonlyArray<string>,
): FastCheck.Arbitrary<ChaosPlanShape> =>
  FastCheck.record({
    lanes: FastCheck.array(laneArbitrary, { minLength: 1, maxLength: 3 }),
    failpointArms: FastCheck.uniqueArray(
      FastCheck.constantFrom(...DurableRuntimeFailpointLocation.literals),
      { maxLength: 3 },
    ),
    adapterArms:
      adapterArms.length === 0
        ? FastCheck.constant<Array<string>>([])
        : FastCheck.uniqueArray(FastCheck.constantFrom(...adapterArms), { maxLength: 2 }),
    abortInjections: FastCheck.uniqueArray(FastCheck.integer({ min: 0, max: 15 }), {
      maxLength: 2,
    }),
    resolutionInjections: FastCheck.array(
      FastCheck.constantFrom<ChaosResolutionKind>(
        "never-happened",
        "completed-from-supplier",
        "abort-submission",
      ),
      { maxLength: 4 },
    ),
    approvalDecisions: FastCheck.array(
      FastCheck.constantFrom<ChaosApprovalDecision>("approved", "denied"),
      { maxLength: 2 },
    ),
  }).map((shape) => {
    const submissions = shape.lanes.flatMap((lane, index) =>
      Array.from({ length: lane.depth }, () =>
        ChaosSubmissionSpec.make({ lane: index, kind: lane.kind }),
      ),
    );
    const [first, ...rest] = submissions;
    // `lanes` >= 1 and every lane has depth >= 1, so `first` always exists.
    if (first === undefined) throw new Error("chaos generator produced an empty plan");
    return {
      lanes: shape.lanes.length,
      submissions: [first, ...rest] as const,
      failpointArms: shape.failpointArms,
      adapterArms: shape.adapterArms,
      abortInjections: shape.abortInjections,
      resolutionInjections: shape.resolutionInjections,
      approvalDecisions: shape.approvalDecisions,
    };
  });

/**
 * Derive `count` chaos plans deterministically from one root seed. The same
 * `{seed, count, adapterArms}` triple always yields byte-identical plans, so a failure line
 * `CHAOS_SEED=<seed>` replays the exact schedule.
 */
export const generateChaosPlans = (options: ChaosGeneratorOptions): ReadonlyArray<ChaosPlan> => {
  const sampled = FastCheck.sample(planShapeArbitrary(options.adapterArms ?? []), {
    seed: options.seed,
    numRuns: options.count,
  });
  return sampled.map((shape, index) =>
    ChaosPlan.make({ ...shape, seed: (Math.imul(options.seed, 31) + index) | 0 }),
  );
};

/** Deterministic PRNG for the runner's small ordering choices (lane drive order). */
const mulberry32 = (seed: number): (() => number) => {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

// ---------------------------------------------------------------------------
// Lane fixtures (agents, scripted models, deterministic desk)
// ---------------------------------------------------------------------------

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

const toolCallPart = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

/**
 * Prompt-shaped scripted model: the response depends ONLY on the request prompt, so it stays
 * deterministic across Attempt re-invocations, batch resumes, and joined steering — no counter
 * to drift when chaos re-enters a Turn.
 */
const promptScriptedModel = (
  label: string,
  script: (prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    label,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => Stream.fromIterable(script(request.prompt)),
      }),
    ),
  );

const lastRole = (prompt: Prompt.Prompt): string | undefined => prompt.content.at(-1)?.role;

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

const PlainInput = Schema.Struct({ question: Schema.String });
const PlainOutput = Schema.Struct({ answer: Schema.String });

const plainDefinition = Agent.define("chaos-plain", {
  input: PlainInput,
  output: PlainOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

/** Unannotated → fail-closed `uncertain`: enters the prepared/settled protocol (DUR-009). */
const BookUncertain = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
const bookTools = Toolkit.make(BookUncertain);
const bookDefinition = Agent.define("chaos-book", {
  input: PlainInput,
  output: PlainOutput,
  instructions: "Book it.",
  toolkit: bookTools,
  policy,
});

const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
const approvalTools = Toolkit.make(BookApproval);
const approvalDefinition = Agent.define("chaos-approval", {
  input: PlainInput,
  output: PlainOutput,
  instructions: "Book after approval.",
  toolkit: approvalTools,
  policy,
});

const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
}).annotate(ToolExecutionClass, "uncertain");
const itineraryTools = Toolkit.make(Itinerary);
const itineraryDefinition = Agent.define("chaos-itinerary", {
  input: PlainInput,
  output: PlainOutput,
  instructions: "Reserve the itinerary.",
  toolkit: itineraryTools,
  policy,
});

const childDefinition = Agent.define("chaos-child", {
  input: PlainInput,
  output: PlainOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

class ChaosDelegationFailed extends Schema.TaggedErrorClass<ChaosDelegationFailed>()(
  "ChaosDelegationFailed",
  { childErrorTag: Schema.String },
) {}

const chaosDelegation = Subagent.define("delegate_chaos", {
  description: "Delegate one bounded chaos question.",
  target: childDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: ChaosDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `chaos:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
  }),
});

const coordinatorDefinition = Agent.define("chaos-coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then report as JSON.",
  toolkit: Toolkit.make(chaosDelegation.tool),
  policy,
});

const DELEGATE_CALL_ID = "chaos-delegate-1";

const HEX = "0123456789abcdef";
const decodeDigest = Schema.decodeSync(Digest);
const laneDigests = (lane: number): DefinitionDigests => {
  const digest = decodeDigest(HEX[lane % 8]!.repeat(64));
  return DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
};
const childDigestStrings = (lane: number) => {
  const char = HEX[8 + (lane % 8)]!;
  return { agent: char.repeat(64), model: char.repeat(64), tools: char.repeat(64) } as const;
};
const childLaneDigests = (lane: number): DefinitionDigests => {
  const strings = childDigestStrings(lane);
  return DefinitionDigests.make({
    agent: decodeDigest(strings.agent),
    model: decodeDigest(strings.model),
    tools: decodeDigest(strings.tools),
  });
};

const CHAOS_PRINCIPAL = Schema.decodeSync(Principal)("principal-chaos");
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const chaosIdentifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);
    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );
    return {
      nextConversationId: next(decodeConversationId, "chaos-fixture-conversation"),
      nextRunId: next(decodeRunId, "chaos-fixture-run"),
      nextTurnId: next(decodeTurnId, "chaos-fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, chaosIdentifiers);

/**
 * The deterministic external desk of one plan: every produced value is recorded so the final
 * non-fabrication sweep can prove each canonical Tool success came from here (durability §10).
 */
interface ChaosDesk {
  readonly produced: Effect.Effect<ReadonlySet<string>>;
  readonly record: (value: string) => Effect.Effect<void>;
}

const makeChaosDesk: Effect.Effect<ChaosDesk> = Effect.gen(function* () {
  const produced = yield* Ref.make<ReadonlySet<string>>(new Set());
  return {
    produced: Ref.get(produced),
    record: (value: string) => Ref.update(produced, (current) => new Set(current).add(value)),
  };
});

const bookConfirmation = (ref: string): string => `confirmed-${ref}`;
const flightValue = (ref: string): string => `flight-${ref}`;
const lodgingValue = (ref: string): string => `lodging-${ref}`;

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/** Adapter-owned failpoint control the SQLite runner supplies; memory has none. */
export interface ChaosAdapterFailpoints {
  readonly arm: (location: string) => Effect.Effect<void>;
  readonly clear: Effect.Effect<void>;
}

export interface ChaosRunOptions {
  readonly adapterFailpoints?: ChaosAdapterFailpoints | undefined;
  /**
   * Executed at the end of every round. Adapters whose ownership leases block every new claim
   * until expiry (the SQLite ledger's D5 semantics — expiry only revokes the liveness
   * assumption; producer epochs stay the correctness fence) pass a deterministic
   * `TestClock.adjust` here so a dead Attempt's lane becomes reclaimable next round. The memory
   * ledger needs nothing: it allows same-producer reclaim under a live lease.
   */
  readonly betweenRounds?: Effect.Effect<void> | undefined;
}

/** Success → Some; typed failure → None (chaos tolerates it); defect → rethrown loudly. */
const tolerateTyped = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<Option.Option<A>, never, R> =>
  effect.pipe(
    Effect.exit,
    Effect.flatMap((exit) => {
      if (Exit.isSuccess(exit)) return Effect.succeed(Option.some(exit.value));
      if (Option.isSome(Cause.findErrorOption(exit.cause))) {
        return Effect.succeed(Option.none<A>());
      }
      return Effect.die(new Error(`chaos step died: ${Cause.pretty(exit.cause)}`));
    }),
  );

interface LaneFixture {
  readonly index: number;
  readonly kind: ChaosScenarioKind;
  readonly conversationId: ConversationId;
  readonly ref: string;
  readonly deskInPlay: boolean;
  readonly submissionIndexes: ReadonlyArray<number>;
  readonly submitOne: (flatIndex: number) => Effect.Effect<Receipt, unknown>;
  readonly drives: (
    firstReceipt: Receipt | undefined,
  ) => ReadonlyArray<Effect.Effect<ReadonlyArray<Settlement>, unknown>>;
  readonly childConversationOf: (firstReceipt: Receipt) => ConversationId | undefined;
}

interface SubmissionState {
  readonly flatIndex: number;
  readonly lane: LaneFixture;
  receipt: Receipt | undefined;
}

const scriptFor = (
  kind: ChaosScenarioKind,
  ref: string,
): ((prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded>) => {
  switch (kind) {
    case "plain":
    case "join":
      return () => finalParts('{"answer":"chaos"}');
    case "uncertain-tool":
    case "approval":
      return (prompt) =>
        lastRole(prompt) === "tool"
          ? finalParts('{"answer":"booked"}')
          : toolTurn(toolCallPart(`book-${ref}`, "book", { ref }));
    case "durable-steps":
      return (prompt) =>
        lastRole(prompt) === "tool"
          ? finalParts('{"answer":"reserved"}')
          : toolTurn(toolCallPart(`itinerary-${ref}`, "itinerary", { ref }));
    case "delegation":
      return (prompt) =>
        lastRole(prompt) === "tool"
          ? finalParts('{"report":"done"}')
          : toolTurn(toolCallPart(DELEGATE_CALL_ID, "delegate_chaos", { topic: ref }));
  }
};

const makeLaneFixture = Effect.fn("Chaos.makeLaneFixture")(function* (
  plan: ChaosPlan,
  laneIndex: number,
  kind: ChaosScenarioKind,
  submissionIndexes: ReadonlyArray<number>,
  desk: ChaosDesk,
) {
  const runtime = yield* DurableAgentRuntime;
  const conversationId = decodeConversationId(`chaos-${plan.seed}-lane-${laneIndex}`);
  const ref = `ref-l${laneIndex}`;
  const script = scriptFor(kind, ref);
  const model = promptScriptedModel(`chaos-${kind}-${laneIndex}`, script);
  const digests = laneDigests(laneIndex);

  const submitOptionsFor = (flatIndex: number) => ({
    conversationId,
    principal: CHAOS_PRINCIPAL,
    idempotencyKey: decodeIdempotencyKey(`chaos-${plan.seed}-s${flatIndex}`),
    definitions: digests,
  });

  const bookToolLayerFor = (tools: typeof bookTools | typeof approvalTools) =>
    tools.toLayer({
      book: ({ ref: called }) =>
        desk
          .record(bookConfirmation(called))
          .pipe(Effect.as({ confirmation: bookConfirmation(called) })),
    });

  const plainLaneFixture = (
    deskInPlay: boolean,
    drive: Effect.Effect<ReadonlyArray<Settlement>, unknown>,
    submitOne: (flatIndex: number) => Effect.Effect<Receipt, unknown>,
  ): LaneFixture => ({
    index: laneIndex,
    kind,
    conversationId,
    ref,
    deskInPlay,
    submissionIndexes,
    submitOne,
    drives: () => [drive],
    childConversationOf: () => undefined,
  });

  switch (kind) {
    case "plain":
    case "join": {
      const agent = Agent.withModel(plainDefinition, model);
      return plainLaneFixture(
        false,
        runtime.processConversation(agent, conversationId),
        (flatIndex) =>
          runtime.submit(agent, { question: `chaos ${flatIndex}` }, submitOptionsFor(flatIndex)),
      );
    }
    case "uncertain-tool": {
      const agent = Agent.withModel(bookDefinition, model);
      return plainLaneFixture(
        true,
        runtime
          .processConversation(agent, conversationId)
          .pipe(Effect.provide(bookToolLayerFor(bookTools))),
        (flatIndex) =>
          runtime.submit(agent, { question: `chaos ${flatIndex}` }, submitOptionsFor(flatIndex)),
      );
    }
    case "approval": {
      const agent = Agent.withModel(approvalDefinition, model);
      return plainLaneFixture(
        true,
        runtime
          .processConversation(agent, conversationId)
          .pipe(Effect.provide(bookToolLayerFor(approvalTools))),
        (flatIndex) =>
          runtime.submit(agent, { question: `chaos ${flatIndex}` }, submitOptionsFor(flatIndex)),
      );
    }
    case "durable-steps": {
      const agent = Agent.withModel(itineraryDefinition, model);
      const toolLayer = itineraryTools.toLayer({
        itinerary: ({ ref: called }) =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            const flight = yield* step.do(
              "reserve-flight",
              Schema.String,
              desk.record(flightValue(called)).pipe(Effect.as(flightValue(called))),
            );
            const lodging = yield* step.do(
              "reserve-lodging",
              Schema.String,
              desk.record(lodgingValue(called)).pipe(Effect.as(lodgingValue(called))),
            );
            return { state: `${flight}+${lodging}` };
          }),
      });
      return plainLaneFixture(
        true,
        runtime.processConversation(agent, conversationId).pipe(Effect.provide(toolLayer)),
        (flatIndex) =>
          runtime.submit(agent, { question: `chaos ${flatIndex}` }, submitOptionsFor(flatIndex)),
      );
    }
    case "delegation": {
      const parentBinding = Agent.withModel(coordinatorDefinition, model);
      const childModel = promptScriptedModel(`chaos-child-${laneIndex}`, () =>
        finalParts('{"answer":"child"}'),
      );
      const childBinding = Agent.withModel(childDefinition, childModel);
      const delegationLayer = SubagentRuntime.layer(chaosDelegation, childBinding, {
        mapChildFailure: (failure) => ChaosDelegationFailed.make({ childErrorTag: failure._tag }),
        durable: { targetDigests: childDigestStrings(laneIndex) },
      }).pipe(Layer.provide(delegationSupport));
      const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
        parentBinding,
        digests,
      ).pipe(Effect.provide(delegationLayer));
      const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
        childBinding,
        childLaneDigests(laneIndex),
      );
      const resolver = AgentBindingResolver.fromBindings([parentResolved, childResolved]);
      const driveResolved = (conversation: ConversationId) =>
        runtime
          .processConversationResolved(conversation)
          .pipe(Effect.provideService(AgentBindingResolver, resolver));
      const fixture: LaneFixture = {
        index: laneIndex,
        kind,
        conversationId,
        ref,
        deskInPlay: false,
        submissionIndexes,
        submitOne: (flatIndex) =>
          runtime.submit(
            { definition: { id: coordinatorDefinition.id, input: coordinatorDefinition.input } },
            { mission: `chaos ${flatIndex}` },
            submitOptionsFor(flatIndex),
          ),
        drives: (firstReceipt) => {
          const drives: Array<Effect.Effect<ReadonlyArray<Settlement>, unknown>> = [
            driveResolved(conversationId),
          ];
          if (firstReceipt !== undefined) {
            drives.push(
              driveResolved(
                childConversationIdFor(
                  firstReceipt.submissionId,
                  decodeToolCallId(DELEGATE_CALL_ID),
                ),
              ),
            );
          }
          return drives;
        },
        childConversationOf: (firstReceipt) =>
          childConversationIdFor(firstReceipt.submissionId, decodeToolCallId(DELEGATE_CALL_ID)),
      };
      return fixture;
    }
  }
});

/** Stable per-call index into an injection list (identical across resolution passes). */
const injectionIndex = (submissionFlatIndex: number, callId: string, length: number): number => {
  let hash = submissionFlatIndex + 1;
  for (const char of callId) hash = (Math.imul(hash, 31) + char.charCodeAt(0)) | 0;
  return ((hash % length) + length) % length;
};

const resolutionFor = (
  kind: ChaosResolutionKind,
  toolName: string,
  ref: string,
  produced: ReadonlySet<string>,
): UnknownResolution => {
  switch (kind) {
    case "abort-submission":
      return ResolutionAbortSubmission.make();
    case "completed-from-supplier": {
      if (toolName === "book" && produced.has(bookConfirmation(ref))) {
        return ResolutionCompletedWithResult.make({
          result: { confirmation: bookConfirmation(ref) },
          isFailure: false,
        });
      }
      if (
        toolName === "itinerary" &&
        produced.has(flightValue(ref)) &&
        produced.has(lodgingValue(ref))
      ) {
        return ResolutionCompletedWithResult.make({
          result: { state: `${flightValue(ref)}+${lodgingValue(ref)}` },
          isFailure: false,
        });
      }
      // The desk never produced a value for this call — resolving "completed" would fabricate.
      return ResolutionNeverHappened.make();
    }
    case "never-happened":
      return ResolutionNeverHappened.make();
  }
};

/** Drive one DUR-017 pass: resolve Unknown Outcomes and pending approvals from the plan. */
const resolutionPass = Effect.fn("Chaos.resolutionPass")(function* (
  plan: ChaosPlan,
  states: ReadonlyArray<SubmissionState>,
  desk: ChaosDesk,
) {
  const runtime = yield* DurableAgentRuntime;
  const ledger = yield* SubmissionLedger;
  const produced = yield* desk.produced;
  const nonterminal = yield* tolerateTyped(Stream.runCollect(ledger.scanNonterminal));
  if (Option.isNone(nonterminal)) return;
  const byId = new Map<SubmissionId, SubmissionState>();
  for (const state of states) {
    if (state.receipt !== undefined) byId.set(state.receipt.submissionId, state);
  }
  for (const row of nonterminal.value) {
    if (row.state !== "unknown" && row.state !== "suspended") continue;
    const state = byId.get(row.submissionId);
    const explanation = yield* tolerateTyped(runtime.explain(row.submissionId));
    if (Option.isNone(explanation)) continue;
    const flatIndex = state?.flatIndex ?? 0;
    const ref = state?.lane.ref ?? "ref-child";
    if (row.state === "unknown") {
      for (const call of explanation.value.evidence.unknownCalls) {
        if (call.resolved) continue;
        const kind =
          plan.resolutionInjections.length === 0
            ? "never-happened"
            : plan.resolutionInjections[
                injectionIndex(flatIndex, call.toolCallId, plan.resolutionInjections.length)
              ]!;
        yield* tolerateTyped(
          runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: row.submissionId,
              toolCallId: call.toolCallId,
              author: "chaos-runner",
              reason: `chaos plan ${plan.seed} resolution (${kind})`,
              resolution: resolutionFor(kind, call.toolName, ref, produced),
            }),
          ),
        );
      }
    } else {
      for (const pending of explanation.value.evidence.approvalsPending) {
        const decision =
          plan.approvalDecisions.length === 0
            ? "approved"
            : plan.approvalDecisions[
                injectionIndex(flatIndex, pending.toolCallId, plan.approvalDecisions.length)
              ]!;
        yield* tolerateTyped(
          runtime.resolveApproval(
            ApprovalDecisionCommand.make({
              submissionId: row.submissionId,
              toolCallId: pending.toolCallId,
              decision,
              resolver: "chaos-runner",
              reason: `chaos plan ${plan.seed} approval (${decision})`,
            }),
          ),
        );
      }
    }
  }
});

const submissionIdsNamedBy = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): ReadonlySet<SubmissionId> => {
  const named = new Set<SubmissionId>();
  for (const envelope of records) {
    const payload = envelope.record.payload;
    if (
      payload._tag === "UserInputRecorded" ||
      payload._tag === "SubmissionSettled" ||
      payload._tag === "AbortRequested"
    ) {
      named.add(payload.submissionId);
    }
  }
  return named;
};

/**
 * The final non-fabrication sweep (durability §10): every canonical Tool success recorded on a
 * desk-backed lane must be a value the desk actually produced.
 */
const BookResult = Schema.Struct({ confirmation: Schema.String });
const ItineraryResult = Schema.Struct({ state: Schema.String });
const decodeBookResult = Schema.decodeUnknownOption(BookResult);
const decodeItineraryResult = Schema.decodeUnknownOption(ItineraryResult);
const decodeStepOutput = Schema.decodeUnknownOption(Schema.String);

const assertNoFabrication = (
  plan: ChaosPlan,
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  produced: ReadonlySet<string>,
): Effect.Effect<void, ChaosConvergenceFailure> => {
  const fabricated: Array<string> = [];
  const requireProduced = (value: string, label: string): void => {
    if (!produced.has(value)) fabricated.push(`${label} "${value}"`);
  };
  for (const envelope of records) {
    const payload = envelope.record.payload;
    if (payload._tag === "ToolCallSettled" && !payload.isFailure) {
      if (payload.toolName === "book") {
        const result = decodeBookResult(payload.result);
        if (Option.isSome(result)) requireProduced(result.value.confirmation, "book result");
      }
      if (payload.toolName === "itinerary") {
        const result = decodeItineraryResult(payload.result);
        if (Option.isSome(result)) {
          for (const part of result.value.state.split("+")) {
            requireProduced(part, "itinerary step result");
          }
        }
      }
    }
    if (payload._tag === "ToolStepSettled") {
      const output = decodeStepOutput(payload.output);
      if (Option.isSome(output)) requireProduced(output.value, "step output");
    }
  }
  return fabricated.length === 0
    ? Effect.void
    : Effect.fail(
        ChaosConvergenceFailure.make({
          seed: plan.seed,
          message: `fabricated Tool results absent from the desk: ${fabricated.join(", ")}`,
        }),
      );
};

/**
 * Execute one chaos plan against whatever adapters the ambient Layer provides and end in the
 * shared invariant claims. Deterministic: same plan + same adapters → same schedule.
 */
export const runChaosPlan = Effect.fn("Chaos.runChaosPlan")(function* (
  plan: ChaosPlan,
  options?: ChaosRunOptions,
) {
  const runtime = yield* DurableAgentRuntime;
  const ledger = yield* SubmissionLedger;
  const store = yield* ConversationStore;
  const config = yield* DurableRuntimeConfig;
  const failpoints = yield* DurableRuntimeFailpointTestControl;
  const random = mulberry32(plan.seed);
  const desk = yield* makeChaosDesk;

  // Lane fixtures: the FIRST spec of each lane fixes the lane's agent kind.
  const laneKinds = new Map<number, ChaosScenarioKind>();
  const laneSubmissions = new Map<number, Array<number>>();
  plan.submissions.forEach((spec, flatIndex) => {
    const lane = spec.lane % plan.lanes;
    if (!laneKinds.has(lane)) laneKinds.set(lane, spec.kind);
    const list = laneSubmissions.get(lane) ?? [];
    list.push(flatIndex);
    laneSubmissions.set(lane, list);
  });
  const lanes: Array<LaneFixture> = [];
  for (const [lane, kind] of laneKinds) {
    lanes.push(yield* makeLaneFixture(plan, lane, kind, laneSubmissions.get(lane) ?? [], desk));
  }

  const states: Array<SubmissionState> = plan.submissions.map((spec, flatIndex) => ({
    flatIndex,
    lane: lanes.find((fixture) => fixture.index === spec.lane % plan.lanes)!,
    receipt: undefined,
  }));
  const appliedAborts = new Set<number>();

  type ArmEntry =
    | { readonly family: "coordinator"; readonly location: DurableRuntimeFailpointLocation }
    | { readonly family: "adapter"; readonly location: string };
  const armQueue: Array<ArmEntry> = [
    ...plan.failpointArms.map((location): ArmEntry => ({ family: "coordinator", location })),
    ...plan.adapterArms.map((location): ArmEntry => ({ family: "adapter", location })),
  ];

  const allSettled = Effect.gen(function* () {
    if (states.some((state) => state.receipt === undefined)) return false;
    const nonterminal = yield* tolerateTyped(Stream.runCollect(ledger.scanNonterminal));
    return Option.isSome(nonterminal) && Array.from(nonterminal.value).length === 0;
  });

  const maxRounds = armQueue.length + states.length * 2 + 12;
  let rounds = 0;
  let converged = false;
  for (let round = 0; round < maxRounds; round++) {
    rounds = round + 1;
    const arm = armQueue[round];
    if (arm?.family === "coordinator") {
      const location = arm.location;
      yield* failpoints.setHandler((hit) =>
        hit === location
          ? Effect.fail(DurableRuntimeFailpointError.make({ location: hit }))
          : Effect.void,
      );
    } else if (arm?.family === "adapter" && options?.adapterFailpoints !== undefined) {
      yield* options.adapterFailpoints.arm(arm.location);
    }

    // Admission chaos: pending submissions retry under the active arm until a Receipt lands;
    // the identical (conversation, principal, key) triple reattaches, never duplicates.
    for (const state of states) {
      if (state.receipt !== undefined) continue;
      const receipt = yield* tolerateTyped(state.lane.submitOne(state.flatIndex));
      if (Option.isSome(receipt)) state.receipt = receipt.value;
    }

    // Drive every lane (and discovered child lanes) in seeded order under the active arm.
    const order = [...lanes].sort(() => random() - 0.5);
    for (const lane of order) {
      const firstFlat = lane.submissionIndexes[0];
      const firstReceipt = firstFlat === undefined ? undefined : states[firstFlat]?.receipt;
      for (const drive of lane.drives(firstReceipt)) {
        yield* tolerateTyped(drive);
      }
    }

    // Abort injections fire once, while arms may still be active (abort:after-intent etc.).
    if (round >= 1) {
      for (const rawIndex of plan.abortInjections) {
        const index = rawIndex % states.length;
        if (appliedAborts.has(index)) continue;
        const receipt = states[index]?.receipt;
        if (receipt === undefined) continue;
        appliedAborts.add(index);
        yield* tolerateTyped(
          runtime.abort(
            AbortCommand.make({
              submissionId: receipt.submissionId,
              author: "chaos-runner",
              reason: `chaos plan ${plan.seed} abort injection`,
            }),
          ),
        );
      }
    }

    // First resolution pass runs under the arm so resolve:* locations can fire.
    yield* resolutionPass(plan, states, desk);

    yield* failpoints.clear;
    if (options?.adapterFailpoints !== undefined) yield* options.adapterFailpoints.clear;

    yield* tolerateTyped(runtime.runRecovery);
    // Second, unarmed pass guarantees forward progress for newly marked Unknown lanes.
    yield* resolutionPass(plan, states, desk);

    if (yield* allSettled) {
      converged = true;
      break;
    }
    if (options?.betweenRounds !== undefined) yield* options.betweenRounds;
  }

  if (!converged) {
    const nonterminal = yield* tolerateTyped(Stream.runCollect(ledger.scanNonterminal));
    const detail = Option.isSome(nonterminal)
      ? Array.from(nonterminal.value)
          .map((row: SubmissionSnapshot) => `${row.submissionId}(${row.state})`)
          .join(", ")
      : "ledger scan failed";
    return yield* ChaosConvergenceFailure.make({
      seed: plan.seed,
      message: `plan did not converge within ${maxRounds} rounds; nonterminal: [${detail}]; pending receipts: ${states.filter((state) => state.receipt === undefined).length}`,
    });
  }

  // Final claims: the shared invariant checker per touched Conversation, in convergence mode,
  // with the full digest chain (single known producer), plus the desk non-fabrication sweep.
  const produced = yield* desk.produced;
  const laneReports: Array<ChaosLaneReport> = [];
  const verifyConversation = Effect.fn("Chaos.verifyConversation")(function* (
    conversationId: ConversationId,
    kind: ChaosScenarioKind,
    deskInPlay: boolean,
  ) {
    const exported = yield* store.export(ConversationExportRequest.make({ conversationId })).pipe(
      Effect.mapError((error) =>
        ChaosConvergenceFailure.make({
          seed: plan.seed,
          message: `export of ${conversationId} failed: ${String(error)}`,
        }),
      ),
    );
    const rows: Array<SubmissionSnapshot> = [];
    for (const submissionId of submissionIdsNamedBy(exported.records)) {
      const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId })).pipe(
        Effect.mapError((error) =>
          ChaosConvergenceFailure.make({
            seed: plan.seed,
            message: `lookup of ${submissionId} failed: ${String(error)}`,
          }),
        ),
      );
      if (Option.isSome(found)) rows.push(found.value);
    }
    const batchProducers = new Map<BatchId, ProducerId>(
      exported.records.map((envelope) => [envelope.batchId, config.producerId]),
    );
    const report = yield* verifyConversationInvariants({
      export: exported,
      submissions: rows,
      batchProducers,
      requireAllSettled: true,
    });
    if (!report.ok) {
      const failed = report.checks
        .filter((check) => check.status === "failed")
        .map((check) => `${check.name}: ${check.detail ?? "failed"}`)
        .join("; ");
      return yield* ChaosConvergenceFailure.make({
        seed: plan.seed,
        message: `invariants failed for ${conversationId} (${kind}): ${failed}`,
      });
    }
    if (deskInPlay) {
      yield* assertNoFabrication(plan, exported.records, produced);
    }
    laneReports.push(
      ChaosLaneReport.make({
        conversationId,
        kind,
        submissionCount: rows.length,
        verified: report.ok,
      }),
    );
  });

  for (const lane of lanes) {
    yield* verifyConversation(lane.conversationId, lane.kind, lane.deskInPlay);
    // Delegation lanes: verify every materialized child Conversation too.
    for (const flatIndex of lane.submissionIndexes) {
      const receipt = states[flatIndex]?.receipt;
      if (receipt === undefined) continue;
      const child = lane.childConversationOf(receipt);
      if (child === undefined) continue;
      const childExport = yield* Effect.exit(
        store.export(ConversationExportRequest.make({ conversationId: child })),
      );
      if (Exit.isSuccess(childExport) && childExport.value.records.length > 0) {
        yield* verifyConversation(child, "plain", false);
      }
    }
  }

  const obligations = yield* runtime
    .scanObligations(ObligationThresholds.make({ agingSeconds: 0, overdueSeconds: 0 }))
    .pipe(
      Effect.mapError((error) =>
        ChaosConvergenceFailure.make({
          seed: plan.seed,
          message: `scanObligations failed: ${String(error)}`,
        }),
      ),
    );
  if (obligations.entries.length > 0) {
    return yield* ChaosConvergenceFailure.make({
      seed: plan.seed,
      message: `open obligations after convergence: ${obligations.entries
        .map((entry) => `${entry.submissionId}(${entry.blockedOn})`)
        .join(", ")}`,
    });
  }

  return ChaosPlanReport.make({
    seed: plan.seed,
    rounds,
    lanes: laneReports,
    openObligations: obligations.entries.length,
  });
});
