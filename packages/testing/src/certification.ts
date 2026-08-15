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
  ToolCallId,
  TurnId,
  type AgentId,
  type SubmissionId,
} from "@effect-agent/core";
import { DurableStep, DurableStepError } from "@effect-agent/engine";
import {
  AgentBindingResolver,
  ApprovalDecisionCommand,
  CertificationCaseResult,
  CertificationReport,
  CertificationSweepResult,
  CertificationTierThreeReport,
  CertifiedAdapterIdentity,
  ConversationExportRequest,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointLocation,
  DurableRuntimeFailpointTestControl,
  DurableWorkerBinding,
  IdempotencyKey,
  LoadCheckpointRequest,
  Principal,
  ProducerId,
  ResolutionSafeToRetry,
  SubmissionLedger,
  SubmissionLookupById,
  ToolReconciler,
  UnknownResolutionCommand,
  WakeScheduler,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  certifyPorts,
  childConversationIdFor,
  verifyConversationInvariants,
  type BatchId,
  type CertificationScenario,
  type DurableSubmitFailure,
  type DurableSubmitOptions,
  type Receipt,
  type ResolvedBinding,
  type SubmissionSnapshot,
} from "@effect-agent/session";
import {
  Cause,
  Clock,
  Crypto,
  DateTime,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

/**
 * P7 WP2 — `certifyDurableAdapters` (plan §1): the one certification entry point a durable
 * adapter pair runs to earn a Schema-encoded certificate.
 *
 * - **Tier 1 — port contract**: the two shared conformance case arrays, verbatim, through
 *   `certifyPorts` (TEST-004/STORE-010).
 * - **Tier 2 — coordinator protocol + failpoint convergence**: the durable coordinator is
 *   assembled over the CANDIDATE Layer pair with a scripted deterministic model; every
 *   `DurableRuntimeFailpointLocation` is armed one-shot across the six scenario shapes
 *   (plain / uncertain-tool / durable-steps / approval / join / delegation). After the injected
 *   fault the runner asserts the state stays CLASSIFIABLE (recovery + the public unblocking
 *   operations `resolveUnknown`/`resolveApproval` are the only levers used) and that the
 *   re-drive CONVERGES to `verifyConversationInvariants` with `requireAllSettled` — including a
 *   fully discharged digest-chain check, because the runner captures per-batch producer
 *   identity at append time.
 * - **Tier 3 — real loss lever**: recorded honestly. A durable adapter either supplies a
 *   `CertificationCrashLever` executed in this run, cites its committed real-loss evidence
 *   (process-kill / eviction suites), or the certificate says `not-exercised`. A non-durable
 *   reference adapter records `not-applicable`.
 *
 * The runner is adapter-neutral and platform-neutral: it imports nothing Node-only, so the
 * same entry point runs under `@effect/vitest` on Node and inside workerd (storage-cloudflare's
 * in-workerd runner). It must run under a TestClock (Tier 1 drives lease expiry through
 * virtual time), and requires only `Crypto.Crypto` from the environment.
 */

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/**
 * A real-loss lever supplied by an adapter that wants Tier 3 exercised IN this certification
 * run (kill/evict/reopen around a designated row subset). Rows report with
 * `suite: "real-loss"`. Failures must be captured per-row — the lever's error channel is
 * `never` so a certificate is always produced.
 */
export type CertificationCrashLever = Effect.Effect<ReadonlyArray<CertificationCaseResult>>;

export interface CertifyDurableAdaptersOptions<LedgerE = never, StoreE = never> {
  /** Adapter pair identity named by the certificate (durability is read from the ledger). */
  readonly adapter: {
    readonly name: string;
    readonly version?: string | undefined;
  };
  /**
   * The candidate Layer pair. When both ports must share one connection root (the ADR-0011
   * "same file" rule), pass the SAME combined Layer instance for both fields — Layer
   * memoization builds it once. A candidate may require `Crypto.Crypto` (the memory reference
   * does); the certification's own environment supplies nothing else.
   */
  readonly submissionLedger: Layer.Layer<SubmissionLedger, LedgerE, Crypto.Crypto>;
  readonly conversationStore: Layer.Layer<ConversationStore, StoreE, Crypto.Crypto>;
  /** Defaults to `WakeScheduler.layerNoop`; the runner re-drives lanes explicitly. */
  readonly wakeScheduler?: Layer.Layer<WakeScheduler> | undefined;
  /** Executes Tier 3 in this run; takes precedence over `tierThreeEvidence`. */
  readonly crashLever?: CertificationCrashLever | undefined;
  /** Repository-relative citations of committed real-loss suites (Tier 3 `recorded-evidence`). */
  readonly tierThreeEvidence?: ReadonlyArray<string> | undefined;
  /**
   * The candidate ledger's configured ownership lease (defaults to the D5 default, 30s).
   * Every Tier-2 re-drive round advances the TestClock past this lease before reclaiming:
   * a live lease may block ALL new claims (the SQL adapters do; the memory reference also
   * allows same-producer reclaim), so the adapter-NEUTRAL recovery lever after a mid-Attempt
   * fault is lease expiry — exactly the documented D5 liveness mechanism, driven through
   * virtual time.
   */
  readonly ownershipLeaseDuration?: Duration.Duration | undefined;
}

/** The six Tier-2 scenario shapes in sweep order. */
export const CERTIFICATION_SCENARIOS: ReadonlyArray<CertificationScenario> = [
  "plain",
  "uncertain-tool",
  "durable-steps",
  "approval",
  "join",
  "delegation",
];

/**
 * Coordinator failpoint locations that none of the six scenario shapes can reach, recorded
 * honestly instead of silently claimed: all three sit on operator/abort paths the shapes do
 * not take. They are pinned in-process by the P5/S2 suites
 * (`packages/testing/test/durable-tools.test.ts` "resolveUnknown is idempotent across the
 * intent failpoint", `durable-runtime.test.ts` abort rows,
 * `durable-subagents.test.ts` abort propagation) and by the process-kill/eviction crash
 * matrices. Runner tests assert the observed never-fired set equals EXACTLY this list, so a
 * protocol change that silently stops exercising a location fails the certification.
 */
export const TIER2_UNREACHED_LOCATIONS: ReadonlyArray<DurableRuntimeFailpointLocation> = [
  "abort:after-intent",
  // Compaction requires a `contextTokenLimit` policy plus prior-Run history
  // none of the six scenario shapes carries; pinned in-process by the
  // RUN-026 rows in `packages/testing/test/durable-runtime.test.ts`
  // (compaction failpoint idempotence across re-drive).
  "compaction:after-canonical-append",
  "resolve:after-intent",
  "subagent:after-child-abort-intent",
];

/** Locations of `tier2` rows whose armed fault never fired in ANY scenario, sorted. */
export const tier2NeverFiredLocations = (
  tier2: ReadonlyArray<CertificationSweepResult>,
): ReadonlyArray<DurableRuntimeFailpointLocation> => {
  const fired = new Set<DurableRuntimeFailpointLocation>();
  for (const row of tier2) {
    if (row.failpointFired) fired.add(row.location);
  }
  return DurableRuntimeFailpointLocation.literals.filter((location) => !fired.has(location)).sort();
};

// ---------------------------------------------------------------------------
// Deterministic fixtures (scripted prompt-shape models, agents, delegation)
// ---------------------------------------------------------------------------

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;
const CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.tools),
});
const PRINCIPAL = Schema.decodeSync(Principal)("principal-certification");
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallPart = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Stateless scripted model that decides by PROMPT SHAPE instead of call count: while the
 * prompt carries no committed tool result the model declares `toolParts` (when given),
 * otherwise it answers with the final text. Deciding on the canonical prompt keeps every cell
 * deterministic regardless of where the injected fault fell — a re-invoked Turn re-declares
 * the same batch and a resumed batch flows into the final answer, so every scenario always
 * exercises its tool path and always converges.
 */
const promptShapeModel = (
  name: string,
  finalText: string,
  toolParts?: ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => {
          const hasToolResult = request.prompt.content.some((message) => message.role === "tool");
          const parts =
            toolParts === undefined || hasToolResult ? finalParts(finalText) : toolParts;
          return Stream.fromIterable(parts);
        },
      }),
    ),
  );

const policy = AgentPolicy.make({
  maxTurns: 4,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

const QuestionInput = Schema.Struct({ question: Schema.String });
const AnswerOutput = Schema.Struct({ answer: Schema.String });

/** plain / join: no tools — the pure Turn/submission/join seams. */
const plainDefinition = Agent.define("certify-plain", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

/** uncertain-tool: unannotated → fail-closed `uncertain`, enters the prepared/settled protocol. */
const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
const bookToolkit = Toolkit.make(Book);
const uncertainDefinition = Agent.define("certify-uncertain", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Book it.",
  toolkit: bookToolkit,
  policy,
});

/** durable-steps: declaring `DurableStep` as a dependency is what makes the Tool durable. */
const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});
const itineraryToolkit = Toolkit.make(Itinerary);
const stepsDefinition = Agent.define("certify-steps", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Reserve the itinerary.",
  toolkit: itineraryToolkit,
  policy,
});

/** approval: fail-closed — no `DurableApprovalResolver` Layer, so undecided approvals suspend. */
const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
const approvalToolkit = Toolkit.make(BookApproval);
const approvalDefinition = Agent.define("certify-approval", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Book after approval.",
  toolkit: approvalToolkit,
  policy,
});

/** delegation: durable attached child plus an ordinary uncertain sibling in ONE batch. */
const childDefinition = Agent.define("certify-child", {
  input: QuestionInput,
  output: AnswerOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

class CertifyDelegationFailed extends Schema.TaggedError<CertifyDelegationFailed>()(
  "CertifyDelegationFailed",
  { childErrorTag: Schema.String },
) {}

const researchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: childDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: CertifyDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "10 seconds",
  }),
});

const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});

const coordinatorDefinition = Agent.define("certify-coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate and look up, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool, Lookup),
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const mapChildFailure = (failure: { readonly _tag: string }) =>
  CertifyDelegationFailed.make({ childErrorTag: failure._tag });

const DELEGATE_CALL = decodeToolCallId("delegate-1");

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const identifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);
    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );
    return {
      nextConversationId: next(decodeConversationId, "certify-fixture-conversation"),
      nextRunId: next(Schema.decodeSync(RunId), "certify-fixture-run"),
      nextTurnId: next(Schema.decodeSync(TurnId), "certify-fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, identifiers);

// ---------------------------------------------------------------------------
// Tier 2 — scenario cells
// ---------------------------------------------------------------------------

interface CertificationCell {
  readonly resolver: (typeof AgentBindingResolver)["Service"];
  /** Idempotent submission batch — safe to replay verbatim after a submit-boundary fault. */
  readonly submit: Effect.Effect<ReadonlyArray<Receipt>, DurableSubmitFailure, DurableAgentRuntime>;
  /** All lanes of the cell in drive order, computed from the (possibly replayed) receipts. */
  readonly lanes: (receipts: ReadonlyArray<Receipt>) => ReadonlyArray<ConversationId>;
}

const submitOptionsFor = (slug: string, conversationId: ConversationId): DurableSubmitOptions => ({
  conversationId,
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(`certify-key-${slug}`),
  definitions: DIGESTS,
});

/** One single-agent cell: one lane, one Submission, one registered exact-digest binding. */
const makeSingleAgentCell = (
  definition: { readonly id: AgentId; readonly input: typeof QuestionInput },
  resolved: ResolvedBinding,
  slug: string,
): CertificationCell => {
  const conversationId = decodeConversationId(`certify-${slug}`);
  const submit = Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const receipt = yield* runtime.submit(
      { definition: { id: definition.id, input: definition.input } },
      { question: `certify ${slug}` },
      submitOptionsFor(slug, conversationId),
    );
    return [receipt];
  });
  return {
    resolver: AgentBindingResolver.fromBindings([resolved]),
    submit,
    lanes: () => [conversationId],
  };
};

const makeCell = Effect.fn("Certification.makeCell")(function* (
  scenario: CertificationScenario,
  slug: string,
) {
  switch (scenario) {
    case "plain": {
      const binding = Agent.withModel(
        plainDefinition,
        promptShapeModel("certify-plain", '{"answer":"done"}'),
      );
      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS);
      return makeSingleAgentCell(plainDefinition, resolved, slug);
    }
    case "uncertain-tool": {
      const binding = Agent.withModel(
        uncertainDefinition,
        promptShapeModel(
          "certify-uncertain",
          '{"answer":"booked"}',
          toolTurn(toolCallPart("book-1", "book", { ref: `r-${slug}` })),
        ),
      );
      const toolLayer = bookToolkit.toLayer({
        book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
      });
      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );
      return makeSingleAgentCell(uncertainDefinition, resolved, slug);
    }
    case "durable-steps": {
      const binding = Agent.withModel(
        stepsDefinition,
        promptShapeModel(
          "certify-steps",
          '{"answer":"reserved"}',
          toolTurn(toolCallPart("itinerary-1", "itinerary", { ref: `trip-${slug}` })),
        ),
      );
      const toolLayer = itineraryToolkit.toLayer({
        itinerary: ({ ref }) =>
          Effect.gen(function* () {
            const step = yield* DurableStep;
            const flight = yield* step.do(
              "reserve-flight",
              Schema.String,
              Effect.succeed(`flight-${ref}`),
            );
            const lodging = yield* step.do(
              "reserve-lodging",
              Schema.String,
              Effect.succeed(`lodging-${ref}`),
            );
            return { state: `${flight}+${lodging}` };
          }),
      });
      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );
      return makeSingleAgentCell(stepsDefinition, resolved, slug);
    }
    case "approval": {
      const binding = Agent.withModel(
        approvalDefinition,
        promptShapeModel(
          "certify-approval",
          '{"answer":"approved"}',
          toolTurn(toolCallPart("book-1", "book", { ref: `r-${slug}` })),
        ),
      );
      const toolLayer = approvalToolkit.toLayer({
        book: ({ ref }) => Effect.succeed({ confirmation: `confirmed-${ref}` }),
      });
      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS).pipe(
        Effect.provide(toolLayer),
      );
      return makeSingleAgentCell(approvalDefinition, resolved, slug);
    }
    case "join": {
      const binding = Agent.withModel(
        plainDefinition,
        promptShapeModel("certify-join", '{"answer":"host answer"}'),
      );
      const resolved = yield* DurableWorkerBinding.make(binding, DIGESTS);
      const conversationId = decodeConversationId(`certify-${slug}`);
      const submitOne = (key: string, question: string) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          return yield* runtime.submit(
            { definition: { id: plainDefinition.id, input: plainDefinition.input } },
            { question },
            {
              conversationId,
              principal: PRINCIPAL,
              idempotencyKey: decodeIdempotencyKey(key),
              definitions: DIGESTS,
            },
          );
        });
      const cell: CertificationCell = {
        resolver: AgentBindingResolver.fromBindings([resolved]),
        submit: Effect.gen(function* () {
          const host = yield* submitOne(`certify-key-${slug}-host`, "host question");
          const queued = yield* submitOne(`certify-key-${slug}-queued`, "queued question");
          return [host, queued];
        }),
        lanes: () => [conversationId],
      };
      return cell;
    }
    case "delegation": {
      const childBinding = Agent.withModel(
        childDefinition,
        promptShapeModel("certify-child", '{"answer":"child-answer"}'),
      );
      const parentBinding = Agent.withModel(
        coordinatorDefinition,
        promptShapeModel(
          "certify-parent",
          '{"report":"done"}',
          toolTurn(
            toolCallPart("delegate-1", "delegate_research", { topic: "paris" }),
            toolCallPart("lookup-1", "lookup", { key: "hotels" }),
          ),
        ),
      );
      const delegationLayer = SubagentRuntime.layer(researchDelegation, childBinding, {
        mapChildFailure,
        durable: { targetDigests: CHILD_DIGEST_STRINGS },
      }).pipe(Layer.provide(delegationSupport));
      const lookupLayer = Toolkit.make(Lookup).toLayer({
        lookup: ({ key }) => Effect.succeed({ value: `found-${key}` }),
      });
      const parentResolved = yield* DurableWorkerBinding.make(parentBinding, DIGESTS).pipe(
        Effect.provide(Layer.mergeAll(delegationLayer, lookupLayer)),
      );
      const childResolved = yield* DurableWorkerBinding.make(childBinding, CHILD_DIGESTS);
      const conversationId = decodeConversationId(`certify-${slug}`);
      const cell: CertificationCell = {
        resolver: AgentBindingResolver.fromBindings([parentResolved, childResolved]),
        submit: Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const receipt = yield* runtime.submit(
            { definition: { id: coordinatorDefinition.id, input: coordinatorDefinition.input } },
            { mission: "plan" },
            submitOptionsFor(slug, conversationId),
          );
          return [receipt];
        }),
        lanes: (receipts) => {
          const parent = receipts.at(0);
          return parent === undefined
            ? [conversationId]
            : [conversationId, childConversationIdFor(parent.submissionId, DELEGATE_CALL)];
        },
      };
      return cell;
    }
  }
});

/** Maximum recovery/drive/unblock rounds before a cell is reported non-convergent. */
const MAX_REDRIVE_ROUNDS = 8;

/**
 * Verify one lane after convergence: canonical export + every lane Submission the ledger or
 * the log names (the same collection rule as the admin `verify` member), fed to the shared
 * invariant checker in convergence mode WITH the captured per-batch producer directory, so
 * the digest chain is fully recomputed instead of skipped.
 */
const verifyLane = Effect.fn("Certification.verifyLane")(function* (
  lane: ConversationId,
  batchProducers: ReadonlyMap<BatchId, ProducerId>,
) {
  const store = yield* ConversationStore;
  const ledger = yield* SubmissionLedger;
  const exported = yield* store.export(ConversationExportRequest.make({ conversationId: lane }));
  const rows = new Map<SubmissionId, SubmissionSnapshot>();
  const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
  for (const submission of nonterminal) {
    if (submission.conversationId === lane) rows.set(submission.submissionId, submission);
  }
  const named = new Set<SubmissionId>();
  for (const envelope of exported.records) {
    const payload = envelope.record.payload;
    if (
      payload._tag === "UserInputRecorded" ||
      payload._tag === "SubmissionSettled" ||
      payload._tag === "AbortRequested"
    ) {
      named.add(payload.submissionId);
    }
  }
  for (const submissionId of named) {
    if (rows.has(submissionId)) continue;
    const found = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    if (Option.isSome(found) && found.value.conversationId === lane) {
      rows.set(submissionId, found.value);
    }
  }
  const checkpoint = yield* store.loadCheckpoint(
    LoadCheckpointRequest.make({ conversationId: lane }),
  );
  return yield* verifyConversationInvariants({
    export: exported,
    submissions: [...rows.values()],
    batchProducers,
    checkpoint: Option.getOrUndefined(checkpoint),
    requireAllSettled: true,
  });
});

const failureTagOf = <E>(cause: Cause.Cause<E>): string => {
  const failure = Cause.findErrorOption(cause);
  if (Option.isSome(failure)) {
    const error: unknown = failure.value;
    if (typeof error === "object" && error !== null && "_tag" in error) {
      return String((error as { _tag: unknown })._tag);
    }
    return String(error).slice(0, 256);
  }
  return "defect";
};

/** One Tier-2 sweep cell: arm `location` one-shot, drive `scenario`, converge, verify. */
const runSweepCell = Effect.fn("Certification.runSweepCell")(function* (
  scenario: CertificationScenario,
  location: DurableRuntimeFailpointLocation,
  batchProducers: ReadonlyMap<BatchId, ProducerId>,
  leaseAdvance: Duration.Duration,
) {
  const runtime = yield* DurableAgentRuntime;
  const ledger = yield* SubmissionLedger;
  const control = yield* DurableRuntimeFailpointTestControl;
  const slug = `${scenario}-${location.replaceAll(":", "-")}`;

  const failed = (detail: string, fired: boolean): CertificationSweepResult =>
    CertificationSweepResult.make({
      scenario,
      location,
      failpointFired: fired,
      status: "failed",
      digestChainVerified: false,
      detail: detail.slice(0, 4_096),
    });

  const cell = yield* makeCell(scenario, slug);

  // One-shot arm: the fault fires at most once anywhere in the cell (initial drive OR a
  // re-drive round's public unblocking operation), modelling one crash at this boundary.
  const fired = yield* Ref.make(false);
  yield* control.setHandler((hit) =>
    hit !== location
      ? Effect.void
      : Ref.getAndSet(fired, true).pipe(
          Effect.flatMap((already) =>
            already
              ? Effect.void
              : Effect.fail(DurableRuntimeFailpointError.make({ location: hit })),
          ),
        ),
  );

  // Submissions are idempotent (DUR-001): one replay recovers a submit-boundary fault.
  let receipts: ReadonlyArray<Receipt>;
  const firstSubmit = yield* Effect.exit(cell.submit);
  if (Exit.isSuccess(firstSubmit)) {
    receipts = firstSubmit.value;
  } else {
    const secondSubmit = yield* Effect.exit(cell.submit);
    if (Exit.isFailure(secondSubmit)) {
      yield* control.clear;
      return failed(
        `submission replay did not recover: ${failureTagOf(secondSubmit.cause)}`,
        yield* Ref.get(fired),
      );
    }
    receipts = secondSubmit.value;
  }
  const lanes = cell.lanes(receipts);

  const driveLane = (lane: ConversationId) =>
    runtime
      .processConversationResolved(lane)
      .pipe(Effect.provideService(AgentBindingResolver, cell.resolver));

  const allSettled = Effect.gen(function* () {
    for (const receipt of receipts) {
      const snapshot = yield* ledger.lookup(
        SubmissionLookupById.make({ submissionId: receipt.submissionId }),
      );
      if (Option.isNone(snapshot) || snapshot.value.state !== "settled") return false;
    }
    return true;
  });

  // Re-drive to convergence using ONLY public operations: worker drives, recovery passes,
  // and the authorized DUR-017/approval unblocking paths chosen from `explainConversation`.
  let converged = false;
  for (let round = 0; round < MAX_REDRIVE_ROUNDS && !converged; round++) {
    // Expire any lease a faulted Attempt left behind (D5): virtual time is the
    // adapter-neutral reclaim lever — a live lease may block every new claim.
    yield* TestClock.adjust(leaseAdvance);
    yield* Effect.exit(runtime.runRecovery);
    for (const lane of lanes) {
      yield* Effect.exit(driveLane(lane));
    }
    for (const lane of lanes) {
      const explains = yield* Effect.exit(runtime.explainConversation(lane));
      if (Exit.isFailure(explains)) continue;
      for (const explanation of explains.value) {
        for (const unknown of explanation.evidence.unknownCalls) {
          if (unknown.resolved) continue;
          yield* Effect.exit(
            runtime.resolveUnknown(
              UnknownResolutionCommand.make({
                submissionId: explanation.submission.submissionId,
                toolCallId: unknown.toolCallId,
                author: "certification-runner",
                reason: `re-drive after injected fault at ${location}`,
                resolution: ResolutionSafeToRetry.make(),
              }),
            ),
          );
        }
        for (const pending of explanation.evidence.approvalsPending) {
          const decided = explanation.evidence.approvalDecisions.some(
            (decision) => decision.toolCallId === pending.toolCallId,
          );
          if (decided) continue;
          yield* Effect.exit(
            runtime.resolveApproval(
              ApprovalDecisionCommand.make({
                submissionId: explanation.submission.submissionId,
                toolCallId: pending.toolCallId,
                decision: "approved",
                resolver: "certification-runner",
                reason: `re-drive after injected fault at ${location}`,
              }),
            ),
          );
        }
      }
    }
    const settled = yield* Effect.exit(allSettled);
    converged = Exit.isSuccess(settled) && settled.value;
  }
  yield* control.clear;
  const wasFired = yield* Ref.get(fired);

  if (!converged) {
    return failed(`did not converge within ${MAX_REDRIVE_ROUNDS} re-drive rounds`, wasFired);
  }

  // Every lane of the cell must verify in convergence mode with a recomputed digest chain.
  let digestChainVerified = true;
  const failedChecks: Array<string> = [];
  for (const lane of lanes) {
    const verdict = yield* Effect.exit(verifyLane(lane, batchProducers));
    if (Exit.isFailure(verdict)) {
      return failed(`lane ${lane} could not be verified: ${failureTagOf(verdict.cause)}`, wasFired);
    }
    for (const check of verdict.value.checks) {
      if (check.status === "failed") {
        failedChecks.push(
          `${lane}:${check.name}${check.detail === undefined ? "" : ` (${check.detail})`}`,
        );
      }
      if (check.name === "digest-chain" && check.status !== "passed") {
        digestChainVerified = false;
      }
    }
  }
  if (failedChecks.length > 0 || !digestChainVerified) {
    return failed(
      failedChecks.length > 0
        ? `invariant checks failed: ${failedChecks.join("; ")}`
        : "the digest chain was not fully recomputed",
      wasFired,
    );
  }

  return CertificationSweepResult.make({
    scenario,
    location,
    failpointFired: wasFired,
    status: wasFired ? "converged" : "not-triggered",
    digestChainVerified,
  });
});

// ---------------------------------------------------------------------------
// Tier 3 — real loss lever record
// ---------------------------------------------------------------------------

/**
 * Resolve the Tier-3 record honestly (plan §1): a non-durable reference adapter has no real
 * loss to exercise (`not-applicable`); a supplied lever runs NOW (`exercised`); committed
 * real-loss citations are recorded (`recorded-evidence`); otherwise the certificate says
 * `not-exercised` — a scoped statement, never a silent claim.
 */
export const resolveTierThree = Effect.fn("Certification.resolveTierThree")(function* (
  durability: CertifiedAdapterIdentity["durability"],
  options: {
    readonly crashLever?: CertificationCrashLever | undefined;
    readonly tierThreeEvidence?: ReadonlyArray<string> | undefined;
  },
): Effect.fn.Return<CertificationTierThreeReport, never> {
  if (durability === "non-durable") {
    return CertificationTierThreeReport.make({
      status: "not-applicable",
      evidence: [],
      cases: [],
      detail:
        "the adapter declares non-durable state (reference/conformance adapter); there is no real loss to exercise",
    });
  }
  if (options.crashLever !== undefined) {
    const cases = yield* options.crashLever;
    return CertificationTierThreeReport.make({
      status: "exercised",
      evidence: options.tierThreeEvidence ?? [],
      cases,
    });
  }
  if (options.tierThreeEvidence !== undefined && options.tierThreeEvidence.length > 0) {
    return CertificationTierThreeReport.make({
      status: "recorded-evidence",
      evidence: options.tierThreeEvidence,
      cases: [],
    });
  }
  return CertificationTierThreeReport.make({
    status: "not-exercised",
    evidence: [],
    cases: [],
    detail:
      "no crash lever was supplied and no committed real-loss evidence was cited; Tier 3 is NOT discharged for this adapter",
  });
});

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

const nowUtc: Effect.Effect<DateTime.Utc> = Effect.map(Clock.currentTimeMillis, (millis) =>
  DateTime.toUtc(DateTime.makeUnsafe(millis)),
);

/**
 * Certify one durable adapter pair (plan §1, §8 WP2). Runs Tier 2 FIRST over pristine
 * storage (each cell converges to all-settled before the next starts, so the recovery scan
 * never sees foreign leftovers), then Tier 1's port contract cases (whose lanes deliberately
 * end in every nonterminal shape), then records Tier 3. Requires `Crypto.Crypto` and a
 * TestClock-backed environment; the candidate Layers are built exactly once.
 */
export const certifyDurableAdapters = <LedgerE = never, StoreE = never>(
  options: CertifyDurableAdaptersOptions<LedgerE, StoreE>,
): Effect.Effect<CertificationReport, LedgerE | StoreE, Crypto.Crypto> => {
  const batchProducers = new Map<BatchId, ProducerId>();
  // Interpose the candidate store with an append-time capture of each batch's producer
  // identity — the one value the ConversationStore port deliberately does not export — so
  // Tier 2's invariant verification recomputes the FULL digest chain instead of skipping it.
  const capturingStore = Layer.effect(ConversationStore)(
    Effect.gen(function* () {
      const inner = yield* ConversationStore;
      return ConversationStore.of({
        ...inner,
        append: (request) =>
          Effect.sync(() => {
            batchProducers.set(request.batch.batchId, request.batch.producerId);
          }).pipe(Effect.andThen(inner.append(request))),
      });
    }),
  ).pipe(Layer.provide(options.conversationStore));

  const support = Layer.mergeAll(
    options.submissionLedger,
    capturingStore,
    options.wakeScheduler ?? WakeScheduler.layerNoop,
    DurableRuntimeFailpoint.layerTest,
    ToolReconciler.uncertain,
    DurableRuntimeConfig.layer({
      deploymentId: Schema.decodeSync(DeploymentId)("deployment-certification"),
      producerId: Schema.decodeSync(ProducerId)("producer-certification"),
      settlementPollInterval: Duration.millis(50),
      leaseRenewalInterval: Duration.seconds(5),
      abortPollInterval: Duration.millis(50),
    }),
  );
  const environment = DurableAgentRuntime.layer.pipe(Layer.provideMerge(support));

  const leaseAdvance = Duration.millis(
    Duration.toMillis(options.ownershipLeaseDuration ?? DEFAULT_OWNERSHIP_LEASE_DURATION) + 1_000,
  );

  const program = Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;

    // Tier 2 — coordinator failpoint convergence sweep.
    const tier2: Array<CertificationSweepResult> = [];
    for (const scenario of CERTIFICATION_SCENARIOS) {
      for (const location of DurableRuntimeFailpointLocation.literals) {
        tier2.push(yield* runSweepCell(scenario, location, batchProducers, leaseAdvance));
      }
    }

    // Tier 1 — the shared port contract suites, verbatim.
    const tier1 = yield* certifyPorts();

    // Tier 3 — the real loss lever record.
    const capabilities = yield* ledger.capabilities;
    const tier3 = yield* resolveTierThree(capabilities.durability, options);

    const generatedAt = yield* nowUtc;
    const ok =
      tier1.every((result) => result.status === "passed") &&
      tier2.every((result) => result.status !== "failed") &&
      tier3.cases.every((result) => result.status === "passed");

    return CertificationReport.make({
      format: "effect-agent/certification@1",
      adapter: CertifiedAdapterIdentity.make({
        name: options.adapter.name,
        ...(options.adapter.version === undefined ? {} : { version: options.adapter.version }),
        durability: capabilities.durability,
      }),
      generatedAt,
      tier1,
      tier2,
      tier3,
      ok,
    });
  });

  return program.pipe(Effect.provide(environment));
};
