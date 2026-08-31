import * as fs from "node:fs";

import {
  Subagent,
  SubagentPolicy,
  SubagentReservationsMemoryLive,
  SubagentRuntime,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentPolicy,
  ThreadId,
  IdGenerator,
  RunId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import { DurableStep, DurableStepError, ToolExecutionClass } from "@effect-agent/engine";
import {
  DefinitionDigests,
  Digest,
  DurableWorkerBinding,
  IdempotencyKey,
  Principal,
  Receipt,
  ReconciliationCompleted,
  ReconciliationNeverStarted,
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
  Settlement,
  ToolReconciler,
  type DurableSubmitOptions,
  type ResolvedBinding,
} from "@effect-agent/thread";
import { Duration, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

/**
 * Shared contract between the crash-harness test (`crash.test.ts`) and the child worker process
 * (`worker-entry.ts`). Both sides import the SAME agent definitions, identities, and stdout
 * message Schemas so a restarted "client" resubmits byte-identical canonical input.
 */

/** Environment variable names the harness uses to drive one child worker process. */
export const CrashEnv = {
  database: "EFFECT_AGENT_DB",
  scenario: "EFFECT_AGENT_SCENARIO",
  thread: "EFFECT_AGENT_THREAD",
  idempotencyKey: "EFFECT_AGENT_KEY",
  killAt: "EFFECT_AGENT_KILL_AT",
  killAtStorage: "EFFECT_AGENT_KILL_AT_STORAGE",
  /** Coordinator failpoint where the worker BLOCKS (marker file + hang) instead of dying. */
  blockAt: "EFFECT_AGENT_BLOCK_AT",
  leaseMillis: "EFFECT_AGENT_LEASE_MS",
  markerFile: "EFFECT_AGENT_MARKER_FILE",
  releaseFile: "EFFECT_AGENT_RELEASE_FILE",
  supplierDir: "EFFECT_AGENT_SUPPLIER_DIR",
  killRequiresSupplier: "EFFECT_AGENT_KILL_REQUIRES_SUPPLIER",
  decision: "EFFECT_AGENT_DECISION",
  /** S2: the scripted child model writes this marker and blocks mid-stream. */
  childBlockFile: "EFFECT_AGENT_CHILD_BLOCK_FILE",
  /** S2: unblocks the blocked child model, which then emits the STALE child answer. */
  childReleaseFile: "EFFECT_AGENT_CHILD_RELEASE_FILE",
  /** S2: `projectResult` writes this marker and blocks until the release file exists. */
  projectMarkerFile: "EFFECT_AGENT_PROJECT_MARKER_FILE",
  projectReleaseFile: "EFFECT_AGENT_PROJECT_RELEASE_FILE",
} as const;

/**
 * Child scenario scripts. Every scenario first assembles the full DN stack against the SQLite
 * file named by `EFFECT_AGENT_DB`:
 *
 * - `submit` — durably submit one Submission and print its Receipt.
 * - `abort-ready` — submit, then durably abort the still-unclaimed Submission.
 * - `run` — submit, then drain the lane to Settlement with a single-turn model.
 * - `run-two` — submit two FIFO Submissions, then drain the lane.
 * - `run-blocked` — submit, commit Turn 1 (a tool call), then block Turn 2's model stream until
 *   `EFFECT_AGENT_RELEASE_FILE` appears (writing `EFFECT_AGENT_MARKER_FILE` first).
 * - `abort-active` — submit, then block inside Turn 1's model stream forever (marker written),
 *   waiting to be aborted through the durable ledger.
 * - `run-uncertain` — submit, then drain with the UNANNOTATED (fail-closed `uncertain`) book
 *   Tool whose handler writes the file-backed supplier store; with `EFFECT_AGENT_MARKER_FILE`
 *   set the handler books, touches the marker, and blocks forever (SIGKILL mid-handler row).
 * - `run-idempotent` — same flow with the `idempotent`-annotated book Tool.
 * - `suspend-approval` — same flow with the approval-gated book Tool (`needsApproval: true`).
 * - `run-steps` — submit, then drain the Durable Tool `itinerary` (two named Steps, each
 *   recording its supplier call).
 * - `run-join` — submit a host and one queued Submission, then drain; with
 *   `EFFECT_AGENT_MARKER_FILE` set the model blocks forever after the join (marker written).
 * - `resolve-approval` — second-process driver: look the Submission up by key and record the
 *   durable `resolveApproval` intent named by `EFFECT_AGENT_DECISION`.
 * - `resolve-unknown` — second-process driver: record `resolveUnknown(NeverHappened)` for the
 *   book Tool Call through the shared ledger.
 *
 * S2 durable-Subagent scenarios (plan §4.4) — all resolve claimed heads through a
 * binding array built by `makeCrashSubagentBindings`:
 *
 * - `subagent-run` — submit the coordinator, then drive parent → child → parent lanes to the
 *   join; armed kill/block failpoints land inside establishment (drive 1), the child Settlement
 *   (drive 2), or the join/release (drive 3).
 * - `subagent-child` — drive ONLY the derived child Thread lane (a second worker process
 *   for the simultaneous-kill and independent-fencing rows); honors the blocked child model.
 * - `subagent-abort` — submit, drive the parent to `waitingForChild`, then durably abort it
 *   (killAt `abort:after-intent` dies right after the parent abort intent commits).
 * - `subagent-recover` — run ONE host startup-recovery pass over the shared file (killAt
 *   `subagent:after-child-abort-intent` dies right after the propagated child abort intent).
 */
export const CrashScenario = Schema.Literals([
  "submit",
  "abort-ready",
  "abort-queued",
  "run",
  "run-two",
  "run-blocked",
  "abort-active",
  "run-uncertain",
  "run-idempotent",
  "suspend-approval",
  "run-steps",
  "run-join",
  "resolve-approval",
  "resolve-unknown",
  "subagent-run",
  "subagent-child",
  "subagent-abort",
  "subagent-recover",
]);
export type CrashScenario = typeof CrashScenario.Type;

/** Exit code armed at a kill failpoint (`process.exit`, mirroring SIGKILL's 128+9). */
export const KILL_EXIT_CODE = 137;
/** Exit code for a child whose Attempt was fenced out by a newer producer epoch (expected). */
export const FENCED_EXIT_CODE = 87;

export const CRASH_DEPLOYMENT_ID = "deployment-crash";
export const CHILD_PRODUCER_ID = "producer-crash-child";
export const HOST_PRODUCER_ID = "producer-crash-host";
/** The canonical question; both sides must submit the exact same payload for digest replay. */
export const CRASH_QUESTION = "does accepted work survive a process kill?";
/** Distinct queued-input payload for join rows so prompt coverage is countable from records. */
export const JOIN_QUESTION = "please fold in the queued follow-up constraint";
export const CHILD_ANSWER = '{"answer":"child"}';
export const STALE_ANSWER = '{"answer":"stale"}';
export const FRESH_ANSWER = '{"answer":"fresh"}';

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
export const CRASH_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
export const CRASH_PRINCIPAL = Schema.decodeSync(Principal)("principal-crash");

export const decodeThreadId = Schema.decodeSync(ThreadId);
export const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
export const decodeToolCallId = Schema.decodeSync(ToolCallId);

export const crashSubmitOptions = (
  threadId: string,
  idempotencyKey: string,
): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
  principal: CRASH_PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: CRASH_DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

export const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

export const TOOL_CALL_ID = "search-1";

export const toolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: TOOL_CALL_ID,
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

export const BOOK_CALL_ID = "book-1";
export const BOOK_REF = "crash-booking";

export const bookToolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: BOOK_CALL_ID,
    name: "book",
    params: { ref: BOOK_REF },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

export const ITINERARY_CALL_ID = "itinerary-1";
export const STEP_REF = "crash-trip";

export const itineraryToolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: ITINERARY_CALL_ID,
    name: "itinerary",
    params: { ref: STEP_REF },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * File-backed external supplier store shared by the child worker processes and the harness
 * (plan §4.3 "supplier-marker files"). Every handler/Step invocation appends one line, so
 * external truth SURVIVES a SIGKILL exactly like a real supplier's ledger would: the harness
 * asserts per-call invocation counts honestly (at-least-once is observable, never hidden) and
 * the reconciler recovers completed outcomes from this store alone (durability §10).
 */
export interface SupplierRecord {
  readonly op: string;
  readonly key: string;
  readonly value: string;
}

const SupplierRecordSchema = Schema.Struct({
  op: Schema.String,
  key: Schema.String,
  value: Schema.String,
});
const decodeSupplierRecord = Schema.decodeUnknownOption(SupplierRecordSchema);

const supplierLogPath = (dir: string): string => `${dir}/supplier-log.jsonl`;

export const recordSupplierCall = (dir: string, op: string, key: string, value: string): void => {
  fs.mkdirSync(dir, { recursive: true });
  fs.appendFileSync(supplierLogPath(dir), `${JSON.stringify({ op, key, value })}\n`);
};

export const readSupplierRecords = (dir: string): ReadonlyArray<SupplierRecord> => {
  if (!fs.existsSync(supplierLogPath(dir))) return [];
  return fs
    .readFileSync(supplierLogPath(dir), "utf8")
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .flatMap((line) => {
      try {
        const parsed: unknown = JSON.parse(line);
        return Option.match(decodeSupplierRecord(parsed), {
          onNone: () => [],
          onSome: (record) => [record],
        });
      } catch {
        return [];
      }
    });
};

/** Exact per-operation invocation counts, keyed `{op}:{key}` — the honesty-claim currency. */
export const supplierCounts = (dir: string): Record<string, number> => {
  const counts: Record<string, number> = {};
  for (const record of readSupplierRecords(dir)) {
    const key = `${record.op}:${record.key}`;
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
};

export const supplierCount = (dir: string, op: string, key: string): number =>
  supplierCounts(dir)[`${op}:${key}`] ?? 0;

/** Every value the external supplier ever produced (the never-fabricate reference set). */
export const supplierValues = (dir: string): ReadonlySet<string> =>
  new Set(readSupplierRecords(dir).map((record) => record.value));

/** Scripted model whose per-call scripts are full streams (so a call can block on a file). */
export const makeScriptedStreamModel = Effect.fn("CrashFixtures.makeScriptedStreamModel")(
  function* (script: (call: number) => Stream.Stream<Response.StreamPartEncoded>) {
    const calls = yield* Ref.make(0);
    return Model.make(
      "scripted",
      "crash-harness",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(Effect.map((call) => script(call))),
            ),
        }),
      ),
    );
  },
);

/** Scripted model that emits a fixed part list per call. */
export const makeScriptedModel = (
  script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>,
) => makeScriptedStreamModel((call) => Stream.fromIterable(script(call)));

export const plannerDefinition = Agent.make("crash-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

// `readonly` is a deliberate P5 migration (plan §4.3): the crash matrix's search tool performs
// no external mutation, so annotating it keeps the P4 rows' canonical record shape byte-stable —
// an unannotated tool would fail closed to `uncertain` and gain `ToolCallPrepared` records.
const Search = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");
export const searchTools = Toolkit.make(Search);

export const searchDefinition = Agent.make("crash-search", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: searchTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

const bookPolicy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

/** UNANNOTATED booking tool: fails closed to `uncertain` and enters the prepared/settled protocol. */
const BookUncertain = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
export const bookTools = Toolkit.make(BookUncertain);
export const bookDefinition = Agent.make("crash-book", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it.",
  toolkit: bookTools,
  policy: bookPolicy,
});

/** The declared external idempotency contract: recovery may re-execute without proof. */
const BookIdempotent = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
}).annotate(ToolExecutionClass, "idempotent");
export const bookIdempotentTools = Toolkit.make(BookIdempotent);
export const bookIdempotentDefinition = Agent.make("crash-book-idempotent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it idempotently.",
  toolkit: bookIdempotentTools,
  policy: bookPolicy,
});

/** Approval-gated booking tool; unannotated → fail-closed `uncertain` execution class. */
const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
export const approvalTools = Toolkit.make(BookApproval);
export const approvalDefinition = Agent.make("crash-book-approval", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book after approval.",
  toolkit: approvalTools,
  policy: bookPolicy,
});

/** Durable Tool: declaring `DurableStep` as a dependency is what makes it durable. */
const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});
export const itineraryTools = Toolkit.make(Itinerary);
export const itineraryDefinition = Agent.make("crash-itinerary", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Reserve the itinerary.",
  toolkit: itineraryTools,
  policy: bookPolicy,
});

type BookToolkit = typeof bookTools | typeof bookIdempotentTools | typeof approvalTools;

/** Booking handler over the file-backed supplier store: one appended line per invocation. */
export const makeBookToolLayer = (dir: string, tools: BookToolkit) =>
  tools.toLayer({
    book: ({ ref }) =>
      Effect.sync(() => {
        const confirmation = `confirmed-${ref}`;
        recordSupplierCall(dir, "book", ref, confirmation);
        return { confirmation };
      }),
  });

/**
 * SIGKILL-mid-handler variant (plan §4.3): the handler performs the external booking, proves it
 * with the marker file, then never returns — the parent kills the process while the Tool Call is
 * prepared-but-unsettled with the external effect already done.
 */
export const makeBlockedBookToolLayer = (dir: string, tools: BookToolkit, markerFile: string) =>
  tools.toLayer({
    book: ({ ref }) =>
      Effect.sync(() => {
        recordSupplierCall(dir, "book", ref, `confirmed-${ref}`);
        fs.writeFileSync(markerFile, "1");
      }).pipe(Effect.andThen(Effect.never)),
  });

/** Durable-Step handler: entry and each Step invocation are observable supplier calls. */
export const makeItineraryToolLayer = (dir: string) =>
  itineraryTools.toLayer({
    itinerary: ({ ref }) =>
      Effect.gen(function* () {
        yield* Effect.sync(() => recordSupplierCall(dir, "itinerary-enter", ref, `enter-${ref}`));
        const step = yield* DurableStep;
        const flight = yield* step.do(
          "reserve-flight",
          Schema.String,
          Effect.sync(() => {
            const value = `flight-${ref}`;
            recordSupplierCall(dir, "reserve-flight", ref, value);
            return value;
          }),
        );
        const lodging = yield* step.do(
          "reserve-lodging",
          Schema.String,
          Effect.sync(() => {
            const value = `lodging-${ref}`;
            recordSupplierCall(dir, "reserve-lodging", ref, value);
            return value;
          }),
        );
        return { state: `${flight}+${lodging}` };
      }),
  });

const BookParameters = Schema.Struct({ ref: Schema.String });
const decodeBookParameters = Schema.decodeUnknownOption(BookParameters);

/**
 * Reconciliation policy backed by the supplier store (durability §10): a decision is a claim
 * about EXTERNAL truth read from the store the killed process actually mutated — a present
 * booking is recovered `CompletedWithResult`, an absent one is proof the handler `NeverStarted`,
 * the re-enterable Durable Tool is `SafeToRetry`, and anything else stays fail-closed Uncertain.
 */
export const supplierReconcilerLayer = (dir: string): Layer.Layer<ToolReconciler> =>
  Layer.succeed(ToolReconciler)({
    reconcile: (evidence) =>
      Effect.sync(() => {
        if (evidence.toolName === "book") {
          const params = decodeBookParameters(evidence.parameters);
          if (Option.isNone(params)) {
            return ReconciliationUncertain.make({ reason: "Unreadable book parameters" });
          }
          const booking = readSupplierRecords(dir).find(
            (record) => record.op === "book" && record.key === params.value.ref,
          );
          return booking !== undefined
            ? ReconciliationCompleted.make({
                result: { confirmation: booking.value },
                isFailure: false,
              })
            : ReconciliationNeverStarted.make();
        }
        if (evidence.toolName === "itinerary") return ReconciliationSafeToRetry.make();
        return ReconciliationUncertain.make({
          reason: `No supplier proof exists for ${evidence.toolName}`,
        });
      }),
  });

// ------------------------------------------------------------------------------------------
// S2 durable attached Subagents (plan §4.4): coordinator/researcher fixture shared by the
// harness and the worker processes. The child model's invocation count is FILE-BACKED through
// the same supplier store, so "a completed child is never re-executed" (SUB-018, §16.4) is
// asserted from external truth that survives SIGKILL — the P5 counter pattern.
// ------------------------------------------------------------------------------------------

export const DELEGATE_CALL_ID = "delegate-1";
export const RESEARCH_TOPIC = "coast";
/** Supplier-store operation recorded once per child LanguageModel invocation. */
export const CHILD_MODEL_OP = "child-model";
export const COORDINATOR_REPORT = '{"report":"done"}';
/** The bounded parent Tool result projected from the child answer `{"answer":"child"}`. */
export const PROJECTED_SUMMARY = "finding:child";

export const CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;
export const CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.tools),
});

/** Exact per-site child model invocation count (the never-re-executed currency). */
export const childModelInvocations = (dir: string): number =>
  supplierCount(dir, CHILD_MODEL_OP, RESEARCH_TOPIC);

const awaitFilePoll = (path: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!fs.existsSync(path)) {
      yield* Effect.sleep(Duration.millis(25));
    }
  });

export const researcherDefinition = Agent.make("crash-researcher", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Research the question and answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export class CrashDelegationFailed extends Schema.TaggedError<CrashDelegationFailed>()(
  "CrashDelegationFailed",
  { childErrorTag: Schema.String },
) {}

/** Gate that blocks `projectResult` between child verification and the join append. */
export interface ProjectionGate {
  readonly markerFile: string;
  readonly releaseFile: string;
}

/**
 * The delegation declaration. The projection gate (worker-only) blocks the resumed parent
 * Attempt EXACTLY between reading the settled child and appending the join batch, so the
 * stale-parent fencing row can replace the parent while its join is still pending.
 */
export const makeCrashDelegation = (projectionGate?: ProjectionGate) =>
  Subagent.define("delegate_research", {
    description: "Research one bounded question and return findings.",
    target: researcherDefinition,
    parameters: Schema.Struct({ topic: Schema.String }),
    success: Schema.Struct({ summary: Schema.String }),
    failure: CrashDelegationFailed,
    prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
    projectResult: (output) =>
      projectionGate === undefined
        ? Effect.succeed({ summary: `finding:${output.answer}` })
        : Effect.sync(() => {
            fs.writeFileSync(projectionGate.markerFile, "1");
          }).pipe(
            Effect.andThen(awaitFilePoll(projectionGate.releaseFile)),
            Effect.as({ summary: `finding:${output.answer}` }),
          ),
    policy: SubagentPolicy.make({
      maxChildren: 1,
      maxConcurrency: 1,
      maxTurns: 4,
      maxToolCalls: 4,
      maxDuration: "30 seconds",
    }),
  });

export const makeCrashCoordinator = (delegation: ReturnType<typeof makeCrashDelegation>) =>
  Agent.make("crash-coordinator", {
    input: Schema.Struct({ mission: Schema.String }),
    output: Schema.Struct({ report: Schema.String }),
    instructions: "Delegate the research, then report as JSON.",
    toolkit: Toolkit.make(delegation.tool),
    policy: AgentPolicy.make({
      maxTurns: 3,
      maxToolCalls: 2,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
    }),
  });

/**
 * Structural submit slice (identity + input schema): both sides submit the coordinator with
 * this exact identity so a restarted "client" reattaches to the same accepted Submission.
 */
export const coordinatorSubmitSlice = (() => {
  const definition = makeCrashCoordinator(makeCrashDelegation());
  return { definition: { id: definition.id, input: definition.input } } as const;
})();

export const delegationToolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: DELEGATE_CALL_ID,
    name: "delegate_research",
    params: { topic: RESEARCH_TOPIC },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/** Blocked child model behavior: write the marker, then hang (or resume STALE on release). */
export interface ChildModelBlock {
  readonly markerFile: string;
  /** When present the blocked stream resumes after this file exists and emits STALE_ANSWER. */
  readonly releaseFile?: string | undefined;
}

/**
 * Scripted child model with FILE-BACKED invocation counting: every `streamText` call appends
 * one `child-model:{topic}` supplier line before any part is emitted, so invocation counts
 * aggregate across processes and survive SIGKILL.
 */
export const makeCrashChildModel = (dir: string, block?: ChildModelBlock) =>
  Model.make(
    "scripted",
    "crash-child",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () =>
          Stream.unwrap(
            Effect.sync(() => {
              recordSupplierCall(dir, CHILD_MODEL_OP, RESEARCH_TOPIC, "invoked");
            }).pipe(
              Effect.map(() =>
                block === undefined
                  ? Stream.fromIterable(finalParts(CHILD_ANSWER))
                  : Stream.fromEffect(
                      Effect.sync(() => {
                        fs.writeFileSync(block.markerFile, "1");
                      }).pipe(
                        Effect.andThen(
                          block.releaseFile === undefined
                            ? Effect.never
                            : awaitFilePoll(block.releaseFile),
                        ),
                      ),
                    ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts(STALE_ANSWER)))),
              ),
            ),
          ),
      }),
    ),
  );

/**
 * Parent model scripts: a FRESH establishment declares the delegation call on its first
 * invocation; a post-crash restart drive must ALWAYS answer final, because the batch resume
 * replays the canonical declaration without re-invoking the model (P5 precedent).
 */
export type CrashParentScript = "delegate-then-final" | "final-only";

export const makeCrashParentModel = (script: CrashParentScript) =>
  makeScriptedModel((call) =>
    script === "delegate-then-final" && call === 0
      ? delegationToolCallParts
      : finalParts(COORDINATOR_REPORT),
  );

const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const crashIdentifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);
    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );
    return {
      nextThreadId: next(decodeThreadId, "crash-fixture-thread"),
      nextRunId: next(decodeRunId, "crash-fixture-run"),
      nextTurnId: next(decodeTurnId, "crash-fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, crashIdentifiers);

const mapCrashChildFailure = (failure: { readonly _tag: string }) =>
  CrashDelegationFailed.make({ childErrorTag: failure._tag });

export interface CrashSubagentOptions {
  readonly supplierDir: string;
  /** Defaults to `"final-only"` (the restart-drive script). */
  readonly parentScript?: CrashParentScript | undefined;
  readonly childBlock?: ChildModelBlock | undefined;
  readonly projectionGate?: ProjectionGate | undefined;
}

/**
 * The two resolvable worker Bindings of the S2 fixture, registered under EXACTLY the digest
 * strings the application submits with (`CRASH_DIGESTS`) and declares on the durable
 * delegation Layer (`CHILD_DIGEST_STRINGS`) — the spec §11 host obligation.
 */
export const makeCrashSubagentBindings = Effect.fn("CrashFixtures.makeCrashSubagentBindings")(
  function* (options: CrashSubagentOptions) {
    const delegation = makeCrashDelegation(options.projectionGate);
    const coordinatorDefinition = makeCrashCoordinator(delegation);
    const parentModel = yield* makeCrashParentModel(options.parentScript ?? "final-only");
    const parentBinding = Agent.withModel(coordinatorDefinition, parentModel);
    const childModel = makeCrashChildModel(options.supplierDir, options.childBlock);
    const childBinding = Agent.withModel(researcherDefinition, childModel);
    const delegationLayer = SubagentRuntime.layer(delegation, childBinding, {
      mapChildFailure: mapCrashChildFailure,
      durable: { targetDigests: CHILD_DIGEST_STRINGS },
    }).pipe(Layer.provide(delegationSupport));
    const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      parentBinding,
      CRASH_DIGESTS,
    ).pipe(Effect.provide(delegationLayer));
    const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      CHILD_DIGESTS,
    );
    return [parentResolved, childResolved] as const;
  },
);

/** Line-framed JSON messages the child prints on stdout for the harness to decode. */
export const ChildMessage = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("receipt"),
    key: Schema.String,
    receipt: Receipt,
  }),
  Schema.Struct({
    kind: Schema.Literal("settlements"),
    settlements: Schema.Array(Settlement),
  }),
  Schema.Struct({
    kind: Schema.Literal("worker-failure"),
    tag: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("resolved"),
    value: Schema.String,
  }),
]);
export type ChildMessage = typeof ChildMessage.Type;

export const encodeChildMessage = Schema.encodeSync(ChildMessage);
export const decodeChildMessageOption = Schema.decodeUnknownOption(ChildMessage);
