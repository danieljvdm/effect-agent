import { Agent, AgentPolicy, ConversationId } from "@effect-agent/core";
import type { SubmissionId } from "@effect-agent/core";
import type { AgentBindingResolver } from "@effect-agent/session";
import {
  AdmissionRequest,
  ClaimRequest,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  Digest,
  digestJson,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  ProducerId,
  SubmissionLedger,
  SubmissionLookupById,
  WakeScheduler,
  type DurableSubmitOptions,
  type PersistedJson,
  type SubmissionState,
} from "@effect-agent/session";
import {
  CurrentSqliteStorageVersion,
  SqliteStorageCompatibilityError,
  type SqliteStorageInitializationError,
} from "@effect-agent/storage-sqlite";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  Cause,
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  NodeDurableHost,
  NodeDurableRuntime,
  NodeDurableRuntimeConfig,
  type NodeDurableRuntimeInitializationError,
  type NodeDurableRuntimeOptions,
  type NodeDurableRuntimeServices,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const runtimeLayerProbe = NodeDurableRuntime.layer({
  filename: "unused.sqlite",
  deploymentId: "deployment-proof",
  producerId: "producer-proof",
});
type RuntimeLayerServicesProof = Assert<
  Equal<Layer.Success<typeof runtimeLayerProbe>, NodeDurableRuntimeServices>
>;
type RuntimeLayerErrorProof = Assert<
  Equal<Layer.Error<typeof runtimeLayerProbe>, NodeDurableRuntimeInitializationError>
>;
type RuntimeLayerRequirementsProof = Assert<Equal<Layer.Services<typeof runtimeLayerProbe>, never>>;
type RuntimeInitializationErrorProof = Assert<
  Equal<
    NodeDurableRuntimeInitializationError,
    | SqliteStorageInitializationError
    | Extract<NodeDurableRuntimeInitializationError, { readonly _tag: "NodePlatformConfigError" }>
  >
>;
type HostLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof NodeDurableHost.layer>,
    DurableAgentRuntime | NodeDurableRuntimeConfig | AgentBindingResolver
  >
>;

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const PRINCIPAL = Schema.decodeSync(Principal)("principal-platform-node");
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodeAgentId = Schema.decodeSync(AdmissionRequest.fields.agentId);
const decodeDeploymentId = Schema.decodeSync(AdmissionRequest.fields.deploymentId);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: "deployment-platform-node",
  producerId: "producer-platform-node",
  wakeScanInterval: 1_000,
  ...overrides,
});

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const makeScriptedModel = Effect.fn("PlatformNodeTest.makeScriptedModel")(function* (
  script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>,
) {
  const calls = yield* Ref.make(0);
  return Model.make(
    "scripted",
    "platform-node-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () =>
          Stream.unwrap(
            Ref.getAndUpdate(calls, (call) => call + 1).pipe(
              Effect.map((call) => Stream.fromIterable(script(call))),
            ),
          ),
      }),
    ),
  );
});

const plannerDefinition = Agent.define("platform-node-planner", {
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

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-platform-node-",
      });
      return yield* use(`${directory}/host.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

/** One host "process": the full DN stack over `filename`, closed (and drained) when `effect` ends. */
const withHost = <A, E, R>(
  options: NodeDurableRuntimeOptions,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | Layer.Error<typeof NodeDurableHost.layer> | NodeDurableRuntimeInitializationError,
  Exclude<R, NodeDurableHost | NodeDurableRuntimeServices>
> => Effect.provide(effect, NodeDurableHost.layerStack(options));

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  return Cause.squash(exit.cause);
};

const lookupState = (
  submissionId: SubmissionId,
): Effect.Effect<SubmissionState, never, SubmissionLedger> =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger
      .lookup(SubmissionLookupById.make({ submissionId }))
      .pipe(Effect.orDie);
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

const readLogTags = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    const records = yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
    return records.map((envelope) => envelope.record.payload._tag);
  });

describe("NodeDurableRuntime", () => {
  it("keeps the assembled Layer contract visible in its types", () => {
    const servicesProof: RuntimeLayerServicesProof = true;
    const errorProof: RuntimeLayerErrorProof = true;
    const requirementsProof: RuntimeLayerRequirementsProof = true;
    const initializationProof: RuntimeInitializationErrorProof = true;
    const hostProof: HostLayerRequirementsProof = true;

    expect(servicesProof).toBe(true);
    expect(errorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    expect(initializationProof).toBe(true);
    expect(hostProof).toBe(true);
  });

  it.effect("refuses out-of-bounds configuration with a typed error", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const opened = yield* Effect.service(NodeDurableRuntimeConfig).pipe(
          Effect.provide(
            NodeDurableRuntime.layer(runtimeOptions(filename, { workerConcurrency: 0 })),
          ),
          Effect.exit,
        );
        const error = failureOf(opened);
        expect(error).toHaveProperty("_tag", "NodePlatformConfigError");
        const databaseExists = yield* FileSystem.FileSystem.use((fs) => fs.exists(filename)).pipe(
          Effect.provide(NodeFileSystem.layer),
        );
        expect(databaseExists).toBe(false);
      }),
    ),
  );

  it.effect("startup refuses an incompatible v1 storage file without mutating it", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql.unsafe("PRAGMA user_version = 1");
          }),
        );

        const opened = yield* Effect.service(NodeDurableHost).pipe(
          Effect.provide(NodeDurableHost.layerStack(runtimeOptions(filename))),
          Effect.exit,
        );
        const error = failureOf(opened);
        expect(error).toBeInstanceOf(SqliteStorageCompatibilityError);
        if (error instanceof SqliteStorageCompatibilityError) {
          expect(error.actualVersion).toBe(1);
          expect(error.supportedVersion).toBe(CurrentSqliteStorageVersion);
          expect(error.message).toContain("Reset the database file explicitly");
        }

        // Failing closed must not mutate the incompatible file.
        const tables = yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT name
              FROM sqlite_master
              WHERE type = 'table'
                AND name LIKE 'effect_agent_%'
            `;
          }),
        );
        expect(tables).toEqual([]);
      }),
    ),
  );

  it.effect("startup reconciliation settles an orphaned reserved settlement before admission", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const conversation = decodeConversationId("conversation-reconcile");

        // Host process 1: the Attempt crashes (typed failpoint) AFTER reserving the exact
        // settlement record but BEFORE appending it canonically (durability §12 step 1→2 gap).
        const receipt = yield* withHost(
          runtimeOptions(filename, {
            runtimeFailpoint: (location) =>
              location === "terminalize:after-reserve"
                ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                : Effect.void,
          }),
          Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            const runtime = yield* DurableAgentRuntime;
            const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
            const agent = Agent.withModel(plannerDefinition, model);

            const receipt = yield* host.submit(
              agent,
              { question: "reconcile?" },
              submitOptions("conversation-reconcile", "reconcile-1"),
            );
            const crashed = yield* Effect.exit(runtime.processConversation(agent, conversation));
            const error = failureOf(crashed);
            expect(error).toHaveProperty("_tag", "DurableRuntimeFailpointError");
            expect(yield* lookupState(receipt.submissionId)).toBe("terminalizing");
            return receipt;
          }),
        );

        // Host process 2: startup recovery appends the EXACT reserved record and finalizes the
        // ledger BEFORE admission opens; the same idempotency key then resumes to the original
        // Receipt and its recorded Settlement.
        yield* withHost(
          runtimeOptions(filename),
          Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            expect(yield* host.admissionOpen).toBe(true);
            const report = host.startupRecovery.find(
              (candidate) => candidate.submissionId === receipt.submissionId,
            );
            expect(report?.decision._tag).toBe("AppendReservedSettlement");
            expect(report?.disposition).toBe("repaired");
            expect(yield* lookupState(receipt.submissionId)).toBe("settled");

            const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
            const agent = Agent.withModel(plannerDefinition, model);
            const replayed = yield* host.submit(
              agent,
              { question: "reconcile?" },
              submitOptions("conversation-reconcile", "reconcile-1"),
            );
            expect(replayed).toEqual(receipt);

            const settlement = yield* host.awaitSettlement(receipt);
            expect(settlement.outcome).toBe("completed");
            expect(settlement.receiptId).toBe(receipt.receiptId);

            expect(yield* readLogTags(conversation)).toEqual([
              "ConversationCreated",
              "UserInputRecorded",
              "ModelResponseRecorded",
              "SubmissionSettled",
              "RepairAnnotated",
            ]);
          }),
        );
      }),
    ),
  );

  it.effect("shutdown closes admission and releases ownership for the next host", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const conversation = decodeConversationId("conversation-shutdown");

        // Host process 1, with an explicit Scope so shutdown ordering is observable.
        const scope = yield* Scope.make();
        const context = yield* Layer.build(
          NodeDurableHost.layerStack(runtimeOptions(filename)),
        ).pipe(Scope.provide(scope));
        const host = Context.get(context, NodeDurableHost);
        const ledger = Context.get(context, SubmissionLedger);

        const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
        const agent = Agent.withModel(plannerDefinition, model);
        const receipt = yield* host.submit(
          agent,
          { question: "shutdown?" },
          submitOptions("conversation-shutdown", "shutdown-1"),
        );
        expect(yield* host.admissionOpen).toBe(true);

        // Hold an ownership lease (default 30s; the TestClock never advances past it).
        const claimed = yield* ledger.claim(
          ClaimRequest.make({
            conversationId: conversation,
            producerId: decodeProducerId("producer-platform-node"),
          }),
        );
        expect(Option.isSome(claimed)).toBe(true);

        yield* Scope.close(scope, Exit.void);

        // Shutdown step 1: admission is closed before anything else.
        expect(yield* host.admissionOpen).toBe(false);
        const refused = yield* Effect.exit(
          host.submit(
            agent,
            { question: "late" },
            submitOptions("conversation-shutdown", "late-1"),
          ),
        );
        expect(failureOf(refused)).toHaveProperty("_tag", "AdmissionClosed");

        // Host process 2 claims the lane IMMEDIATELY: the drain released the lease instead of
        // leaving the next owner to wait out its expiry.
        yield* withHost(
          runtimeOptions(filename),
          Effect.gen(function* () {
            const nextHost = yield* NodeDurableHost;
            const nextLedger = yield* SubmissionLedger;
            const report = nextHost.startupRecovery.find(
              (candidate) => candidate.submissionId === receipt.submissionId,
            );
            expect(report?.decision._tag).toBe("ApplyInput");
            expect(report?.disposition).toBe("repaired");

            const reclaimed = yield* nextLedger.claim(
              ClaimRequest.make({
                conversationId: conversation,
                producerId: decodeProducerId("producer-platform-node-2"),
              }),
            );
            expect(Option.isSome(reclaimed)).toBe(true);
            if (Option.isSome(claimed) && Option.isSome(reclaimed)) {
              expect(reclaimed.value.submissionId).toBe(receipt.submissionId);
              expect(reclaimed.value.producerEpoch).toBeGreaterThan(claimed.value.producerEpoch);
            }
          }),
        );
      }),
    ),
  );

  it.effect("wake-scan fallback claims ready work without any notify", () =>
    withTemporaryDatabase((filename) =>
      withHost(
        runtimeOptions(filename, { wakeScanInterval: 1_000 }),
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const wake = yield* WakeScheduler;
          const runtime = yield* DurableAgentRuntime;
          const conversation = decodeConversationId("conversation-wake");

          // Seed accepted work through the ledger alone: no `notify` is ever sent, exactly like
          // an admission from another process that this worker never heard about.
          const input: PersistedJson = { question: "wake?" };
          const inputDigest = yield* digestJson(input).pipe(Effect.provide(NodeCrypto.layer));
          const admitted = yield* ledger.admit(
            AdmissionRequest.make({
              conversationId: conversation,
              principal: PRINCIPAL,
              idempotencyKey: decodeIdempotencyKey("wake-1"),
              agentId: decodeAgentId("platform-node-planner"),
              agentDigests: DIGESTS,
              deploymentId: decodeDeploymentId("deployment-platform-node"),
              inputPayload: input,
              inputDigest,
            }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

          const woken = yield* Effect.forkChild(Stream.runCollect(Stream.take(wake.wakes, 1)));
          yield* TestClock.adjust(Duration.millis(1_000));
          expect(yield* Fiber.join(woken)).toEqual([conversation]);

          const model = yield* makeScriptedModel(() => finalParts('{"answer":"woken"}'));
          const agent = Agent.withModel(plannerDefinition, model);
          const settlements = yield* runtime.processConversation(agent, conversation);
          expect(settlements).toHaveLength(1);
          expect(settlements[0]?.outcome).toBe("completed");
          expect(yield* lookupState(admitted.submissionId)).toBe("settled");
        }),
      ),
    ),
  );

  it.effect("derives every operational cadence from the validated configuration", () =>
    withTemporaryDatabase((filename) =>
      withHost(
        runtimeOptions(filename, {
          settlementPollInterval: 111,
          leaseRenewalInterval: 2_222,
          abortPollInterval: 333,
          workerConcurrency: 3,
        }),
        Effect.gen(function* () {
          const nodeConfig = yield* NodeDurableRuntimeConfig;
          const sessionConfig = yield* DurableRuntimeConfig;
          expect(nodeConfig.workerConcurrency).toBe(3);
          expect(nodeConfig.filename).toBe(filename);
          expect(sessionConfig.deploymentId).toBe("deployment-platform-node");
          expect(sessionConfig.producerId).toBe("producer-platform-node");
          expect(Duration.toMillis(sessionConfig.settlementPollInterval)).toBe(111);
          expect(Duration.toMillis(sessionConfig.leaseRenewalInterval)).toBe(2_222);
          expect(Duration.toMillis(sessionConfig.abortPollInterval)).toBe(333);

          // runWorkers drives exactly `workerConcurrency` copies of the worker effect.
          const started = yield* Ref.make(0);
          const host = yield* NodeDurableHost;
          yield* host.runWorkers(Ref.update(started, (count) => count + 1));
          expect(yield* Ref.get(started)).toBe(3);
        }),
      ),
    ),
  );
});
