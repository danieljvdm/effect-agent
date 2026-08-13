import * as fs from "node:fs";

import { Agent, type ConversationId } from "@effect-agent/core";
import {
  AbortCommand,
  AgentBindingResolver,
  ApprovalDecisionCommand,
  DurableAgentRuntime,
  DurableRuntimeFailpointLocation,
  ResolutionNeverHappened,
  SubmissionLedger,
  SubmissionLookupByKey,
  UnknownResolutionCommand,
  childConversationIdFor,
  type DurableRuntimeFailpointHandler,
  type Receipt,
  type Settlement,
  type SubmissionSnapshot,
} from "@effect-agent/session";
import {
  SqliteStorageFailpointLocation,
  type SqliteStorageFailpointHandler,
} from "@effect-agent/storage-sqlite";
import { Cause, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import type { Response } from "effect/unstable/ai";

import {
  NodeDurableHost,
  NodeDurableRuntime,
  type NodeDurableRuntimeOptions,
} from "../../src/index.ts";
import {
  CHILD_ANSWER,
  CHILD_PRODUCER_ID,
  CRASH_DEPLOYMENT_ID,
  CRASH_QUESTION,
  BOOK_CALL_ID,
  CrashScenario,
  DELEGATE_CALL_ID,
  JOIN_QUESTION,
  KILL_EXIT_CODE,
  FENCED_EXIT_CODE,
  STALE_ANSWER,
  bookDefinition,
  bookIdempotentDefinition,
  bookIdempotentTools,
  bookToolCallParts,
  bookTools,
  approvalDefinition,
  approvalTools,
  coordinatorSubmitSlice,
  crashSubmitOptions,
  decodeConversationId,
  decodeToolCallId,
  encodeChildMessage,
  finalParts,
  itineraryDefinition,
  itineraryToolCallParts,
  makeBlockedBookToolLayer,
  makeBookToolLayer,
  makeCrashSubagentBindings,
  makeItineraryToolLayer,
  makeScriptedModel,
  makeScriptedStreamModel,
  plannerDefinition,
  searchDefinition,
  searchToolLayer,
  supplierCount,
  toolCallParts,
  type ChildMessage,
} from "./fixtures.ts";

/**
 * Crash-harness child entrypoint (plan §Crash matrix). One process = one host "lifetime": it
 * assembles the real DN stack over the SQLite file named by `EFFECT_AGENT_DB`, arms the
 * requested failpoint to HARD-KILL the process (`process.exit(137)` — no finalizers, no drain,
 * exactly like a crash), and executes one scenario step. The spawning test then restarts against
 * the same file and asserts the crash-matrix row's required durable outcome.
 */

const MillisFromString = Schema.FiniteFromString.check(
  Schema.isInt(),
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(600_000),
);

const WorkerEnv = Schema.Struct({
  EFFECT_AGENT_DB: Schema.NonEmptyString,
  EFFECT_AGENT_SCENARIO: CrashScenario,
  EFFECT_AGENT_CONVERSATION: Schema.NonEmptyString,
  EFFECT_AGENT_KEY: Schema.NonEmptyString,
  EFFECT_AGENT_KILL_AT: Schema.optionalKey(DurableRuntimeFailpointLocation),
  EFFECT_AGENT_KILL_AT_STORAGE: Schema.optionalKey(SqliteStorageFailpointLocation),
  EFFECT_AGENT_BLOCK_AT: Schema.optionalKey(DurableRuntimeFailpointLocation),
  EFFECT_AGENT_LEASE_MS: Schema.optionalKey(MillisFromString),
  EFFECT_AGENT_MARKER_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_RELEASE_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_SUPPLIER_DIR: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_KILL_REQUIRES_SUPPLIER: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_DECISION: Schema.optionalKey(Schema.Literals(["approved", "denied"])),
  EFFECT_AGENT_CHILD_BLOCK_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_CHILD_RELEASE_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_PROJECT_MARKER_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_PROJECT_RELEASE_FILE: Schema.optionalKey(Schema.NonEmptyString),
});

const env = Schema.decodeUnknownSync(WorkerEnv)(process.env);
const conversationId = decodeConversationId(env.EFFECT_AGENT_CONVERSATION);
const idempotencyKey = env.EFFECT_AGENT_KEY;

/**
 * Optional supplier-store gate on an armed kill (`{op}:{key}`): the kill fires only once the
 * external effect is already in the store. Arming `append:before` with the book gate kills at
 * the FIRST canonical append AFTER the handler returned — the response/prepared appends precede
 * the handler, so the gated hit is exactly the Turn's results append (plan §4.3 "after handler
 * return, before turn:after-results-append").
 */
const killGateSatisfied = (): boolean => {
  const gate = env.EFFECT_AGENT_KILL_REQUIRES_SUPPLIER;
  if (gate === undefined) return true;
  const dir = env.EFFECT_AGENT_SUPPLIER_DIR;
  if (dir === undefined) throw new Error("A supplier kill gate requires EFFECT_AGENT_SUPPLIER_DIR");
  const separator = gate.indexOf(":");
  const op = gate.slice(0, separator);
  const key = gate.slice(separator + 1);
  return supplierCount(dir, op, key) >= 1;
};

/** A kill failpoint is a crash, not a failure: exit immediately, skipping every finalizer. */
const hardKill: Effect.Effect<never> = Effect.sync(() => process.exit(KILL_EXIT_CODE));

const killIfGated: Effect.Effect<void> = Effect.suspend(() =>
  killGateSatisfied() ? hardKill : Effect.void,
);

/**
 * A blocking failpoint (S2 simultaneous-kill rows): the worker proves it reached the location
 * with the marker file, then hangs mid-Attempt — still holding its ownership lease — until the
 * harness SIGKILLs it, exactly like a process that stalled at that durable step.
 */
const blockAndHang: Effect.Effect<never> = Effect.suspend(() => {
  const marker = env.EFFECT_AGENT_MARKER_FILE;
  if (marker === undefined)
    throw new Error("EFFECT_AGENT_BLOCK_AT requires EFFECT_AGENT_MARKER_FILE");
  fs.writeFileSync(marker, "1");
  return Effect.never;
});

const runtimeKill: DurableRuntimeFailpointHandler | undefined =
  env.EFFECT_AGENT_KILL_AT === undefined && env.EFFECT_AGENT_BLOCK_AT === undefined
    ? undefined
    : (location) => {
        if (env.EFFECT_AGENT_KILL_AT !== undefined && location === env.EFFECT_AGENT_KILL_AT) {
          return killIfGated;
        }
        if (env.EFFECT_AGENT_BLOCK_AT !== undefined && location === env.EFFECT_AGENT_BLOCK_AT) {
          return blockAndHang;
        }
        return Effect.void;
      };

const storageKill: SqliteStorageFailpointHandler | undefined =
  env.EFFECT_AGENT_KILL_AT_STORAGE === undefined
    ? undefined
    : (location) => (location === env.EFFECT_AGENT_KILL_AT_STORAGE ? killIfGated : Effect.void);

const options: NodeDurableRuntimeOptions = {
  filename: env.EFFECT_AGENT_DB,
  deploymentId: CRASH_DEPLOYMENT_ID,
  producerId: CHILD_PRODUCER_ID,
  ownershipLeaseDuration: env.EFFECT_AGENT_LEASE_MS ?? 30_000,
  // Long enough that a deliberately expired short lease is never renewed mid-scenario.
  leaseRenewalInterval: 60_000,
  abortPollInterval: 50,
  settlementPollInterval: 50,
  wakeScanInterval: 1_000,
  runtimeFailpoint: runtimeKill,
  storageFailpoint: storageKill,
};

const emit = (message: ChildMessage): Effect.Effect<void> =>
  Effect.sync(() => {
    process.stdout.write(`${JSON.stringify(encodeChildMessage(message))}\n`);
  });

const touch = (path: string): Effect.Effect<void> =>
  Effect.sync(() => {
    fs.writeFileSync(path, "1");
  });

const awaitFile = (path: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!fs.existsSync(path)) {
      yield* Effect.sleep(Duration.millis(25));
    }
  });

const requireEnv = (value: string | undefined, name: string): string => {
  if (value === undefined) throw new Error(`Scenario requires ${name}.`);
  return value;
};

/** Turn 1 declares a tool call; Turn 2 blocks on the release file after writing the marker. */
const blockedSearchScript = (
  markerFile: string,
  releaseFile: string,
): ((call: number) => Stream.Stream<Response.StreamPartEncoded>) => {
  return (call) =>
    call === 0
      ? Stream.fromIterable(toolCallParts)
      : Stream.fromEffect(touch(markerFile).pipe(Effect.andThen(awaitFile(releaseFile)))).pipe(
          Stream.flatMap(() => Stream.fromIterable(finalParts(STALE_ANSWER))),
        );
};

/** Every call writes the marker, then blocks until interrupted (the abort path). */
const blockedForeverScript = (
  markerFile: string,
): ((call: number) => Stream.Stream<Response.StreamPartEncoded>) => {
  return () => Stream.fromEffect(touch(markerFile).pipe(Effect.andThen(Effect.never)));
};

const submitPlannerQuestion = (key: string, question: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
    const agent = Agent.withModel(plannerDefinition, model);
    const receipt: Receipt = yield* runtime.submit(
      agent,
      { question },
      crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, key),
    );
    yield* emit({ kind: "receipt", key, receipt });
    return receipt;
  });

const submitPlanner = (key: string) => submitPlannerQuestion(key, CRASH_QUESTION);

const emitSettlements = (settlements: ReadonlyArray<Settlement>) =>
  emit({ kind: "settlements", settlements });

/** Second-process drivers address the Submission by its durable identity, not shared memory. */
const lookupSubmission: Effect.Effect<SubmissionSnapshot, never, SubmissionLedger> = Effect.gen(
  function* () {
    const ledger = yield* SubmissionLedger;
    const options = crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey);
    const snapshot = yield* ledger
      .lookup(
        SubmissionLookupByKey.make({
          conversationId,
          principal: options.principal,
          idempotencyKey: options.idempotencyKey,
        }),
      )
      .pipe(Effect.orDie);
    if (Option.isNone(snapshot)) {
      return yield* Effect.die(new Error(`No accepted Submission exists for ${idempotencyKey}`));
    }
    return snapshot.value;
  },
);

const requireSupplierDir = (): string =>
  requireEnv(env.EFFECT_AGENT_SUPPLIER_DIR, "EFFECT_AGENT_SUPPLIER_DIR");

/**
 * S2 fixture assembly for one worker process (plan §4.4): the coordinator/researcher bindings
 * registered under their exact digests, with the optional blocked child model and projection
 * gate taken from the environment. The resolver serves `processConversationResolved`, so one
 * worker process can drive BOTH the parent and the derived child Conversation lane.
 */
const makeSubagentResolver = Effect.gen(function* () {
  const supplierDir = requireSupplierDir();
  const childBlock =
    env.EFFECT_AGENT_CHILD_BLOCK_FILE === undefined
      ? undefined
      : {
          markerFile: env.EFFECT_AGENT_CHILD_BLOCK_FILE,
          releaseFile: env.EFFECT_AGENT_CHILD_RELEASE_FILE,
        };
  const projectionGate =
    env.EFFECT_AGENT_PROJECT_MARKER_FILE === undefined
      ? undefined
      : {
          markerFile: env.EFFECT_AGENT_PROJECT_MARKER_FILE,
          releaseFile: requireEnv(
            env.EFFECT_AGENT_PROJECT_RELEASE_FILE,
            "EFFECT_AGENT_PROJECT_RELEASE_FILE",
          ),
        };
  const bindings = yield* makeCrashSubagentBindings({
    supplierDir,
    parentScript: "delegate-then-final",
    childBlock,
    projectionGate,
  });
  return AgentBindingResolver.fromBindings([...bindings]);
});

const driveResolved = (
  resolver: (typeof AgentBindingResolver)["Service"],
  conversation: ConversationId,
) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime
      .processConversationResolved(conversation)
      .pipe(Effect.provideService(AgentBindingResolver, resolver));
  });

const submitCoordinator = (key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const receipt: Receipt = yield* runtime.submit(
      coordinatorSubmitSlice,
      { mission: CRASH_QUESTION },
      crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, key),
    );
    yield* emit({ kind: "receipt", key, receipt });
    return receipt;
  });

const scenario = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  switch (env.EFFECT_AGENT_SCENARIO) {
    case "submit": {
      yield* submitPlanner(idempotencyKey);
      return;
    }
    case "abort-ready": {
      const receipt = yield* submitPlanner(idempotencyKey);
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "crash harness abort",
        }),
      );
      return;
    }
    case "run": {
      yield* submitPlanner(idempotencyKey);
      const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
      const agent = Agent.withModel(plannerDefinition, model);
      yield* emitSettlements(yield* runtime.processConversation(agent, conversationId));
      return;
    }
    case "run-two": {
      yield* submitPlanner(`${idempotencyKey}-1`);
      yield* submitPlanner(`${idempotencyKey}-2`);
      const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
      const agent = Agent.withModel(plannerDefinition, model);
      yield* emitSettlements(yield* runtime.processConversation(agent, conversationId));
      return;
    }
    case "run-blocked": {
      const markerFile = requireEnv(env.EFFECT_AGENT_MARKER_FILE, "EFFECT_AGENT_MARKER_FILE");
      const releaseFile = requireEnv(env.EFFECT_AGENT_RELEASE_FILE, "EFFECT_AGENT_RELEASE_FILE");
      const model = yield* makeScriptedStreamModel(blockedSearchScript(markerFile, releaseFile));
      const agent = Agent.withModel(searchDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      yield* emitSettlements(yield* runtime.processConversation(agent, conversationId));
      return;
    }
    case "abort-active": {
      const markerFile = requireEnv(env.EFFECT_AGENT_MARKER_FILE, "EFFECT_AGENT_MARKER_FILE");
      const model = yield* makeScriptedStreamModel(blockedForeverScript(markerFile));
      const agent = Agent.withModel(plannerDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      yield* emitSettlements(yield* runtime.processConversation(agent, conversationId));
      return;
    }
    case "run-uncertain": {
      const dir = requireSupplierDir();
      const model = yield* makeScriptedModel((call) =>
        call === 0 ? bookToolCallParts : finalParts(CHILD_ANSWER),
      );
      const agent = Agent.withModel(bookDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      const toolLayer =
        env.EFFECT_AGENT_MARKER_FILE === undefined
          ? makeBookToolLayer(dir, bookTools)
          : makeBlockedBookToolLayer(dir, bookTools, env.EFFECT_AGENT_MARKER_FILE);
      yield* emitSettlements(
        yield* runtime.processConversation(agent, conversationId).pipe(Effect.provide(toolLayer)),
      );
      return;
    }
    case "run-idempotent": {
      const dir = requireSupplierDir();
      const model = yield* makeScriptedModel((call) =>
        call === 0 ? bookToolCallParts : finalParts(CHILD_ANSWER),
      );
      const agent = Agent.withModel(bookIdempotentDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      yield* emitSettlements(
        yield* runtime
          .processConversation(agent, conversationId)
          .pipe(Effect.provide(makeBookToolLayer(dir, bookIdempotentTools))),
      );
      return;
    }
    case "suspend-approval": {
      const dir = requireSupplierDir();
      const model = yield* makeScriptedModel((call) =>
        call === 0 ? bookToolCallParts : finalParts(CHILD_ANSWER),
      );
      const agent = Agent.withModel(approvalDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      yield* emitSettlements(
        yield* runtime
          .processConversation(agent, conversationId)
          .pipe(Effect.provide(makeBookToolLayer(dir, approvalTools))),
      );
      return;
    }
    case "run-steps": {
      const dir = requireSupplierDir();
      const model = yield* makeScriptedModel((call) =>
        call === 0 ? itineraryToolCallParts : finalParts(CHILD_ANSWER),
      );
      const agent = Agent.withModel(itineraryDefinition, model);
      const receipt = yield* runtime.submit(
        agent,
        { question: CRASH_QUESTION },
        crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, idempotencyKey),
      );
      yield* emit({ kind: "receipt", key: idempotencyKey, receipt });
      yield* emitSettlements(
        yield* runtime
          .processConversation(agent, conversationId)
          .pipe(Effect.provide(makeItineraryToolLayer(dir))),
      );
      return;
    }
    case "run-join": {
      // Host plus one queued Submission with a DISTINCT payload: the join rows count prompt
      // coverage of the queued text from canonical records alone.
      yield* submitPlanner(`${idempotencyKey}-1`);
      yield* submitPlannerQuestion(`${idempotencyKey}-2`, JOIN_QUESTION);
      const model =
        env.EFFECT_AGENT_MARKER_FILE === undefined
          ? yield* makeScriptedModel(() => finalParts(CHILD_ANSWER))
          : // The model blocks AFTER the pre-Turn join drain claimed, appended, and marked the
            // queued input joined — SIGKILL then leaves `joined` + a nonterminal host.
            yield* makeScriptedStreamModel(blockedForeverScript(env.EFFECT_AGENT_MARKER_FILE));
      const agent = Agent.withModel(plannerDefinition, model);
      yield* emitSettlements(yield* runtime.processConversation(agent, conversationId));
      return;
    }
    case "resolve-approval": {
      const decision = requireEnv(env.EFFECT_AGENT_DECISION, "EFFECT_AGENT_DECISION");
      const submission = yield* lookupSubmission;
      const intent = yield* runtime.resolveApproval(
        ApprovalDecisionCommand.make({
          submissionId: submission.submissionId,
          toolCallId: decodeToolCallId(BOOK_CALL_ID),
          decision: decision === "approved" ? "approved" : "denied",
          resolver: "operator",
          reason: "crash-harness decision from a second process",
        }),
      );
      yield* emit({ kind: "resolved", value: intent.decision });
      return;
    }
    case "resolve-unknown": {
      const submission = yield* lookupSubmission;
      const intent = yield* runtime.resolveUnknown(
        UnknownResolutionCommand.make({
          submissionId: submission.submissionId,
          toolCallId: decodeToolCallId(BOOK_CALL_ID),
          author: "operator",
          reason: "the supplier store shows the call never started",
          resolution: ResolutionNeverHappened.make(),
        }),
      );
      yield* emit({ kind: "resolved", value: intent.resolution._tag });
      return;
    }
    case "subagent-run": {
      // Establishment → child Settlement → join, all in one process over one pool of resolved
      // Bindings; armed kill/block failpoints land at exactly one durable step of that flow.
      const resolver = yield* makeSubagentResolver;
      const receipt = yield* submitCoordinator(idempotencyKey);
      const childConversation = childConversationIdFor(
        receipt.submissionId,
        decodeToolCallId(DELEGATE_CALL_ID),
      );
      const establishment = yield* driveResolved(resolver, conversationId);
      const child = yield* driveResolved(resolver, childConversation);
      const join = yield* driveResolved(resolver, conversationId);
      yield* emitSettlements([...establishment, ...child, ...join]);
      return;
    }
    case "subagent-child": {
      // Second-process child worker: derive the child Conversation from the parent Submission's
      // durable identity and drive ONLY that lane (independent ownership, SUB-020).
      const resolver = yield* makeSubagentResolver;
      const parent = yield* lookupSubmission;
      const childConversation = childConversationIdFor(
        parent.submissionId,
        decodeToolCallId(DELEGATE_CALL_ID),
      );
      yield* emitSettlements(yield* driveResolved(resolver, childConversation));
      return;
    }
    case "subagent-abort": {
      const resolver = yield* makeSubagentResolver;
      const receipt = yield* submitCoordinator(idempotencyKey);
      yield* driveResolved(resolver, conversationId);
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "crash harness abort",
        }),
      );
      yield* emit({ kind: "resolved", value: "aborted" });
      return;
    }
    case "subagent-recover": {
      // One host startup-recovery pass over the shared file (binding-free executors only); the
      // armed kill dies mid-pass, e.g. right after the propagated child abort intent commits.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const host = yield* NodeDurableHost;
          yield* emit({ kind: "resolved", value: String(host.startupRecovery.length) });
        }).pipe(Effect.provide(NodeDurableHost.layer)),
      );
      return;
    }
  }
});

const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });
const decodeErrorTag = Schema.decodeUnknownOption(ErrorTag);

/** A superseded Attempt failing typed is EXPECTED evidence of fencing, not a harness defect. */
const isFencedFailure = (tag: string): boolean =>
  tag === "FenceRejected" || tag === "OwnershipLost";

const exit = await Effect.runPromiseExit(
  scenario.pipe(Effect.provide(Layer.mergeAll(NodeDurableRuntime.layer(options), searchToolLayer))),
);

if (Exit.isFailure(exit)) {
  const failure = Cause.findErrorOption(exit.cause);
  const tag = Option.isSome(failure)
    ? Option.match(decodeErrorTag(failure.value), {
        onNone: () => "UnknownError",
        onSome: ({ _tag }) => _tag,
      })
    : "Defect";
  if (isFencedFailure(tag)) {
    await Effect.runPromise(emit({ kind: "worker-failure", tag }));
    process.exitCode = FENCED_EXIT_CODE;
  } else {
    console.error(Cause.pretty(exit.cause));
    process.exitCode = 1;
  }
}
