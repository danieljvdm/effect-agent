import * as fs from "node:fs";

import { Agent } from "@effect-agent/core";
import {
  AbortCommand,
  DurableAgentRuntime,
  DurableRuntimeFailpointLocation,
  type DurableRuntimeFailpointHandler,
  type Receipt,
  type Settlement,
} from "@effect-agent/session";
import {
  SqliteStorageFailpointLocation,
  type SqliteStorageFailpointHandler,
} from "@effect-agent/storage-sqlite";
import { Cause, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import type { Response } from "effect/unstable/ai";

import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "../../src/index.ts";
import {
  CHILD_ANSWER,
  CHILD_PRODUCER_ID,
  CRASH_DEPLOYMENT_ID,
  CRASH_QUESTION,
  CrashScenario,
  KILL_EXIT_CODE,
  FENCED_EXIT_CODE,
  STALE_ANSWER,
  crashSubmitOptions,
  decodeConversationId,
  encodeChildMessage,
  finalParts,
  makeScriptedModel,
  makeScriptedStreamModel,
  plannerDefinition,
  searchDefinition,
  searchToolLayer,
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
  EFFECT_AGENT_LEASE_MS: Schema.optionalKey(MillisFromString),
  EFFECT_AGENT_MARKER_FILE: Schema.optionalKey(Schema.NonEmptyString),
  EFFECT_AGENT_RELEASE_FILE: Schema.optionalKey(Schema.NonEmptyString),
});

const env = Schema.decodeUnknownSync(WorkerEnv)(process.env);
const conversationId = decodeConversationId(env.EFFECT_AGENT_CONVERSATION);
const idempotencyKey = env.EFFECT_AGENT_KEY;

/** A kill failpoint is a crash, not a failure: exit immediately, skipping every finalizer. */
const hardKill: Effect.Effect<never> = Effect.sync(() => process.exit(KILL_EXIT_CODE));

const runtimeKill: DurableRuntimeFailpointHandler | undefined =
  env.EFFECT_AGENT_KILL_AT === undefined
    ? undefined
    : (location) => (location === env.EFFECT_AGENT_KILL_AT ? hardKill : Effect.void);

const storageKill: SqliteStorageFailpointHandler | undefined =
  env.EFFECT_AGENT_KILL_AT_STORAGE === undefined
    ? undefined
    : (location) => (location === env.EFFECT_AGENT_KILL_AT_STORAGE ? hardKill : Effect.void);

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

const submitPlanner = (key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
    const agent = Agent.withModel(plannerDefinition, model);
    const receipt: Receipt = yield* runtime.submit(
      agent,
      { question: CRASH_QUESTION },
      crashSubmitOptions(env.EFFECT_AGENT_CONVERSATION, key),
    );
    yield* emit({ kind: "receipt", key, receipt });
    return receipt;
  });

const emitSettlements = (settlements: ReadonlyArray<Settlement>) =>
  emit({ kind: "settlements", settlements });

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
