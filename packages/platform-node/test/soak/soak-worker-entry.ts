import type { ThreadId } from "@effect-agent/core";
import { DurableAgentRuntime, SubmissionLedger } from "@effect-agent/thread";
import { NodeRuntime } from "@effect/platform-node";
import { Cause, Duration, Effect, Exit, Layer, Option, Schema, Stream } from "effect";

import { NodeDurableAgentRuntime, type NodeDurableAgentRuntimeOptions } from "../../src/index.ts";
import { SOAK_DEPLOYMENT_ID, SoakEnv, makeSoakBindings } from "./soak-fixtures.ts";

/**
 * Soak worker entrypoint (P7 WP4): one process = one host lifetime over the shared SQLite
 * file. It loops forever — recovery pass, nonterminal scan, one resolved drive per discovered
 * lane — until the soak harness SIGKILLs it (no finalizers, no drain, exactly like a crash).
 * Typed failures inside one iteration (fencing, contention, mid-flight kills of siblings) are
 * tolerated: a real host supervises and restarts its worker loop; defects crash the process
 * loudly so the harness surfaces them.
 */

const MillisFromString = Schema.FiniteFromString.check(
  Schema.isInt(),
  Schema.isGreaterThan(0),
  Schema.isLessThanOrEqualTo(600_000),
);

const WorkerEnv = Schema.Struct({
  [SoakEnv.database]: Schema.NonEmptyString,
  [SoakEnv.producer]: Schema.NonEmptyString,
  [SoakEnv.leaseMillis]: Schema.optionalKey(MillisFromString),
});

const env = Schema.decodeUnknownSync(WorkerEnv)(process.env);

const options: NodeDurableAgentRuntimeOptions = {
  filename: env[SoakEnv.database],
  deploymentId: SOAK_DEPLOYMENT_ID,
  producerId: env[SoakEnv.producer],
  ownershipLeaseDuration: env[SoakEnv.leaseMillis] ?? 750,
  leaseRenewalInterval: 200,
  abortPollInterval: 50,
  settlementPollInterval: 50,
  wakeScanInterval: 500,
};

/** Success or typed failure both continue the loop; defects escape and crash the worker. */
const tolerateTyped = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<void, never, R> =>
  effect.pipe(
    Effect.exit,
    Effect.flatMap((exit) => {
      if (Exit.isSuccess(exit)) return Effect.void;
      if (Option.isSome(Cause.findErrorOption(exit.cause))) return Effect.void;

      return Effect.die(new Error(`soak worker step died: ${Cause.pretty(exit.cause)}`));
    }),
  );

const workerLoop = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  const ledger = yield* SubmissionLedger;
  const driveResolved = (thread: ThreadId) => runtime.processThreadResolved(thread);

  while (true) {
    // Heal what a killed sibling left behind, then drive every discovered lane once.
    yield* tolerateTyped(runtime.runRecovery);

    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal).pipe(
      Effect.exit,
      Effect.map((exit) => (Exit.isSuccess(exit) ? Array.from(exit.value) : [])),
    );

    const seen = new Set<ThreadId>();

    for (const submission of nonterminal) {
      if (seen.has(submission.threadId)) continue;
      seen.add(submission.threadId);
      yield* tolerateTyped(driveResolved(submission.threadId));
    }
    yield* Effect.sleep(Duration.millis(25));
  }
});

const runtimeLayer = Layer.unwrap(
  Effect.map(makeSoakBindings(), (bindings) =>
    NodeDurableAgentRuntime.layerWithBindings(bindings, options),
  ),
);

NodeRuntime.runMain(workerLoop.pipe(Effect.provide(runtimeLayer)));
