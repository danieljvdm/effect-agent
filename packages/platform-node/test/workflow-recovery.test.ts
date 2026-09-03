import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentRuntime } from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { AbortCommand } from "@effect-agent/thread/SubmissionLedger";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Clock, Deferred, Effect, Fiber, FileSystem, Layer, Ref, Stream } from "effect";
import { Toolkit, type Response } from "effect/unstable/ai";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  makePlanner,
  pendingIntents,
  planner,
  readLog,
  submitOptions,
  temporaryDirectory,
  until,
  usage,
} from "./workflow-fixtures.ts";

const platform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer);

it.live(
  "releases the only execution permit while an attached child runs and recovers both after restart",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const started = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);

      const childModel = yield* makeModel((call) =>
        call === 0
          ? Stream.fromEffect(
              Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
            ).pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1)))
          : Stream.fromIterable(finalParts("child")),
      );

      const childDefinitions = definitionsFor(planner.id);
      const childDigests = yield* digestDefinitions(childDefinitions);

      const delegation = Subagent.define("delegate_workflow", {
        target: planner,
        failureMode: "return",
      });

      const parentModel = yield* makeModel((call) =>
        Stream.fromIterable<Response.StreamPartEncoded>(
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "delegate-1",
                  name: delegation.name,
                  params: { question: "child" },
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts("parent"),
        ),
      );

      const parent = Agent.withModel(
        Agent.make("workflow-parent", {
          input: planner.input,
          output: planner.output,
          instructions: "Delegate once then answer.",
          toolkit: Toolkit.make(delegation.tool),
          policy: planner.policy,
        }),
        parentModel.model,
      );

      const definitions = definitionsFor(parent.definition.id);
      const digests = yield* digestDefinitions(definitions);

      const handlers = SubagentRuntime.layer(delegation, childModel.model, {
        durable: { targetDigests: childDigests },
      }).pipe(Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]));

      const stack = hostLayer(directory, [
        { agent: parent, definitions },
        { agent: planner, model: childModel.model, definitions: childDefinitions },
      ]).pipe(Layer.provide(handlers));

      const first = yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        yield* host.submit(parent, { question: "parent" }, submitOptions(digests));

        return yield* Effect.never;
      }).pipe(Effect.provide(stack), Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(first);
      expect(yield* Ref.get(finalized)).toBe(1);

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;
        const receipt = yield* host.submit(parent, { question: "parent" }, submitOptions(digests));

        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        const records = yield* readLog(receipt.threadId);

        expect(
          records.filter((row) => row.record.payload._tag === "SubagentRequested"),
        ).toHaveLength(1);
        expect(records.filter((row) => row.record.payload._tag === "ToolCallSettled")).toHaveLength(
          1,
        );
        expect(
          records.filter((row) => row.record.payload._tag === "SubmissionSettled"),
        ).toHaveLength(1);
        expect(yield* Ref.get(childModel.calls)).toBe(2);
      }).pipe(Effect.provide(stack));
    }).pipe(Effect.scoped, Effect.provide(platform)),
  // Both SQLite hosts restart before the cleanup wait, which allows up to ten seconds.
  30_000,
);

it.live(
  "a timed out waiter detaches; explicit abort interrupts the model and releases its resources",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const started = yield* Deferred.make<void>();
      const finalized = yield* Ref.make(0);

      const fixture = yield* makePlanner(() =>
        Stream.fromEffect(
          Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
        ).pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1))),
      );

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        const receipt = yield* host.submit(
          fixture.agent,
          { question: "wait" },
          submitOptions(fixture.digests),
        );

        yield* Deferred.await(started);
        const waited = yield* host.awaitSettlement(receipt).pipe(Effect.timeoutOption("20 millis"));

        expect(waited._tag).toBe("None");
        expect(yield* Ref.get(finalized)).toBe(0);
        expect((yield* host.submissionStatus(receipt))._tag).toBe("pending");
        yield* host.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "operator",
            reason: "cancel",
          }),
        );
        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("aborted");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        expect(yield* Ref.get(finalized)).toBe(1);
      }).pipe(Effect.provide(hostLayer(directory, [fixture])));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("competing Workflow owners fence the stale model result out of canonical history", () =>
  Effect.gen(function* () {
    let phase = "constructing first owner";

    yield* Effect.addFinalizer(() =>
      Effect.logInfo(`Workflow ownership test stopped while ${phase}`),
    );
    const directory = yield* temporaryDirectory;
    const fs = yield* FileSystem.FileSystem;

    yield* fs.makeDirectory(`${directory}/first`);
    yield* fs.makeDirectory(`${directory}/second`);
    const clock = yield* Clock.Clock;
    let offset = 0;

    // Advance once to expire the first claim without sleeping. Native delivery timers
    // continue advancing, and the replacement uses a long, actively renewed lease.
    const ownershipClock: Clock.Clock = {
      sleep: (duration) => clock.sleep(duration),
      monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
      monotonicTimeNanos: clock.monotonicTimeNanos,
      currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe() + offset,
      currentTimeMillis: Effect.sync(() => clock.currentTimeMillisUnsafe() + offset),
      currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
      currentTimeNanos: Effect.sync(
        () => clock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
      ),
    };

    const started = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const firstCompleted = yield* Deferred.make<void>();

    const fixture = yield* makePlanner((call) =>
      call === 0
        ? Stream.fromEffect(
            Deferred.succeed(started, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts("stale"))))
        : Stream.fromIterable(finalParts("fresh")),
    );

    // Independent SQL Workflow engines contend only over the shared canonical agent ledger.
    const stack = (owner: "first" | "second") =>
      hostLayer(`${directory}/${owner}`, [fixture], {
        filename: `${directory}/agent.sqlite`,
        producerId: owner,
        // These two SQLite connections share an event loop. A synchronous busy wait
        // would prevent its lock holder from committing; let Workflow retry contention.
        busyTimeout: 0,
        ownershipLeaseDuration: owner === "first" ? 200 : 30000,
        leaseRenewalInterval: owner === "first" ? 30000 : 50,
      }).pipe(Layer.provide(Layer.succeedContext(Clock.Clock.context(ownershipClock))));

    const first = yield* Effect.gen(function* () {
      const host = yield* WorkflowAgentHost;

      yield* host.submit(fixture.agent, { question: "fence" }, submitOptions(fixture.digests));

      // Dispatch cleanup proves this owner's native completion without another settlement waiter.
      yield* until(pendingIntents, (rows) => rows.length === 0);
      yield* Deferred.succeed(firstCompleted, undefined);

      return yield* Effect.never;
    }).pipe(Effect.provide(stack("first")), Effect.forkChild);

    phase = "waiting for first model";
    yield* Deferred.await(started);
    offset = 1000;
    phase = "constructing replacement owner";

    yield* Effect.gen(function* () {
      const host = yield* WorkflowAgentHost;

      phase = "submitting to replacement owner";

      const receipt = yield* host.submit(
        fixture.agent,
        { question: "fence" },
        submitOptions(fixture.digests),
      );

      phase = "waiting for replacement settlement";
      expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
      yield* Deferred.succeed(release, undefined);
      phase = "waiting for stale native Workflow completion and cleanup";
      yield* Effect.raceFirst(Deferred.await(firstCompleted), Fiber.join(first));
      phase = "checking canonical history";
      yield* host.repair;
      const log = yield* readLog(receipt.threadId);

      expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(1);
      const responses = log.filter((row) => row.record.payload._tag === "ModelResponseRecorded");

      expect(responses).toHaveLength(1);
      expect(JSON.stringify(responses)).toContain("fresh");
      expect(JSON.stringify(responses)).not.toContain("stale");
      expect(yield* Ref.get(fixture.calls)).toBe(2);
    }).pipe(Effect.provide(stack("second")));
    yield* Fiber.interrupt(first);
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("a one-row repair page reaches Unicode thread names in SQLite order", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const fixture = yield* makePlanner(() => Stream.never);
    const ids = ["\uE000", "😀"];

    yield* Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      for (const id of ids)
        yield* runtime.submit(fixture.agent, { question: id }, submitOptions(fixture.digests, id));
      const rows = yield* until(pendingIntents, (intents) => intents.length === 2);

      expect(rows.map((row) => row.receipt.threadId).sort()).toEqual([...ids].sort());
    }).pipe(Effect.provide(hostLayer(directory, [fixture], {}, false, 1)));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);
