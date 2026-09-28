import {
  BrowserSessionError,
  BrowserSessionReference,
  BrowserSessions,
} from "@effect-agent/platform-cloudflare/browser-session";
import { assert, expectTypeOf, it } from "@effect/vitest";
import type { Scope } from "effect";
import { Deferred, Effect, Exit, Fiber, Redacted, Schema } from "effect";
import { TestClock } from "effect/testing";

import type { Browser } from "../src/browser.ts";
import { LabError, seed, verify, type RunInput } from "../src/contract.ts";
import type { connectKitesurf } from "../src/kitesurf.ts";
import { emptyControl, makeOwner, Control } from "../src/owner.ts";
import type { executeTask } from "../src/runner.ts";
import { fromReady, runOutcome, verifiedMillis } from "../src/state.ts";
import type { traceModels, Trace } from "../src/telemetry.ts";
import { makeTrace } from "../src/telemetry.ts";

expectTypeOf<Effect.Error<ReturnType<typeof executeTask>>>().toEqualTypeOf<LabError>();
expectTypeOf<Effect.Services<ReturnType<typeof executeTask>>>().toEqualTypeOf<
  Scope.Scope | Browser | Trace
>();
expectTypeOf<Effect.Error<ReturnType<typeof connectKitesurf>>>().toEqualTypeOf<LabError>();
expectTypeOf<Effect.Services<ReturnType<typeof connectKitesurf>>>().toEqualTypeOf<Scope.Scope>();
expectTypeOf<
  Effect.Error<ReturnType<typeof traceModels<string, "expected", BrowserSessions>>>
>().toEqualTypeOf<"expected">();
expectTypeOf<
  Effect.Services<ReturnType<typeof traceModels<string, "expected", BrowserSessions>>>
>().toEqualTypeOf<BrowserSessions | Trace>();

const request = (): RunInput => ({
  id: crypto.randomUUID(),
  scenario: "create",
  mode: "scripted",
  prompt: "",
  screenshots: false,
  liveView: false,
});

const reference = BrowserSessionReference.make({
  version: 1,
  sessionId: Redacted.make("00000000-0000-4000-8000-000000000001"),
  contextId: Redacted.make("context"),
  targetId: Redacted.make("page"),
  expiresAt: 600_000,
  commandTimeoutMillis: 5_000,
});

it.effect(
  "closes ephemeral Kitesurf attempts on failure, defect, interruption and timeout without a persistent-session fallback",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["failure", "defect", "stop", "timeout"] as const) {
        let state: Control = emptyControl;
        let opened = 0;
        let closed = 0;
        const started = yield* Deferred.make<void>();

        const owner = makeOwner(
          {
            read: () => state,
            write: (value) => {
              state = value;
            },
            alarm: () => Effect.void,
          },
          {
            model: "unused",
            apiKey: "",
            kitesurf: (retainClose) =>
              Effect.gen(function* () {
                opened++;
                retainClose(
                  Effect.sync(() => {
                    closed++;
                  }),
                );
                yield* Deferred.succeed(started, undefined);
                if (outcome === "failure")
                  return yield* new LabError({
                    code: "browser",
                    message: "Fixture connection failure",
                  });
                if (outcome === "defect") return yield* Effect.die("Fixture defect");

                return yield* Effect.never;
              }),
          },
        );

        const unused = () => Effect.die("Kitesurf must not use persistent Chromium sessions");

        const run = yield* owner.run({ ...request(), engine: "kitesurf" }).pipe(
          Effect.provideService(
            BrowserSessions,
            BrowserSessions.of({
              createAttached: unused,
              create: unused,
              attach: unused,
              close: unused,
              keepAlive: unused,
            }),
          ),
          Effect.forkChild,
        );

        yield* Deferred.await(started);
        if (outcome === "stop") yield* owner.stop();
        if (outcome === "timeout") yield* TestClock.adjust("4 minutes");
        const report = yield* Fiber.join(run);

        assert.strictEqual(opened, outcome === "failure" ? 2 : 1);
        assert.strictEqual(closed, opened);
        assert.strictEqual(report.status, outcome === "stop" ? "cancelled" : "failed");
        assert.strictEqual(report.cleanup, "closed");
        assert.isNull(report.readyAt);
        assert.isNull(state.reference);
        assert.isNull(state.active);
        assert.isFalse((yield* owner.snapshot()).ready);
      }
    }),
);

it.effect(
  "admits Jev-only without a planner key and rejects conflicting settings before browser allocation",
  () =>
    Effect.gen(function* () {
      let state: Control = emptyControl;
      let allocations = 0;

      const owner = makeOwner(
        {
          read: () => state,
          write: (value) => {
            state = value;
          },
          alarm: () => Effect.void,
        },
        { model: "gpt-6-luna", apiKey: "", jevApiKey: "test-key" },
      );

      const input: RunInput = {
        ...request(),
        scenario: "wikipedia",
        mode: "agent",
        wikiDriver: "jev",
      };

      const services = BrowserSessions.of({
        createAttached: () => Effect.die("The lab creates and attaches separately"),
        create: () => {
          allocations++;

          return Effect.fail(
            new BrowserSessionError({
              reason: "provider",
              dispatch: "not-dispatched",
              cleanup: "not-requested",
            }),
          );
        },
        attach: () => Effect.die("Must not attach"),
        close: () => Effect.void,
        keepAlive: () => Effect.void,
      });

      yield* Effect.gen(function* () {
        for (const invalid of [
          { ...input, scenario: "create" as const },
          { ...input, model: "gpt-6-luna" as const },
          { ...input, grounding: "jev" as const },
        ]) {
          assert.strictEqual((yield* owner.run(invalid).pipe(Effect.result))._tag, "Failure");
          assert.strictEqual(allocations, 0);
        }
        const report = yield* owner.run(input);

        assert.strictEqual(
          allocations,
          2,
          "Missing OpenAI key must not block the decision-only path",
        );
        assert.strictEqual(report.model, "jev-latest");
        assert.strictEqual(report.input.engine, "chromium");
        assert.strictEqual(report.input.reasoning, undefined);
        assert.strictEqual(report.status, "failed", "Retain the simulated allocation failure");
        assert.strictEqual(state.active, null);
      }).pipe(Effect.provideService(BrowserSessions, services));
    }),
);

it.effect(
  "retains failures, releases resources on timeout/interruption/defect, and rejects concurrent or repeated admissions",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["failure", "defect", "stop", "timeout"] as const) {
        let state: Control = emptyControl;
        let closed = 0;
        const attached = yield* Deferred.make<void>();

        const owner = makeOwner(
          {
            read: () => state,
            write: (value) => {
              state = value;
            },
            alarm: () => Effect.void,
          },
          { model: "unused", apiKey: "" },
        );

        const services = BrowserSessions.of({
          createAttached: () => Effect.die("The lab creates and attaches separately"),
          create: (_, retain) => retain(reference).pipe(Effect.as(reference)),
          attach: () =>
            Deferred.succeed(attached, undefined).pipe(
              Effect.andThen(
                outcome === "failure"
                  ? Effect.fail(
                      new BrowserSessionError({
                        reason: "provider",
                        dispatch: "not-dispatched",
                        cleanup: "not-requested",
                      }),
                    )
                  : outcome === "defect"
                    ? Effect.die("test defect")
                    : Effect.never,
              ),
            ),
          close: () =>
            Effect.sync(() => {
              closed++;
            }),
          keepAlive: () => Effect.void,
        });

        yield* Effect.gen(function* () {
          const input = request();
          const fiber = yield* Effect.forkChild(owner.run(input));

          yield* Deferred.await(attached);
          if (outcome === "stop" || outcome === "timeout") {
            const concurrent = yield* Effect.result(owner.run(request()));

            assert.strictEqual(concurrent._tag, "Failure");
            if (outcome === "stop") yield* owner.stop();
            else yield* TestClock.adjust("4 minutes");
          }
          const report = yield* Fiber.join(fiber);

          assert.strictEqual(report.status, outcome === "stop" ? "cancelled" : "failed");
          assert.strictEqual(report.cleanup, "closed");
          assert.strictEqual(closed, outcome === "failure" ? 2 : 1);
          assert.isNull(report.readyAt);
          assert.isNull(verifiedMillis(report));
          assert.strictEqual(
            runOutcome(report),
            outcome === "stop" ? "preparation cancelled" : "preparation failed",
          );
          assert.strictEqual(state.active, null);
          assert.strictEqual(state.reference, null);
          assert.strictEqual((yield* Effect.result(owner.run(input)))._tag, "Failure");
          assert.isTrue(report.spans.some((span) => span.outcome !== "success"));
        }).pipe(Effect.provideService(BrowserSessions, services));
      }
    }),
);

it.effect(
  "reconstructs a lost owner without replay, and keeps a failed cleanup reference for explicit retry",
  () =>
    Effect.gen(function* () {
      const input = request();
      let state: Control = { ...emptyControl, reference, active: input.id, lastRun: input.id };
      let attempts = 0;

      const owner = makeOwner(
        {
          read: () => state,
          write: (value) => {
            state = value;
          },
          alarm: () => Effect.void,
        },
        { model: "unused", apiKey: "" },
      );

      const unused = () => Effect.die("Lost work must not acquire or attach a browser");

      yield* Effect.gen(function* () {
        assert.include((yield* owner.snapshot()).notice ?? "", "unresolved");
        assert.isTrue((yield* owner.snapshot()).canClose, "Lost requests must expose recovery");
        assert.strictEqual((yield* Effect.result(owner.run(request())))._tag, "Failure");
        assert.strictEqual((yield* Effect.result(owner.close()))._tag, "Failure");
        assert.strictEqual(state.reference, reference);
        yield* owner.close();
        assert.strictEqual(state.reference, null);
        assert.strictEqual(state.active, null);
        assert.isFalse((yield* owner.snapshot()).canClose);
        assert.strictEqual(attempts, 2);
      }).pipe(
        Effect.provideService(BrowserSessions, {
          createAttached: unused,
          create: unused,
          attach: unused,
          keepAlive: unused,
          close: () =>
            Effect.suspend(() =>
              ++attempts === 1
                ? Effect.fail(
                    new BrowserSessionError({
                      reason: "cleanup",
                      dispatch: "not-dispatched",
                      cleanup: "unconfirmed",
                    }),
                  )
                : Effect.void,
            ),
        }),
      );
    }),
);

it.effect(
  "records actual span duration and failure instead of treating incomplete work as success",
  () =>
    Effect.gen(function* () {
      const trace = yield* makeTrace(request(), "none");

      const fiber = yield* Effect.forkChild(
        trace.measure(
          "action",
          "click",
          Effect.sleep("2 seconds").pipe(Effect.andThen(Effect.fail("rejected"))),
        ),
      );

      yield* TestClock.adjust("2 seconds");
      assert.isTrue(Exit.isFailure(yield* Fiber.await(fiber)));
      assert.strictEqual(trace.snapshot().spans[0]?.duration, 2_000);
      assert.strictEqual(trace.snapshot().spans[0]?.outcome, "failure");
      assert.strictEqual(trace.snapshot().firstActionAt, null);
    }),
);

it("rejects extra writes, changed unrelated fields and agent-only success claims", () => {
  const created = {
    id: 5,
    title: "Ship demo",
    assignee: "Alex",
    priority: "High",
    status: "Todo",
  } as const;

  assert.isTrue(verify("create", [...seed, created]));
  assert.isFalse(verify("create", [...seed, created, { ...created, id: 6 }]));
  assert.isFalse(
    verify("create", [...seed.map((task) => ({ ...task, priority: "High" as const })), created]),
  );
  assert.isFalse(verify("create", seed));
  assert.isFalse(verify("custom", [...seed, created]));
});

it.effect(
  "accounts for every owner mutation failpoint and retires a recovered browser without replay",
  () =>
    Effect.gen(function* () {
      const transitions = [
        "admitted",
        "retained",
        "settled",
        "closed",
        "released",
        "alarm-set",
        "alarm-clear",
      ];

      const reached = new Set<string>();

      for (const transition of transitions)
        for (const boundary of ["before", "after"]) {
          const location = `${boundary}:${transition}`;
          let armed = true;
          let alive = transition === "closed" || transition === "released";

          let encoded = Schema.encodeSync(Schema.fromJsonString(Control))({
            ...emptyControl,
            reference: alive ? reference : null,
          });

          let acquisitions = 0;

          const hit = (point: string) => {
            if (armed && point === location) {
              armed = false;
              reached.add(location);
              throw new Error(`failpoint ${location}`);
            }
          };

          const store = {
            read: () => Schema.decodeSync(Schema.fromJsonString(Control))(encoded),
            write: (value: Control, operation: string) => {
              hit(`before:${operation}`);
              encoded = Schema.encodeSync(Schema.fromJsonString(Control))(value);
              hit(`after:${operation}`);
            },
            alarm: (at: number | null) =>
              Effect.sync(() => {
                const operation = at === null ? "alarm-clear" : "alarm-set";

                hit(`before:${operation}`);
                hit(`after:${operation}`);
              }),
          };

          const owner = makeOwner(store, { model: "unused", apiKey: "" });

          const services = BrowserSessions.of({
            createAttached: () => Effect.die("The lab creates and attaches separately"),
            create: (_, retain) =>
              Effect.gen(function* () {
                acquisitions++;
                alive = true;
                const exit = yield* Effect.exit(retain(reference));

                if (Exit.isFailure(exit)) alive = false;
                yield* exit;

                return reference;
              }),
            attach: () =>
              Effect.fail(
                new BrowserSessionError({
                  reason: "provider",
                  dispatch: "not-dispatched",
                  cleanup: "not-requested",
                }),
              ),
            close: () =>
              Effect.sync(() => {
                alive = false;
              }),
            keepAlive: () => Effect.void,
          });

          yield* Effect.gen(function* () {
            const input = request();

            const operation =
              transition === "closed" || transition === "released"
                ? owner.close()
                : owner.run(input).pipe(Effect.asVoid);

            yield* Effect.exit(operation);
            assert.isTrue(reached.has(location), `Unreached failpoint: ${location}`);
            const admitted = store.read().admitted.includes(input.id);
            const acquiredBeforeRecovery = acquisitions;
            const recovered = makeOwner(store, { model: "unused", apiKey: "" });

            yield* recovered.close();
            assert.isFalse(alive, location);
            assert.isNull(store.read().reference, location);
            assert.strictEqual(
              acquisitions,
              acquiredBeforeRecovery,
              "Recovery must never create a browser",
            );
            if (admitted)
              assert.strictEqual(
                (yield* Effect.result(recovered.run(input)))._tag,
                "Failure",
                "Admitted input cannot be replayed after recovery",
              );
          }).pipe(Effect.provideService(BrowserSessions, services));
        }
      assert.strictEqual(reached.size, transitions.length * 2);
    }),
);

it.effect(
  "starts flow timing after readiness and excludes preparation and cleanup on one clock",
  () =>
    Effect.gen(function* () {
      const trace = yield* makeTrace(request(), "none");

      yield* TestClock.adjust("40 seconds");
      assert.isNull(verifiedMillis(trace.snapshot()));
      trace.ready();
      yield* TestClock.adjust("2 seconds");
      yield* trace.measure("action", "first interaction", Effect.void);
      trace.ready();
      yield* TestClock.adjust("3 seconds");
      trace.update({ verifiedAt: trace.now(), finishedAt: trace.now(), status: "passed" });
      yield* TestClock.adjust("8 seconds");
      trace.update({ elapsed: trace.now(), cleanup: "closed" });
      const report = trace.snapshot();

      assert.strictEqual(report.readyAt, 40_000, "A later call cannot reset the flow clock");
      assert.strictEqual(fromReady(report, report.firstActionAt), 2_000);
      assert.strictEqual(verifiedMillis(report), 5_000);
      assert.strictEqual(report.elapsed, 53_000, "Keep full request evidence separately");
      assert.strictEqual(runOutcome(report), "passed");
    }),
);
