import { RunEvent } from "@effect-agent/core/RunEvent";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Layer, Schema, Stream } from "effect";
import * as AsyncResult from "effect/unstable/reactivity/AsyncResult";
import * as Atom from "effect/unstable/reactivity/Atom";
import * as AtomRegistry from "effect/unstable/reactivity/AtomRegistry";
import * as RpcTest from "effect/unstable/rpc/RpcTest";

import { chatStateAtom, runCapabilityChatAtom, runChatAtom } from "./chat-state";
import { eventBatches } from "./event-batches";
import {
  DemoControlAccepted,
  DemoRunFailure,
  type DemoOperationalEvent,
} from "./operational-contracts";
import { DemoRunRpcs } from "./run-rpc";
import { DemoRunRpcClient, DemoRunRpcRuntime } from "./run-rpc-client";
import { demoStateAtom, runOperationalDemoAtom } from "./state";

const base = {
  eventVersion: 1 as const,
  runId: "run-streaming",
  threadId: "thread-streaming",
  agentId: "agent-streaming",
  timestamp: "2026-09-08T12:00:00.000Z",
};

const event = (input: typeof RunEvent.Encoded): RunEvent => Schema.decodeSync(RunEvent)(input);
const started = event({ ...base, _tag: "RunStarted", sequence: 0 });

const deltas = Array.from({ length: 130 }, (_, index) =>
  event({
    ...base,
    _tag: index % 2 === 0 ? "TextDelta" : "ReasoningDelta",
    sequence: index + 1,
    turnId: "turn-streaming",
    text: index % 2 === 0 ? '{"answer":' : "Thinking. ",
  }),
);

const tool = event({
  ...base,
  _tag: "ToolCallDeclared",
  sequence: 131,
  turnId: "turn-streaming",
  toolCallId: "tool-1",
  toolName: "calculate",
  parameters: { left: 1, right: 2 },
  providerExecuted: false,
});

const completed = (
  output: Extract<typeof RunEvent.Encoded, { readonly _tag: "RunCompleted" }>["output"],
) =>
  event({
    ...base,
    _tag: "RunCompleted",
    sequence: 132,
    turns: 1,
    finishReason: "completed",
    output,
  });

const failed = DemoRunFailure.make({
  errorTag: "FixtureFailure",
  message: "Fixture stream failed.",
});

const accepted = DemoControlAccepted.make({ accepted: true });

// Keep the production Atom graph and generated RPC cancellation behavior.
const registryFor = Effect.fn("test.registryFor")(function* (
  source: Stream.Stream<DemoOperationalEvent, DemoRunFailure>,
) {
  const lifecycle = { finalized: 0 };
  const observed = source.pipe(Stream.ensuring(Effect.sync(() => lifecycle.finalized++)));

  const handlers = DemoRunRpcs.toLayer({
    StreamChatRun: () => observed.pipe(Stream.filter(Schema.is(RunEvent))),
    StreamOperationalRun: () => observed,
    StreamLiveTravelChatRun: () => observed,
    QueueRunCommand: () => Effect.succeed(accepted),
    ResolveRunApproval: () => Effect.succeed(accepted),
  });

  const layer = Layer.effect(DemoRunRpcClient, RpcTest.makeClient(DemoRunRpcs)).pipe(
    Layer.provide(handlers),
  );

  const registry = yield* Effect.acquireRelease(
    Effect.sync(() => AtomRegistry.make({ initialValues: [[DemoRunRpcRuntime.layer, layer]] })),
    (value) => Effect.sync(() => value.dispose()),
  );

  yield* AtomRegistry.mount(registry, chatStateAtom);
  yield* AtomRegistry.mount(registry, demoStateAtom);

  return { registry, lifecycle };
});

for (const mode of ["general", "capability", "simulator"] as const) {
  describe(`${mode} streamed Atom projection`, () => {
    for (const chunkSize of [1, 256]) {
      it.effect(`retains ordered immutable evidence with chunk size ${chunkSize}`, () =>
        Effect.gen(function* () {
          const output: Extract<
            typeof RunEvent.Encoded,
            { readonly _tag: "RunCompleted" }
          >["output"] = mode === "general" ? { answer: "Validated answer." } : { itineraries: [] };

          const events = [started, ...deltas, tool, completed(output)];

          const { registry, lifecycle } = yield* registryFor(
            Stream.fromIterable(events).pipe(Stream.rechunk(chunkSize)),
          );

          const stateAtom: Atom.Atom<{
            readonly status: string;
            readonly events: ReadonlyArray<DemoOperationalEvent>;
          }> = mode === "simulator" ? demoStateAtom : chatStateAtom;

          const snapshots: Array<ReadonlyArray<DemoOperationalEvent>> = [];

          const release = registry.subscribe(stateAtom, (state) => {
            snapshots.push(state.events);
          });

          yield* Effect.addFinalizer(() => Effect.sync(release));
          if (mode === "general") {
            yield* AtomRegistry.mount(registry, runChatAtom);
            registry.set(runChatAtom, { mode: "deterministic", history: [], message: "Hello" });
            yield* AtomRegistry.getResult(registry, runChatAtom, { suspendOnWaiting: true });
          } else if (mode === "capability") {
            yield* AtomRegistry.mount(registry, runCapabilityChatAtom);
            registry.set(runCapabilityChatAtom, { scenario: "guided", message: "Hello" });
            yield* AtomRegistry.getResult(registry, runCapabilityChatAtom, {
              suspendOnWaiting: true,
            });
          } else {
            yield* AtomRegistry.mount(registry, runOperationalDemoAtom);
            registry.set(runOperationalDemoAtom, "guided");
            yield* AtomRegistry.getResult(registry, runOperationalDemoAtom, {
              suspendOnWaiting: true,
            });
          }
          const state = registry.get(stateAtom);

          expect(state.status).toBe("succeeded");
          expect(state.events).toEqual(events);
          expect(lifecycle.finalized).toBe(1);
          for (const snapshot of snapshots)
            expect(snapshot).toEqual(events.slice(0, snapshot.length));
          expect(snapshots.length).toBeGreaterThan(0);
          if (mode !== "simulator") {
            const chat = registry.get(chatStateAtom);
            const assistant = chat.messages.at(-1);

            expect(assistant?.events).toBe(chat.events);
            if (mode === "general") {
              expect(assistant?.reasoning).toBe("Thinking. ".repeat(65));
              expect(assistant?.content).toBe("Validated answer.");
            }
          }
        }),
      );
    }
    for (const ending of [
      "invalid-output",
      "failure",
      "defect",
      "timeout",
      "ended",
      "run-failed",
      "run-interrupted",
    ] as const) {
      it.effect(`retains the accepted prefix and finalizes after ${ending}`, () =>
        Effect.gen(function* () {
          const prefix = [started, ...deltas];

          const tail =
            ending === "invalid-output"
              ? Stream.succeed(completed({ invalid: true }))
              : ending === "failure"
                ? Stream.fail(failed)
                : ending === "defect"
                  ? Stream.die("Fixture defect")
                  : ending === "timeout"
                    ? Stream.fromEffect(
                        Effect.never.pipe(
                          Effect.timeout("1 millis"),
                          Effect.mapError(() => failed),
                        ),
                      )
                    : ending === "run-failed"
                      ? Stream.succeed(
                          event({
                            ...base,
                            _tag: "RunFailed",
                            sequence: 131,
                            errorTag: "FixtureFailure",
                            message: "Fixture failed.",
                          }),
                        )
                      : ending === "run-interrupted"
                        ? Stream.succeed(
                            event({
                              ...base,
                              _tag: "RunInterrupted",
                              sequence: 131,
                              message: "Fixture stopped.",
                            }),
                          )
                        : Stream.empty;

          const { registry, lifecycle } = yield* registryFor(
            Stream.concat(Stream.fromIterable(prefix), tail),
          );

          let exit;

          if (mode === "general") {
            yield* AtomRegistry.mount(registry, runChatAtom);
            registry.set(runChatAtom, { mode: "deterministic", history: [], message: "Hello" });
            exit = yield* Effect.exit(
              AtomRegistry.getResult(registry, runChatAtom, { suspendOnWaiting: true }),
            );
          } else if (mode === "capability") {
            yield* AtomRegistry.mount(registry, runCapabilityChatAtom);
            registry.set(runCapabilityChatAtom, { scenario: "guided", message: "Hello" });
            exit = yield* Effect.exit(
              AtomRegistry.getResult(registry, runCapabilityChatAtom, { suspendOnWaiting: true }),
            );
          } else {
            yield* AtomRegistry.mount(registry, runOperationalDemoAtom);
            registry.set(runOperationalDemoAtom, "guided");
            exit = yield* Effect.exit(
              AtomRegistry.getResult(registry, runOperationalDemoAtom, { suspendOnWaiting: true }),
            );
          }

          const state =
            mode === "simulator" ? registry.get(demoStateAtom) : registry.get(chatStateAtom);

          expect(state.events.slice(0, prefix.length)).toEqual(prefix);
          expect(state.output).toBeNull();
          expect(lifecycle.finalized).toBe(1);
          expect(Exit.isFailure(exit)).toBe(
            !["ended", "run-failed", "run-interrupted"].includes(ending),
          );
          if (ending === "defect" && Exit.isFailure(exit))
            expect(Cause.hasDies(exit.cause)).toBe(true);
          if (ending !== "defect")
            expect(state.status).toBe(ending === "run-interrupted" ? "interrupted" : "failed");
        }),
      );
    }
    for (const stop of ["interrupt", "dispose"] as const) {
      it.effect(`flushes received deltas and releases the RPC source on ${stop}`, () =>
        Effect.gen(function* () {
          const arrived = yield* Deferred.make<void>();

          const source = Stream.concat(
            Stream.fromIterable([started, ...deltas]),
            Stream.fromEffect(Deferred.succeed(arrived, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
            ),
          );

          const { registry, lifecycle } = yield* registryFor(source);

          if (mode === "general") {
            yield* AtomRegistry.mount(registry, runChatAtom);
            registry.set(runChatAtom, { mode: "deterministic", history: [], message: "Hello" });
          } else if (mode === "capability") {
            yield* AtomRegistry.mount(registry, runCapabilityChatAtom);
            registry.set(runCapabilityChatAtom, { scenario: "guided", message: "Hello" });
          } else {
            yield* AtomRegistry.mount(registry, runOperationalDemoAtom);
            registry.set(runOperationalDemoAtom, "guided");
          }
          if (mode === "general") registry.get(runChatAtom);
          else if (mode === "capability") registry.get(runCapabilityChatAtom);
          else registry.get(runOperationalDemoAtom);
          yield* Deferred.await(arrived);

          const stateAtom: Atom.Atom<{
            readonly status: string;
            readonly events: ReadonlyArray<DemoOperationalEvent>;
          }> = mode === "simulator" ? demoStateAtom : chatStateAtom;

          yield* AtomRegistry.toStream(registry, stateAtom).pipe(
            Stream.filter((state) => state.events.length === deltas.length + 1),
            Stream.take(1),
            Stream.runDrain,
          );
          const snapshot = registry.get(stateAtom);

          expect(snapshot.events).toEqual([started, ...deltas]);
          if (stop === "dispose") registry.dispose();
          else if (mode === "general") registry.set(runChatAtom, Atom.Interrupt);
          else if (mode === "capability") registry.set(runCapabilityChatAtom, Atom.Interrupt);
          else registry.set(runOperationalDemoAtom, Atom.Interrupt);
          yield* Effect.yieldNow;
          expect(lifecycle.finalized).toBe(1);
          expect(snapshot.events).toEqual([started, ...deltas]);
          if (stop === "interrupt") {
            const result =
              mode === "general"
                ? registry.get(runChatAtom)
                : mode === "capability"
                  ? registry.get(runCapabilityChatAtom)
                  : registry.get(runOperationalDemoAtom);

            expect(AsyncResult.isFailure(result)).toBe(true);
            if (AsyncResult.isFailure(result)) expect(Cause.hasInterrupts(result.cause)).toBe(true);
          }
        }),
      );
    }
  });
}

describe("delivered event batch boundaries", () => {
  it("bounds adjacent deltas and retains semantic boundaries without waiting", () => {
    const input = [started, ...deltas, tool];
    const batches = Array.from(eventBatches(input));

    expect(
      batches.map((batch) => (batch._tag === "Event" ? batch.event._tag : batch.events.length)),
    ).toEqual(["RunStarted", 64, 64, 2, "ToolCallDeclared"]);
    expect(
      batches.flatMap((batch) => (batch._tag === "Event" ? [batch.event] : batch.events)),
    ).toEqual(input);
    expect(Array.from(eventBatches(deltas.slice(0, 1)))).toHaveLength(1);
  });
});
