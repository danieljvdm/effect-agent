import {
  SubagentBudgetExhausted,
  type SubagentParentBudgetView,
  SubagentPrestartDenied,
  SubagentReservations,
  SubagentReservationsMemoryLive,
  type SubagentReservationView,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentPolicy,
  IdGenerator,
  type RunEvent,
  RunId,
  ToolCallId,
} from "@effect-agent/core";
import { AgentRuntime, AgentSpawner, ConversationHistory } from "@effect-agent/engine";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing";
import {
  AirportCode,
  CatalogLifecycle,
  coordinatorConfidentialMarker,
  coordinatorResearchTurn,
  coordinatorShortlistTurn,
  DestinationBrief,
  DestinationGuide,
  destinationLookup,
  DestinationRecommendation,
  DestinationReport,
  destinationReportFor,
  DestinationResearcher,
  DestinationResearcherToolkitLayer,
  destinationResearchDelegation,
  DestinationResearchFailed,
  destinationResearchHandlersLayer,
  DestinationResearchSupportLayer,
  DestinationShortlist,
  DeterministicIdGeneratorLayer,
  expectedDestinationShortlist,
  makeDestinationResearcherModel,
  missionConfidentialMarker,
  ResearchDispatchGate,
  researcherHappyPathTurns,
  researchMission,
  TravelCoordinator,
} from "@effect-agent/testing/fixtures/travel-planner";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import { Model, Tool, Toolkit } from "effect/unstable/ai";

const decodeRunId = Schema.decodeSync(RunId);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const decodeAirportCode = Schema.decodeSync(AirportCode);

const scriptedUsage = { inputTokens: { total: 96 }, outputTokens: { total: 64 } };

const makeCoordinator = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(
    TravelCoordinator,
    Model.make("scripted", "travel-coordinator-s1", ScriptedModel.layer(turns)),
  );

const TestSupportLayer = Layer.mergeAll(
  ConversationHistory.layerTransient,
  SubagentReservationsMemoryLive,
  CatalogLifecycle.layerNoDeps,
  DeterministicIdGeneratorLayer,
  ResearchDispatchGate.layerOpen,
);

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }
  return failure.value;
};

const findEvent = <Tag extends RunEvent["_tag"]>(
  events: ReadonlyArray<RunEvent>,
  tag: Tag,
): Extract<RunEvent, { readonly _tag: Tag }> | undefined =>
  events.find((event): event is Extract<RunEvent, { readonly _tag: Tag }> => event._tag === tag);

const subagentTags = (events: ReadonlyArray<RunEvent>): ReadonlyArray<string> =>
  events.map((event) => event._tag).filter((tag) => tag.startsWith("Subagent"));

const subagentLifecycleTags: ReadonlySet<string> = new Set([
  "SubagentRequested",
  "SubagentStarted",
  "SubagentProgress",
  "SubagentCompleted",
  "SubagentFailed",
  "SubagentInterrupted",
  "SubagentJoined",
]);

/** The ordered Subagent lifecycle observed for one delegation Tool Call. */
const subagentLifecycle = (
  events: ReadonlyArray<RunEvent>,
  toolCallId: string,
): ReadonlyArray<string> =>
  events.flatMap((event) =>
    subagentLifecycleTags.has(event._tag) &&
    "toolCallId" in event &&
    event.toolCallId === toolCallId
      ? [event._tag]
      : [],
  );

const dimensionKeys = [
  "turns",
  "toolCalls",
  "durationMillis",
  "inputTokens",
  "outputTokens",
  "costMicrousd",
  "resultBytes",
] as const;

/** Spec/subagents.md §7: at `released`, allocated = covered + released and observed = covered + overrun. */
const expectSettledOnce = (view: SubagentReservationView): void => {
  expect(view.status).toBe("released");
  for (const key of dimensionKeys) {
    expect(view.allocated[key]).toBe(view.coveredConsumed[key] + view.released[key]);
    expect(view.observedConsumed[key] ?? 0).toBe(view.coveredConsumed[key] + view.overrun[key]);
  }
};

const conservationDimensions = [
  ["maxTurns", "turns"],
  ["maxToolCalls", "toolCalls"],
  ["maxDurationMillis", "durationMillis"],
  ["maxInputTokens", "inputTokens"],
  ["maxOutputTokens", "outputTokens"],
  ["maxCostMicrousd", "costMicrousd"],
  ["maxResultBytes", "resultBytes"],
] as const;

/** Spec/subagents.md §7 equation: cap + cumulativeOverrun = available + open reservations + observed. */
const expectParentConservation = (snapshot: SubagentParentBudgetView): void => {
  for (const [capKey, amountKey] of conservationDimensions) {
    const cap = snapshot.caps[capKey];
    if (cap === undefined) {
      continue;
    }
    const available = snapshot.available[amountKey] ?? 0;
    const openReservations = snapshot.reservations
      .filter((view) => view.status !== "released")
      .reduce(
        (total, view) => total + view.allocated[amountKey] - view.coveredConsumed[amountKey],
        0,
      );
    const observed = snapshot.reservations.reduce(
      (total, view) => total + (view.observedConsumed[amountKey] ?? 0),
      0,
    );
    expect(cap + snapshot.cumulativeOverrun[amountKey]).toBe(
      available + openReservations + observed,
    );
  }
};

const lhrShortlist = DestinationShortlist.make({
  recommendations: [
    DestinationRecommendation.make({
      destination: destinationReportFor("LHR").destination,
      summary: destinationReportFor("LHR").advisory,
    }),
  ],
  nextAction: "review",
});

// ---------------------------------------------------------------------------
// Nested-delegation probe fixtures (test 5). `Subagent.define` already rejects
// a target whose Toolkit contains a delegation Tool, so the depth-one attempt
// is exercised through a hand-made spawning Tool that runs a mid-level travel
// desk whose Toolkit contains the delegation Tool.
// ---------------------------------------------------------------------------

const travelDeskDefinition = Agent.make("travel-desk", {
  input: DestinationBrief,
  output: DestinationReport,
  instructions: "Delegate the destination research, then answer as JSON.",
  toolkit: Toolkit.make(destinationResearchDelegation.tool),
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const encodedDeskBrief = Schema.encodeSync(DestinationBrief)(
  DestinationBrief.make({
    destination: decodeAirportCode("LHR"),
    focus: "nested-attempt",
  }),
);

const SpawnTravelDesk = Tool.make("spawn_travel_desk", {
  parameters: Schema.Struct({}),
  success: Schema.Struct({ denied: Schema.Boolean }),
})
  .addDependency(AgentSpawner)
  .addDependency(IdGenerator);
const spawnDeskToolkit = Toolkit.make(SpawnTravelDesk);

const nestedProbeDefinition = Agent.make("nested-delegation-probe", {
  input: Schema.Struct({}),
  output: Schema.Struct({ denied: Schema.Boolean }),
  instructions: "Spawn the travel desk, then answer as JSON.",
  toolkit: spawnDeskToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

describe("TEST-014 S1 Travel Planner Subagent delegation (E)", () => {
  it.effect(
    "commits parallel delegation results in declaration order under reverse child completion",
    () =>
      Effect.gen(function* () {
        const reservations = yield* SubagentReservations;
        const researcher = yield* makeDestinationResearcherModel(["LHR", "CDG"]);
        const childBinding = Agent.withModel(DestinationResearcher, researcher.model);
        let secondPrompt = "";
        const turns: ReadonlyArray<ScriptedTurnInput> = [
          coordinatorResearchTurn([
            { id: "research-lhr-1", destination: "LHR", focus: "museums" },
            { id: "research-cdg-1", destination: "CDG", focus: "galleries" },
          ]),
          {
            ...coordinatorShortlistTurn(expectedDestinationShortlist),
            assertRequest: (request) => {
              secondPrompt = JSON.stringify(request.prompt.content);
            },
          },
        ];
        const runId = decodeRunId("coordinator-run-happy");

        const detached = yield* AgentRuntime.start(makeCoordinator(turns), researchMission, {
          runId,
        }).pipe(
          Effect.provide(
            destinationResearchHandlersLayer(childBinding).pipe(
              Layer.provide(DestinationResearchSupportLayer),
            ),
          ),
        );
        const cdgCompleted = yield* Deferred.make<void>();
        yield* detached.observe.pipe(
          Stream.tap((event) =>
            event._tag === "SubagentCompleted" && event.toolCallId === "research-cdg-1"
              ? Deferred.succeed(cdgCompleted, undefined).pipe(Effect.asVoid)
              : Effect.void,
          ),
          Stream.runDrain,
          Effect.forkScoped,
        );
        yield* researcher.controls.awaitStarted("LHR");
        yield* researcher.controls.awaitStarted("CDG");
        // Release the children in reverse declaration order and require the
        // second call's completion to surface before the first is released.
        yield* researcher.controls.release("CDG");
        yield* Deferred.await(cdgCompleted);
        yield* researcher.controls.release("LHR");
        const result = yield* detached.await;
        const events = yield* detached.events;

        // The parent's final structured output decodes through its Schema.
        expect(result.output).toEqual(expectedDestinationShortlist);
        expect(result.turns).toBe(2);

        // Children completed in real (reverse) order...
        const completedCalls = events.flatMap((event) =>
          event._tag === "SubagentCompleted" ? [event.toolCallId] : [],
        );
        expect(completedCalls).toEqual(["research-cdg-1", "research-lhr-1"]);
        for (const toolCallId of ["research-lhr-1", "research-cdg-1"]) {
          expect(subagentLifecycle(events, toolCallId)).toEqual([
            "SubagentRequested",
            "SubagentStarted",
            "SubagentCompleted",
            "SubagentJoined",
          ]);
        }

        // ...while parent Tool results committed in declaration order
        // (SUB-013): the next model request materializes the first call's
        // finding before the second call's finding.
        expect(secondPrompt).toContain("London favors");
        expect(secondPrompt).toContain("Paris rewards");
        expect(secondPrompt.indexOf("London favors")).toBeLessThan(
          secondPrompt.indexOf("Paris rewards"),
        );

        const snapshot = yield* reservations.parentSnapshot(runId);
        expect(snapshot.totalChildInvocations).toBe(2);
        expect(snapshot.reservations).toHaveLength(2);
        for (const view of snapshot.reservations) {
          expectSettledOnce(view);
        }
        expectParentConservation(snapshot);
      }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("correlates the Subagent lifecycle events at depth one on the parent stream", () =>
    Effect.gen(function* () {
      const childBinding = Agent.withModel(
        DestinationResearcher,
        Model.make(
          "scripted",
          "destination-researcher-events",
          ScriptedModel.layer(researcherHappyPathTurns("LHR")),
        ),
      );
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        coordinatorResearchTurn([{ id: "research-lhr-1", destination: "LHR", focus: "museums" }]),
        coordinatorShortlistTurn(lhrShortlist),
      ];
      const runId = decodeRunId("coordinator-run-events");

      const detached = yield* AgentRuntime.start(makeCoordinator(turns), researchMission, {
        runId,
      }).pipe(
        Effect.provide(
          destinationResearchHandlersLayer(childBinding).pipe(
            Layer.provide(DestinationResearchSupportLayer),
          ),
        ),
      );
      const result = yield* detached.await;
      const events = yield* detached.events;

      expect(result.output).toEqual(lhrShortlist);
      expect(subagentTags(events)).toEqual([
        "SubagentRequested",
        "SubagentStarted",
        "SubagentCompleted",
        "SubagentJoined",
      ]);

      const requested = findEvent(events, "SubagentRequested");
      const started = findEvent(events, "SubagentStarted");
      const completed = findEvent(events, "SubagentCompleted");
      const joined = findEvent(events, "SubagentJoined");
      for (const event of [requested, started, completed, joined]) {
        // Base identity is the parent's; delegation lineage names the child.
        expect(event).toMatchObject({
          runId,
          conversationId: result.conversationId,
          agentId: "travel-coordinator",
          toolCallId: "research-lhr-1",
          delegationId: "delegate_destination_research",
          targetAgentId: "destination-researcher",
          depth: 1,
        });
        expect(event?.turnId).toBeDefined();
        expect(event?.childRunId).toBe(requested?.childRunId);
        expect(event?.childConversationId).toBe(requested?.childConversationId);
      }
      // The child owns fresh, distinct identity (SUB-004).
      expect(requested?.childRunId).not.toBe(runId);
      expect(requested?.childConversationId).not.toBe(result.conversationId);
      expect(completed).toMatchObject({ turns: 2 });

      // The parent Tool result is the projected, Schema-encoded finding.
      expect(findEvent(events, "ToolCallSucceeded")).toMatchObject({
        toolName: "delegate_destination_research",
        result: {
          destination: "LHR",
          summary: "London favors museum mornings and riverside evenings.",
        },
      });
    }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("total-maps an expected child failure and interrupts the attached sibling", () =>
    Effect.gen(function* () {
      const lifecycle = yield* CatalogLifecycle;
      const reservations = yield* SubagentReservations;
      const researcher = yield* makeDestinationResearcherModel(["LHR", "ZZZ"]);
      const childBinding = Agent.withModel(DestinationResearcher, researcher.model);
      // Deterministic sibling choreography: the doomed lookup waits until the
      // sibling's SubagentStarted event has surfaced on the parent stream, so
      // the batch failure always interrupts a running attached sibling.
      const lhrStartedEvent = yield* Deferred.make<void>();
      const gatedGuideLayer = Layer.succeed(
        DestinationGuide,
        DestinationGuide.of({
          lookup: (query) =>
            query.destination === "ZZZ"
              ? Deferred.await(lhrStartedEvent).pipe(Effect.andThen(destinationLookup(query)))
              : destinationLookup(query),
        }),
      );
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        coordinatorResearchTurn([
          { id: "research-lhr-1", destination: "LHR", focus: "museums" },
          { id: "research-zzz-1", destination: "ZZZ", focus: "fog" },
        ]),
      ];
      const runId = decodeRunId("coordinator-run-child-failure");

      const detached = yield* AgentRuntime.start(makeCoordinator(turns), researchMission, {
        runId,
      }).pipe(
        Effect.provide(
          destinationResearchHandlersLayer(childBinding).pipe(
            Layer.provide(Layer.mergeAll(DestinationResearcherToolkitLayer, gatedGuideLayer)),
          ),
        ),
      );
      yield* detached.observe.pipe(
        Stream.tap((event) =>
          event._tag === "SubagentStarted" && event.toolCallId === "research-lhr-1"
            ? Deferred.succeed(lhrStartedEvent, undefined).pipe(Effect.asVoid)
            : Effect.void,
        ),
        Stream.runDrain,
        Effect.forkScoped,
      );
      const exit = yield* Effect.exit(detached.await);
      const events = yield* detached.events;

      // The child's expected failure is total-mapped to the declared Tool
      // failure (SUB-028) and fails the parent batch.
      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(DestinationResearchFailed);
      expect(failure).toMatchObject({ childErrorTag: "DestinationGuideUnavailable" });

      expect(subagentLifecycle(events, "research-zzz-1")).toEqual([
        "SubagentRequested",
        "SubagentStarted",
        "SubagentFailed",
      ]);
      expect(findEvent(events, "SubagentFailed")).toMatchObject({
        toolCallId: "research-zzz-1",
        errorTag: "DestinationGuideUnavailable",
        depth: 1,
      });
      // The still-running sibling was interrupted, not failed or joined.
      expect(subagentLifecycle(events, "research-lhr-1")).toEqual([
        "SubagentRequested",
        "SubagentStarted",
        "SubagentInterrupted",
      ]);
      expect(findEvent(events, "SubagentJoined")).toBeUndefined();

      // Both child model scopes finalized before the parent Run settled: the
      // interrupted sibling's finalizer ran (SUB-011).
      expect(yield* lifecycle.counts).toEqual({ acquired: 2, finalized: 2 });

      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(2);
      expect(snapshot.reservations).toHaveLength(2);
      for (const view of snapshot.reservations) {
        expectSettledOnce(view);
      }
      expectParentConservation(snapshot);
    }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("interrupting the parent mid-child finalizes children and settles reservations", () =>
    Effect.gen(function* () {
      const lifecycle = yield* CatalogLifecycle;
      const reservations = yield* SubagentReservations;
      const researcher = yield* makeDestinationResearcherModel(["LHR", "CDG"]);
      const childBinding = Agent.withModel(DestinationResearcher, researcher.model);
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        coordinatorResearchTurn([
          { id: "research-lhr-1", destination: "LHR", focus: "museums" },
          { id: "research-cdg-1", destination: "CDG", focus: "galleries" },
        ]),
      ];
      const runId = decodeRunId("coordinator-run-interrupted");

      const fiber = yield* AgentRuntime.run(makeCoordinator(turns), researchMission, {
        runId,
      }).pipe(
        Effect.provide(
          destinationResearchHandlersLayer(childBinding).pipe(
            Layer.provide(DestinationResearchSupportLayer),
          ),
        ),
        Effect.scoped,
        Effect.forkChild,
      );
      yield* researcher.controls.awaitStarted("LHR");
      yield* researcher.controls.awaitStarted("CDG");
      // The guide Layer plus one model Layer per attached child are live.
      expect(yield* lifecycle.counts).toEqual({ acquired: 3, finalized: 0 });

      yield* Fiber.interrupt(fiber);

      // Interruption reached both children and every finalizer ran before the
      // interrupt returned; no child fiber outlived the parent Scope
      // (SUB-011/012).
      expect(yield* lifecycle.counts).toEqual({ acquired: 3, finalized: 3 });

      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(2);
      expect(snapshot.reservations).toHaveLength(2);
      for (const view of snapshot.reservations) {
        expectSettledOnce(view);
        // The wall clock never advanced under TestClock, so the honest
        // duration observation is zero and the full allocation returns.
        expect(view.observedConsumed.durationMillis).toBe(0);
        expect(view.released.durationMillis).toBe(10_000);
      }
      expectParentConservation(snapshot);
    }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("rejects the child's nested delegation attempt at preflight (SUB-029)", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      const grandchildBinding = Agent.withModel(
        DestinationResearcher,
        Model.make("scripted", "grandchild-unused", ScriptedModel.layer([])),
      );
      // The travel desk runs at depth one with the delegation Tool in its own
      // Toolkit; its delegation attempt must be denied before any reservation
      // or child identity exists.
      const travelDeskBinding = Agent.withModel(
        travelDeskDefinition,
        Model.make(
          "scripted",
          "travel-desk-scripted",
          ScriptedModel.layer([
            coordinatorResearchTurn([
              { id: "nested-research-1", destination: "LHR", focus: "nested" },
            ]),
          ]),
        ),
      );
      const dependencies = yield* Effect.context<
        SubagentReservations | IdGenerator | CatalogLifecycle | ResearchDispatchGate
      >();
      const deskDelegationLayer = destinationResearchHandlersLayer(grandchildBinding).pipe(
        Layer.provide(
          Layer.mergeAll(
            DestinationResearchSupportLayer.pipe(Layer.provide(Layer.succeedContext(dependencies))),
            Layer.succeedContext(dependencies),
          ),
        ),
      );

      const capturedDenial = yield* Ref.make<unknown>(undefined);
      const capturedDeskRunId = yield* Ref.make<RunId | undefined>(undefined);
      const spawnDeskLayer = spawnDeskToolkit.toLayer({
        spawn_travel_desk: () =>
          Effect.gen(function* () {
            const spawner = yield* AgentSpawner;
            const desk = yield* spawner.spawn(travelDeskBinding, encodedDeskBrief, {
              delegationId: destinationResearchDelegation.delegationId,
              parentToolCallId: decodeToolCallId("desk-call-1"),
            });
            yield* Ref.set(capturedDeskRunId, desk.runId);
            const exit = yield* Effect.exit(desk.await);
            if (Exit.isFailure(exit)) {
              yield* Ref.set(
                capturedDenial,
                Option.getOrUndefined(Cause.findErrorOption(exit.cause)),
              );
            }
            return { denied: Exit.isFailure(exit) };
          }).pipe(Effect.provide(deskDelegationLayer), Effect.scoped),
      });
      const probe = Agent.withModel(
        nestedProbeDefinition,
        Model.make(
          "scripted",
          "nested-probe-scripted",
          ScriptedModel.layer([
            {
              _tag: "Stream",
              parts: [
                { type: "tool-call", id: "spawn-desk-1", name: "spawn_travel_desk", params: {} },
                { type: "finish", reason: "tool-calls", usage: scriptedUsage },
              ],
              termination: { _tag: "Complete" },
            },
            {
              _tag: "Stream",
              parts: [
                { type: "text-start", id: "answer" },
                { type: "text-delta", id: "answer", delta: '{"denied":true}' },
                { type: "text-end", id: "answer" },
                { type: "finish", reason: "stop", usage: scriptedUsage },
              ],
              termination: { _tag: "Complete" },
            },
          ]),
        ),
      );

      const result = yield* AgentRuntime.run(probe, {}).pipe(Effect.provide(spawnDeskLayer));
      expect(result.output).toEqual({ denied: true });

      const denial = yield* Ref.get(capturedDenial);
      expect(denial).toBeInstanceOf(SubagentPrestartDenied);
      expect(denial).toMatchObject({
        reason: "nested-delegation",
        delegationId: "delegate_destination_research",
        targetAgentId: "destination-researcher",
      });
      if (denial instanceof SubagentPrestartDenied) {
        expect(denial.message).toContain("depth 1");
      }

      // Fail-closed preflight ran before any reservation: the desk Run never
      // registered a delegation budget.
      const deskRunId = yield* Ref.get(capturedDeskRunId);
      if (deskRunId === undefined) {
        throw new Error("The travel desk child Run was never spawned");
      }
      const snapshotExit = yield* Effect.exit(reservations.parentSnapshot(deskRunId));
      expect(failureFrom(snapshotExit)._tag).toBe("SubagentParentBudgetUnknown");
    }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("denies a third delegation over maxChildren while two run and conserves budget", () =>
    Effect.gen(function* () {
      const reservations = yield* SubagentReservations;
      const researcher = yield* makeDestinationResearcherModel(["LHR", "CDG"]);
      const childBinding = Agent.withModel(DestinationResearcher, researcher.model);
      // Hold only the third preflight so the denial deterministically happens
      // while both admitted children are still running.
      const amsDispatch = yield* Deferred.make<void>();
      const dispatchLayer = Layer.succeed(
        ResearchDispatchGate,
        ResearchDispatchGate.of({
          awaitDispatch: (destination) =>
            destination === "AMS" ? Deferred.await(amsDispatch) : Effect.void,
        }),
      );
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        coordinatorResearchTurn([
          { id: "research-lhr-1", destination: "LHR", focus: "museums" },
          { id: "research-cdg-1", destination: "CDG", focus: "galleries" },
          { id: "research-ams-1", destination: "AMS", focus: "canals" },
        ]),
      ];
      const runId = decodeRunId("coordinator-run-budget");

      const fiber = yield* AgentRuntime.run(makeCoordinator(turns), researchMission, {
        runId,
      }).pipe(
        Effect.provide(
          destinationResearchHandlersLayer(childBinding).pipe(
            Layer.provide(Layer.mergeAll(DestinationResearchSupportLayer, dispatchLayer)),
          ),
        ),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );
      yield* researcher.controls.awaitStarted("LHR");
      yield* researcher.controls.awaitStarted("CDG");
      yield* Deferred.succeed(amsDispatch, undefined);
      const exit = yield* Fiber.join(fiber);

      const failure = failureFrom(exit);
      expect(failure).toBeInstanceOf(SubagentBudgetExhausted);
      expect(failure).toMatchObject({
        dimension: "total-child-invocations",
        limitValue: 2,
        observedValue: 3,
      });

      // Conservation after the run: two settled reservations, no third.
      const snapshot = yield* reservations.parentSnapshot(runId);
      expect(snapshot.totalChildInvocations).toBe(2);
      expect(snapshot.reservations).toHaveLength(2);
      for (const view of snapshot.reservations) {
        expectSettledOnce(view);
      }
      expectParentConservation(snapshot);

      // Double release is impossible: releasing again changes nothing.
      const [view] = snapshot.reservations;
      if (view === undefined) {
        throw new Error("Expected a settled reservation view");
      }
      const releasedAgain = yield* reservations.release(view.reservationId);
      expect(releasedAgain.released).toEqual(view.released);
      const after = yield* reservations.parentSnapshot(runId);
      expect(after.available).toEqual(snapshot.available);
    }).pipe(Effect.provide(TestSupportLayer)),
  );

  it.effect("isolates the child prompt from the parent transcript and vice versa", () =>
    Effect.gen(function* () {
      let childPromptChecks = 0;
      const childTurns = researcherHappyPathTurns("LHR").map(
        (turn): ScriptedTurnInput => ({
          ...turn,
          assertRequest: (request) => {
            const prompt = JSON.stringify(request.prompt.content);
            // The child sees exactly the projected input (SUB-006)...
            expect(prompt).toContain("LHR");
            expect(prompt).toContain("research:museums");
            // ...and never the coordinator's transcript or instructions.
            expect(prompt).not.toContain(missionConfidentialMarker);
            expect(prompt).not.toContain(coordinatorConfidentialMarker);
            childPromptChecks += 1;
          },
        }),
      );
      const childBinding = Agent.withModel(
        DestinationResearcher,
        Model.make("scripted", "destination-researcher-isolated", ScriptedModel.layer(childTurns)),
      );
      let parentSecondPrompt = "";
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        coordinatorResearchTurn([{ id: "research-lhr-1", destination: "LHR", focus: "museums" }]),
        {
          ...coordinatorShortlistTurn(lhrShortlist),
          assertRequest: (request) => {
            parentSecondPrompt = JSON.stringify(request.prompt.content);
          },
        },
      ];

      const result = yield* AgentRuntime.run(makeCoordinator(turns), researchMission, {
        runId: decodeRunId("coordinator-run-isolation"),
      }).pipe(
        Effect.provide(
          destinationResearchHandlersLayer(childBinding).pipe(
            Layer.provide(DestinationResearchSupportLayer),
          ),
        ),
      );

      expect(result.output).toEqual(lhrShortlist);
      expect(childPromptChecks).toBe(2);
      // Only the projected finding crossed back to the parent: the child's
      // guide highlights and report internals stay in the child Conversation
      // (SUB-015).
      expect(parentSecondPrompt).toContain("London favors museum mornings");
      expect(parentSecondPrompt).not.toContain("Barbican brutalism walk");
      expect(parentSecondPrompt).not.toContain("research:museums");
    }).pipe(Effect.provide(TestSupportLayer)),
  );
});
