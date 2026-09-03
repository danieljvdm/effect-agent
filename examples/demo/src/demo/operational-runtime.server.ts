import "@tanstack/react-start/server-only";
import {
  type ApprovalAudit,
  ApprovalApproved,
  type ApprovalAuditLimitExceeded,
  type ApprovalDecision,
  type ApprovalDecisionMismatch,
  ApprovalDenied,
  ApprovalResolver,
  type ApprovalResolverError,
  ApprovalAuditMemoryLive,
} from "@effect-agent/capabilities/Approval";
import {
  BudgetExceeded,
  makeUsageBudget,
  UsageBudgetLimits,
} from "@effect-agent/capabilities/Budget";
import {
  FollowUpCommand,
  makeRunCommandQueue,
  RunCommandQueueConfig,
  SteeringCommand,
} from "@effect-agent/capabilities/Commands";
import {
  EphemeralThreads,
  EphemeralThreadsLive,
} from "@effect-agent/capabilities/EphemeralThreads";
import {
  connectMcp,
  McpConnectionRequest,
  McpConnector,
  McpServerIdentity,
} from "@effect-agent/capabilities/Mcp";
import {
  type Redactor,
  type RedactionError,
  StructuralRedactorLive,
} from "@effect-agent/capabilities/Redaction";
import {
  type ApprovalAdapterError,
  type BudgetAdapterError,
  type ThreadAdapterError,
  toRunApprovalHook,
  toRunBudgetHook,
  toRunThreadOptions,
} from "@effect-agent/capabilities/RunHooks";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type RunEvent } from "@effect-agent/core/RunEvent";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import {
  RunContextPreparationPassthrough,
  type RunInputHook,
  type RunOptions,
} from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { layer as LocalSandboxLayer } from "@effect-agent/sandbox-local/LocalSandbox";
import {
  NetworkDisabled,
  Sandbox,
  SandboxEnvironment,
  SandboxLimits,
  SandboxRequest,
  SandboxRuntime,
} from "@effect-agent/sandbox/Sandbox";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing/ScriptedModel";
import {
  ActivityCatalog,
  ActivitySearchResult,
  expectedTravelPlan,
  FlightCatalog,
  FlightOption,
  ItineraryHold,
  ItineraryHoldGateway,
  LodgingCatalog,
  LodgingOption,
  QuoteId,
  TravelGuidanceLayer,
  TravelPlan,
  TravelPlannerPhase2,
  TravelPlannerPhase2Toolkit,
  TripRequest,
  phase1HappyPathTurns,
  phase1Trip,
} from "@effect-agent/testing/TravelPlanner";
import { OpenAiClient } from "@effect/ai-openai";
import { NodeCrypto } from "@effect/platform-node";
import {
  Cause,
  Clock,
  Context,
  Crypto,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Layer,
  Queue,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import { Model, Prompt, Tool, Toolkit } from "effect/unstable/ai";
import * as McpSchema from "effect/unstable/ai/McpSchema";

import { toDemoRunFailure } from "./error-details";
import { makeRealTravelPlannerAgent, RealTravelHoldToolkit } from "./openai-profile";
import {
  DemoApprovalPending,
  DemoApprovalSettled,
  DemoBudgetChanged,
  DemoBudgetRejected,
  DemoCommandStateChanged,
  type DemoCommandKind,
  DemoContextPrepared,
  DemoControlAccepted,
  DemoControlFailure,
  DemoHoldHandlerState,
  DemoMcpConnected,
  type DemoOperationalEvent,
  DemoRunFailure,
  DemoRunHandle,
  DemoRunOpened,
  type DemoScenario,
  DemoSandboxObserved,
  DemoToolBatchCommitted,
  QueueRunCommandRequest,
  type ResolveRunApprovalRequest,
  type StartLiveTravelChatRequest,
  type StartOperationalRunRequest,
} from "./operational-contracts";

const guidedSteering = "Change the departure date to 2026-09-21.";
const guidedFollowUp = "Prefer a quiet room away from the lift.";

const usage = {
  inputTokens: { total: 128 },
  outputTokens: { total: 96 },
};

const expectedItinerary = expectedTravelPlan.itineraries[0];

if (expectedItinerary === undefined) {
  throw new Error("The deterministic Travel Planner fixture must contain one itinerary.");
}

const redirectedPlan = Schema.decodeSync(TravelPlan)({
  itineraries: [
    {
      title: expectedItinerary.title,
      route: expectedItinerary.route,
      dates: "21–26 September 2026",
      flight: expectedItinerary.flight,
      lodging: expectedItinerary.lodging,
      activities: expectedItinerary.activities,
      estimatedTotalCents: expectedItinerary.estimatedTotalCents,
      currency: expectedItinerary.currency,
      quoteId: expectedItinerary.quoteId,
      assumptions: [
        "Two travelers sharing one quiet studio away from the lift",
        "The September 21 date change was applied only after the first Tool batch completed",
        "Quote is read-only availability, not a reservation",
      ],
      unresolvedConstraints: expectedItinerary.unresolvedConstraints,
      nextAction: expectedItinerary.nextAction,
    },
  ],
});

const finalPlanTurn: ScriptedTurnInput = {
  _tag: "Stream",
  parts: [
    { type: "text-start", id: "final-itinerary" },
    {
      type: "text-delta",
      id: "final-itinerary",
      delta: JSON.stringify(Schema.encodeSync(TravelPlan)(redirectedPlan)),
    },
    { type: "text-end", id: "final-itinerary" },
    { type: "finish", reason: "stop", usage },
  ],
  termination: { _tag: "Complete" },
};

const intermediateStopTurn: ScriptedTurnInput = {
  _tag: "Stream",
  parts: [
    { type: "text-start", id: "redirected-draft" },
    {
      type: "text-delta",
      id: "redirected-draft",
      delta: "The date change is now applied. The review-only draft is ready.",
    },
    { type: "text-end", id: "redirected-draft" },
    {
      type: "finish",
      reason: "stop",
      usage: { inputTokens: { total: 48 }, outputTokens: { total: 18 } },
    },
  ],
  termination: { _tag: "Complete" },
};

const holdTurn: ScriptedTurnInput = {
  _tag: "Stream",
  parts: [
    {
      type: "tool-call",
      id: "hold-call-1",
      name: "hold_itinerary",
      params: {
        quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
        expiresInMinutes: 15,
      },
    },
    {
      type: "finish",
      reason: "tool-calls",
      usage: { inputTokens: { total: 32 }, outputTokens: { total: 16 } },
    },
  ],
  termination: { _tag: "Complete" },
};

const McpDestinationBrief = Tool.make("destination_brief", {
  description: "Return a bounded fixture advisory for a destination.",
  parameters: Schema.Struct({ destination: Schema.NonEmptyString }),
  success: Schema.String,
});

const DemoMcpToolkit = Toolkit.make(McpDestinationBrief);

const demoMcpRequest = McpConnectionRequest.make({
  serverId: "demo-travel-mcp",
  maxToolCount: 4,
  maxToolDescriptionBytes: 512,
  maxDiscoveryBytes: 16_384,
  connectTimeoutMillis: 1_000,
});

const demoMcpIdentity = McpServerIdentity.make({
  serverId: demoMcpRequest.serverId,
  implementation: McpSchema.Implementation.make({
    name: "offline-travel-fixture",
    version: "1.0.0",
  }),
});

const DemoMcpConnectorLayer = Layer.succeed(McpConnector)({
  connect: () =>
    Effect.acquireRelease(
      Effect.succeed({
        identity: demoMcpIdentity,
        capabilities: McpSchema.ServerCapabilities.make({}),
        tools: [
          McpSchema.Tool.make({
            name: McpDestinationBrief.name,
            description: "Return a bounded fixture advisory for a destination.",
            inputSchema: Tool.getJsonSchema(McpDestinationBrief),
          }),
        ],
        toolkit: DemoMcpToolkit,
      }),
      () => Effect.void,
    ),
});

type SearchName = "search_flights" | "search_lodging" | "search_activities";

interface SearchGates {
  readonly started: Readonly<Record<SearchName, Deferred.Deferred<void>>>;
  readonly release: Readonly<Record<SearchName, Deferred.Deferred<void>>>;
  readonly observed: Readonly<Record<SearchName, Deferred.Deferred<void>>>;
}

class DemoRunControls extends Context.Service<
  DemoRunControls,
  {
    readonly gates: SearchGates;
    readonly emit: (event: DemoOperationalEvent) => Effect.Effect<void>;
    readonly eventBase: Effect.Effect<{
      readonly handle: DemoRunHandle;
      readonly emittedAt: DateTime.Utc;
    }>;
    readonly holdStarts: Ref.Ref<number>;
  }
>()("@effect-agent/example-demo/DemoRunControls") {}

const markAndAwait = (
  controls: DemoRunControls["Service"],
  name: SearchName,
): Effect.Effect<void> =>
  Deferred.succeed(controls.gates.started[name], undefined).pipe(
    Effect.andThen(Deferred.await(controls.gates.release[name])),
  );

const DemoTravelToolkitLayer = TravelPlannerPhase2Toolkit.toLayer({
  search_flights: (query) => Effect.flatMap(FlightCatalog, (catalog) => catalog.search(query)),
  search_lodging: (query) => Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(query)),
  search_activities: (query) => Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(query)),
  hold_itinerary: (request) =>
    Effect.flatMap(ItineraryHoldGateway, (gateway) => gateway.hold(request)),
});

/**
 * The real research agent's only application handler: web search executes
 * provider-side, so its Tool needs no Layer here.
 */
const RealTravelToolkitLayer = RealTravelHoldToolkit.toLayer({
  hold_itinerary: (request) =>
    Effect.flatMap(ItineraryHoldGateway, (gateway) => gateway.hold(request)),
});

const DemoHoldGatewayLayer = Layer.effect(
  ItineraryHoldGateway,
  Effect.gen(function* () {
    const controls = yield* DemoRunControls;

    return ItineraryHoldGateway.of({
      hold: (request) =>
        Effect.gen(function* () {
          const starts = yield* Ref.updateAndGet(controls.holdStarts, (count) => count + 1);

          yield* controls.emit(
            DemoHoldHandlerState.make({
              ...(yield* controls.eventBase),
              starts,
            }),
          );

          return ItineraryHold.make({
            holdId: "demo-hold-1",
            quoteId: request.quoteId,
            status: "held",
          });
        }),
    });
  }),
);

const controlledFlight = FlightOption.make({
  quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
  flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
  estimatedCents: 180_000,
  currency: "USD",
});

const controlledLodging = LodgingOption.make({
  lodging: "Bloomsbury House · refundable quiet studio · 4 nights",
  estimatedCents: 104_000,
  currency: "USD",
});

const controlledActivities = ActivitySearchResult.make({
  activities: ["British Museum timed entry", "Thames evening walk"],
});

const ControlledFlightCatalogLayer = Layer.effect(
  FlightCatalog,
  Effect.gen(function* () {
    const controls = yield* DemoRunControls;

    return FlightCatalog.of({
      search: () => markAndAwait(controls, "search_flights").pipe(Effect.as(controlledFlight)),
    });
  }),
);

const ControlledLodgingCatalogLayer = Layer.effect(
  LodgingCatalog,
  Effect.gen(function* () {
    const controls = yield* DemoRunControls;

    return LodgingCatalog.of({
      search: () => markAndAwait(controls, "search_lodging").pipe(Effect.as(controlledLodging)),
    });
  }),
);

const ControlledActivityCatalogLayer = Layer.effect(
  ActivityCatalog,
  Effect.gen(function* () {
    const controls = yield* DemoRunControls;

    return ActivityCatalog.of({
      search: () =>
        markAndAwait(controls, "search_activities").pipe(Effect.as(controlledActivities)),
    });
  }),
);

const ControlledCatalogLayers = Layer.mergeAll(
  ControlledFlightCatalogLayer,
  ControlledLodgingCatalogLayer,
  ControlledActivityCatalogLayer,
);

/**
 * The tool-defect scenario proves a dying Tool handler cannot strand the
 * browser: every search handler dies as a defect, so only the producer
 * boundary's full-Cause handling can terminalize the client stream.
 */
const defectiveSearch = Effect.die(new Error("The demo supplier catalog crashed while searching."));

const DefectiveCatalogLayers = Layer.mergeAll(
  Layer.succeed(FlightCatalog, FlightCatalog.of({ search: () => defectiveSearch })),
  Layer.succeed(LodgingCatalog, LodgingCatalog.of({ search: () => defectiveSearch })),
  Layer.succeed(ActivityCatalog, ActivityCatalog.of({ search: () => defectiveSearch })),
);

const nowUtc = Clock.currentTimeMillis.pipe(
  Effect.map((millis) => DateTime.toUtc(DateTime.makeUnsafe(millis))),
);

interface PendingApproval {
  readonly requestId: string;
  readonly deferred: Deferred.Deferred<ApprovalDecision>;
}

interface ActiveRun {
  readonly handle: DemoRunHandle;
  readonly runId: RunId;
  readonly threadId: ThreadId;
  readonly commandQueue: import("@effect-agent/capabilities/Commands").RunCommandQueue;
  readonly output: Queue.Queue<DemoOperationalEvent, DemoRunFailure | Cause.Done>;
  readonly pendingApprovals: Ref.Ref<ReadonlyMap<string, PendingApproval>>;
  readonly commandCounter: Ref.Ref<number>;
  readonly acceptingCommands: Ref.Ref<boolean>;
  readonly commandGate: Semaphore.Semaphore;
}

type DemoHookError =
  | ApprovalAdapterError
  | ApprovalAuditLimitExceeded
  | ApprovalDecisionMismatch
  | ApprovalResolverError
  | BudgetAdapterError
  | BudgetExceeded
  | ThreadAdapterError
  | RedactionError;

export class DemoInteractiveRuntime extends Context.Service<
  DemoInteractiveRuntime,
  {
    readonly start: (
      request: StartOperationalRunRequest,
    ) => Stream.Stream<DemoOperationalEvent, DemoRunFailure>;
    readonly startLiveTravel: (
      request: StartLiveTravelChatRequest,
    ) => Stream.Stream<DemoOperationalEvent, DemoRunFailure, OpenAiClient.OpenAiClient>;
    readonly queueCommand: (
      request: QueueRunCommandRequest,
    ) => Effect.Effect<DemoControlAccepted, DemoControlFailure>;
    readonly resolveApproval: (
      request: ResolveRunApprovalRequest,
    ) => Effect.Effect<DemoControlAccepted, DemoControlFailure>;
  }
>()("@effect-agent/example-demo/DemoInteractiveRuntime") {}

const findActive = (
  activeRuns: Ref.Ref<ReadonlyMap<DemoRunHandle, ActiveRun>>,
  handle: DemoRunHandle,
): Effect.Effect<ActiveRun, DemoControlFailure> =>
  Ref.get(activeRuns).pipe(
    Effect.flatMap((runs) => {
      const active = runs.get(handle);

      return active === undefined
        ? Effect.fail(
            DemoControlFailure.make({
              reason: "run-not-found",
              message: "The ephemeral Run is no longer active.",
            }),
          )
        : Effect.succeed(active);
    }),
  );

const makeGates = Effect.fn("Demo.makeGates")(function* (): Effect.fn.Return<SearchGates> {
  const flightStarted = yield* Deferred.make<void>();
  const lodgingStarted = yield* Deferred.make<void>();
  const activityStarted = yield* Deferred.make<void>();
  const releaseFlight = yield* Deferred.make<void>();
  const releaseLodging = yield* Deferred.make<void>();
  const releaseActivity = yield* Deferred.make<void>();
  const observedFlight = yield* Deferred.make<void>();
  const observedLodging = yield* Deferred.make<void>();
  const observedActivity = yield* Deferred.make<void>();

  return {
    started: {
      search_flights: flightStarted,
      search_lodging: lodgingStarted,
      search_activities: activityStarted,
    },
    release: {
      search_flights: releaseFlight,
      search_lodging: releaseLodging,
      search_activities: releaseActivity,
    },
    observed: {
      search_flights: observedFlight,
      search_lodging: observedLodging,
      search_activities: observedActivity,
    },
  };
});

const scenarioLimits = (scenario: DemoScenario): UsageBudgetLimits => {
  const ordinary = {
    maxInputTokens: 12_000,
    maxOutputTokens: 4_096,
    maxToolCalls: 8,
    maxCostMicrousd: 10_000,
    maxDurationMillis: 30_000,
  };

  switch (scenario) {
    case "budget-tokens":
      return UsageBudgetLimits.make({ ...ordinary, maxInputTokens: 1 });
    case "budget-tools":
      return UsageBudgetLimits.make({ ...ordinary, maxToolCalls: 1 });
    case "budget-cost":
      return UsageBudgetLimits.make({ ...ordinary, maxCostMicrousd: 100 });
    case "budget-duration":
      return UsageBudgetLimits.make({ ...ordinary, maxDurationMillis: 10 });
    default:
      return UsageBudgetLimits.make(ordinary);
  }
};

/**
 * Real web research is token-heavy: hosted search results routinely exceed the
 * fixture profile's 12k input budget in one turn. The live profile keeps every
 * bound finite but sized for genuine research, with cost as the safety net.
 */
const liveResearchLimits = UsageBudgetLimits.make({
  maxInputTokens: 400_000,
  maxOutputTokens: 16_000,
  maxToolCalls: 10,
  maxCostMicrousd: 2_000_000,
  maxDurationMillis: 180_000,
});

const operationalPlanAgent = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(
    TravelPlannerPhase2,
    Model.make("scripted", "travel-planner-phase-2-demo", ScriptedModel.layer(turns)),
  );

const makeIdLayer = (threadId: ThreadId, runId: RunId): Layer.Layer<IdGenerator> =>
  Layer.effect(
    IdGenerator,
    Effect.gen(function* () {
      const turn = yield* Ref.make(0);

      return IdGenerator.of({
        nextThreadId: Effect.succeed(threadId),
        nextRunId: Effect.succeed(runId),
        nextTurnId: Ref.updateAndGet(turn, (value) => value + 1).pipe(
          Effect.map((value) => Schema.decodeSync(TurnId)(`${runId}-turn-${value}`)),
        ),
      });
    }),
  );

const InteractiveRuntimeLive = Layer.effect(
  DemoInteractiveRuntime,
  Effect.gen(function* () {
    const threads = yield* EphemeralThreads;
    const sandbox = yield* Sandbox;
    const connector = yield* McpConnector;
    const crypto = yield* Crypto.Crypto;
    const activeRuns = yield* Ref.make<ReadonlyMap<DemoRunHandle, ActiveRun>>(new Map());
    const identityCounter = yield* Ref.make(0);

    const queueCommand = Effect.fn("Demo.queueCommand")(function* (
      request: QueueRunCommandRequest,
    ) {
      const active = yield* findActive(activeRuns, request.handle);

      return yield* active.commandGate.withPermit(
        Effect.gen(function* () {
          if (!(yield* Ref.get(active.acceptingCommands))) {
            return yield* DemoControlFailure.make({
              reason: "run-closed",
              message: "The Run has already chosen a terminal outcome.",
            });
          }
          const sequence = yield* Ref.updateAndGet(active.commandCounter, (value) => value + 1);
          const commandId = `${active.runId}-command-${sequence}`;
          const createdAt = yield* nowUtc;

          const command =
            request.kind === "steering"
              ? SteeringCommand.make({
                  id: commandId,
                  runId: active.runId,
                  threadId: active.threadId,
                  author: "traveler",
                  content: request.content,
                  createdAt,
                })
              : FollowUpCommand.make({
                  id: commandId,
                  runId: active.runId,
                  threadId: active.threadId,
                  author: "traveler",
                  content: request.content,
                  createdAt,
                });

          yield* active.commandQueue.offer(command).pipe(
            Effect.mapError(() =>
              DemoControlFailure.make({
                reason: "run-closed",
                message: "The command queue closed before the input could be admitted.",
              }),
            ),
          );
          yield* Queue.offer(
            active.output,
            DemoCommandStateChanged.make({
              handle: active.handle,
              emittedAt: createdAt,
              commandId,
              kind: request.kind,
              content: request.content,
              status: "queued",
              deliverySeam: request.kind === "steering" ? "after-tool-batch" : "otherwise-stop",
            }),
          ).pipe(Effect.asVoid);

          return DemoControlAccepted.make({ accepted: true });
        }),
      );
    });

    const resolveApproval = Effect.fn("Demo.resolveApproval")(function* (
      request: ResolveRunApprovalRequest,
    ) {
      const active = yield* findActive(activeRuns, request.handle);

      const pending = yield* Ref.modify(active.pendingApprovals, (approvals) => {
        const current = approvals.get(request.requestId);

        if (current === undefined) {
          return [undefined, approvals] as const;
        }
        const next = new Map(approvals);

        next.delete(request.requestId);

        return [current, next] as const;
      });

      if (pending === undefined) {
        return yield* DemoControlFailure.make({
          reason: "approval-not-found",
          message: "The approval request is no longer pending.",
        });
      }
      const decidedAt = yield* nowUtc;

      const decision =
        request.choice === "approve"
          ? ApprovalApproved.make({
              requestId: request.requestId,
              decidedAt,
              resolver: "effect-agent.demo-user",
            })
          : ApprovalDenied.make({
              requestId: request.requestId,
              decidedAt,
              resolver: "effect-agent.demo-user",
              reason: "The traveler denied the itinerary hold.",
              timedOut: false,
            });

      const completed = yield* Deferred.succeed(pending.deferred, decision);

      if (!completed) {
        return yield* DemoControlFailure.make({
          reason: "approval-already-decided",
          message: "This approval request already has a decision.",
        });
      }
      yield* Queue.offer(
        active.output,
        DemoApprovalSettled.make({
          handle: active.handle,
          emittedAt: decidedAt,
          requestId: request.requestId,
          choice: request.choice,
        }),
      );

      return DemoControlAccepted.make({ accepted: true });
    });

    const startWithProfile = (
      request: StartOperationalRunRequest | StartLiveTravelChatRequest,
      profile: "scripted" | "openai",
      liveClient: OpenAiClient.Service | null,
    ): Stream.Stream<DemoOperationalEvent, DemoRunFailure> =>
      Stream.unwrap(
        Effect.gen(function* () {
          const live = profile === "openai";
          const identity = yield* Ref.updateAndGet(identityCounter, (value) => value + 1);

          const handle = yield* Schema.decodeUnknownEffect(DemoRunHandle)(
            `demo-handle-${identity}`,
          );

          const runId = yield* Schema.decodeUnknownEffect(RunId)(`demo-run-${identity}`);

          const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(
            live ? "demo-thread-live-travel" : "demo-thread-phase-2",
          );

          yield* threads.create(threadId);

          const output = yield* Queue.bounded<DemoOperationalEvent, DemoRunFailure | Cause.Done>(
            256,
          );

          const commandQueue = yield* makeRunCommandQueue(
            runId,
            RunCommandQueueConfig.make({ capacity: 8 }),
          );

          const pendingApprovals = yield* Ref.make<ReadonlyMap<string, PendingApproval>>(new Map());
          const commandCounter = yield* Ref.make(0);
          const acceptingCommands = yield* Ref.make(true);
          const commandGate = yield* Semaphore.make(1);

          const active: ActiveRun = {
            handle,
            runId,
            threadId,
            commandQueue,
            output,
            pendingApprovals,
            commandCounter,
            acceptingCommands,
            commandGate,
          };

          const registered = yield* Ref.modify(activeRuns, (current) =>
            current.size > 0 ? [false, current] : [true, new Map(current).set(handle, active)],
          );

          if (!registered) {
            return yield* DemoRunFailure.make({
              errorTag: "DemoRunAlreadyActive",
              message: "Finish or stop the current ephemeral Run before starting another.",
            });
          }

          const cleanedUp = yield* Ref.make(false);

          const cleanup = Effect.gen(function* () {
            const alreadyCleaned = yield* Ref.getAndSet(cleanedUp, true);

            if (alreadyCleaned) {
              return;
            }
            yield* Ref.set(acceptingCommands, false);
            yield* Ref.update(activeRuns, (current) => {
              const next = new Map(current);

              next.delete(handle);

              return next;
            });
            const approvals = yield* Ref.getAndSet(pendingApprovals, new Map());

            yield* Effect.forEach(approvals.values(), (pending) =>
              Deferred.interrupt(pending.deferred),
            );
            yield* commandQueue.shutdown;
          }).pipe(Effect.asVoid);

          yield* Effect.addFinalizer(() => cleanup);

          const eventBase = Effect.gen(function* () {
            return {
              handle,
              emittedAt: yield* nowUtc,
            } as const;
          });

          const emit = (event: DemoOperationalEvent): Effect.Effect<void> =>
            Queue.offer(output, event).pipe(Effect.asVoid);

          const gates = yield* makeGates();
          const holdStarts = yield* Ref.make(0);
          const controls = DemoRunControls.of({ gates, emit, eventBase, holdStarts });

          yield* emit(
            DemoRunOpened.make({
              ...(yield* eventBase),
              runId,
              threadId,
              scenario: request.scenario,
              executionClass: "ephemeral",
              schedulerConcurrency: 3,
            }),
          );

          const limits = live ? liveResearchLimits : scenarioLimits(request.scenario);
          const budget = yield* makeUsageBudget(limits);

          yield* emit(
            DemoBudgetChanged.make({
              ...(yield* eventBase),
              scopeLevel: budget.level,
              scopeId: budget.id,
              limits,
              totals: yield* budget.snapshot,
            }),
          );
          yield* emit(
            DemoHoldHandlerState.make({
              ...(yield* eventBase),
              starts: 0,
            }),
          );

          const baseBudgetHook = toRunBudgetHook(budget);

          const budgetHook = {
            guard: baseBudgetHook.guard,
            consume: (delta: Parameters<typeof baseBudgetHook.consume>[0]) =>
              baseBudgetHook.consume(delta).pipe(
                Effect.tap(() =>
                  Effect.gen(function* () {
                    yield* emit(
                      DemoBudgetChanged.make({
                        ...(yield* eventBase),
                        scopeLevel: budget.level,
                        scopeId: budget.id,
                        limits,
                        totals: yield* budget.snapshot,
                      }),
                    );
                  }),
                ),
              ),
          };

          const claimedCommands = new Map<string, DemoCommandKind>();

          const inputHook: RunInputHook = {
            drain: (policy) =>
              commandGate.withPermit(
                commandQueue.drain(policy).pipe(
                  Effect.tap((commands) =>
                    Effect.forEach(commands, (command) => {
                      const kind = command._tag === "SteeringCommand" ? "steering" : "follow-up";

                      claimedCommands.set(command.id, kind);

                      return eventBase.pipe(
                        Effect.flatMap((base) =>
                          emit(
                            DemoCommandStateChanged.make({
                              ...base,
                              commandId: command.id,
                              kind,
                              content: command.content,
                              status: "claimed",
                              deliverySeam:
                                kind === "steering" ? "after-tool-batch" : "otherwise-stop",
                            }),
                          ),
                        ),
                      );
                    }),
                  ),
                  Effect.map((commands) =>
                    commands.map((command) => ({
                      kind:
                        command._tag === "SteeringCommand"
                          ? ("steering" as const)
                          : ("follow-up" as const),
                      input: command.content,
                    })),
                  ),
                  Effect.tap((commands) =>
                    Effect.forEach(commands, (command) => {
                      const source = [...claimedCommands.entries()].find(
                        ([, kind]) => kind === command.kind,
                      );

                      if (source === undefined) return Effect.void;
                      claimedCommands.delete(source[0]);

                      return eventBase.pipe(
                        Effect.flatMap((base) =>
                          emit(
                            DemoCommandStateChanged.make({
                              ...base,
                              commandId: source[0],
                              kind: command.kind,
                              content: command.input,
                              status: "delivered",
                              deliverySeam:
                                command.kind === "steering" ? "after-tool-batch" : "otherwise-stop",
                            }),
                          ),
                        ),
                      );
                    }),
                  ),
                ),
              ),
            end: () => commandQueue.shutdown,
          };

          // Interactive snapshots include incremental updates even if execution later fails.
          // Successful-run retention through ThreadHistory would change that behavior.
          const threadOptions = yield* toRunThreadOptions(threads, threadId, runId);

          const contextHook = {
            prepare: ({
              source,
              turn,
            }: {
              readonly source: Prompt.Prompt;
              readonly turn: number;
            }) =>
              Effect.gen(function* () {
                const sourceText = JSON.stringify(source.content);
                const compacted = !live && turn > 1;

                const summary = compacted
                  ? [
                      "Compacted Travel Planner context.",
                      sourceText.includes("2026-09-21")
                        ? "Traveler changed departure to 2026-09-21."
                        : "Original departure remains active.",
                      sourceText.includes("quiet room")
                        ? "Traveler prefers a quiet room away from the lift."
                        : "No room-location preference is active.",
                      "The complete prior Tool batch is retained in official ephemeral history.",
                      "Quote reference: quote-sfo-lhr-001.",
                    ].join(" ")
                  : "No compaction needed for the first model request.";

                const prompt = compacted ? Prompt.make(summary) : source;

                yield* emit(
                  DemoContextPrepared.make({
                    ...(yield* eventBase),
                    turn,
                    officialMessageCount: source.content.length,
                    modelMessageCount: prompt.content.length,
                    compacted,
                    summary,
                  }),
                );

                return { prompt };
              }),
          };

          const guidedTurns: ReadonlyArray<ScriptedTurnInput> = [
            phase1HappyPathTurns[0],
            intermediateStopTurn,
            finalPlanTurn,
          ];

          const durationTurn: ScriptedTurnInput = {
            ...phase1HappyPathTurns[0],
            onStreamStart: Effect.sleep("50 millis"),
          };

          const turns =
            request.scenario === "hold"
              ? ([holdTurn, finalPlanTurn] satisfies ReadonlyArray<ScriptedTurnInput>)
              : request.scenario === "budget-duration"
                ? ([durationTurn] satisfies ReadonlyArray<ScriptedTurnInput>)
                : request.scenario.startsWith("budget-") || request.scenario === "tool-defect"
                  ? ([phase1HappyPathTurns[0]] satisfies ReadonlyArray<ScriptedTurnInput>)
                  : guidedTurns;

          const approvalResolver = Layer.succeed(ApprovalResolver)({
            request: (approvalRequest) =>
              Effect.gen(function* () {
                const deferred = yield* Deferred.make<ApprovalDecision>();

                yield* Ref.update(pendingApprovals, (current) =>
                  new Map(current).set(approvalRequest.requestId, {
                    requestId: approvalRequest.requestId,
                    deferred,
                  }),
                );
                yield* emit(
                  DemoApprovalPending.make({
                    ...(yield* eventBase),
                    request: approvalRequest,
                  }),
                );

                return yield* Deferred.await(deferred).pipe(
                  Effect.ensuring(
                    Ref.update(pendingApprovals, (current) => {
                      const next = new Map(current);

                      next.delete(approvalRequest.requestId);

                      return next;
                    }),
                  ),
                );
              }),
          });

          const approvalHook = toRunApprovalHook({
            expiresInMillis: 20_000,
            risk: "high",
            denial: "terminal",
            actionSummary: () => "Place a temporary 15-minute hold on the selected itinerary.",
            resourceTargets: () => ["quote:quote-sfo-lhr-001"],
          });

          const controlsLayer = Layer.succeed(DemoRunControls)(controls);

          const commonRuntimeLayers = Layer.mergeAll(
            RunContextPreparationPassthrough,
            ThreadHistory.layerTransient,
            DemoTravelToolkitLayer,
            DemoHoldGatewayLayer,
            makeIdLayer(threadId, runId),
            approvalResolver,
            ApprovalAuditMemoryLive,
            StructuralRedactorLive,
          );

          const scriptedRuntimeLayer = Layer.mergeAll(
            commonRuntimeLayers,
            request.scenario === "tool-defect" ? DefectiveCatalogLayers : ControlledCatalogLayers,
            TravelGuidanceLayer,
          ).pipe(Layer.provide(controlsLayer));

          const liveRuntimeLayer = Layer.mergeAll(
            RunContextPreparationPassthrough,
            ThreadHistory.layerTransient,
            RealTravelToolkitLayer,
            DemoHoldGatewayLayer,
            makeIdLayer(threadId, runId),
            approvalResolver,
            ApprovalAuditMemoryLive,
            StructuralRedactorLive,
          ).pipe(Layer.provide(controlsLayer));

          const runPrelude =
            request.scenario === "guided"
              ? Effect.gen(function* () {
                  const mcp = yield* connectMcp(demoMcpRequest).pipe(
                    Effect.provideService(McpConnector, connector),
                    Effect.provideService(Crypto.Crypto, crypto),
                  );

                  yield* emit(
                    DemoMcpConnected.make({
                      ...(yield* eventBase),
                      serverId: mcp.discovery.identity.serverId,
                      implementationName: mcp.discovery.identity.implementation.name,
                      implementationVersion: mcp.discovery.identity.implementation.version,
                      toolCount: mcp.discovery.tools.length,
                      encodedBytes: mcp.discovery.encodedBytes,
                      toolkitSchemaDigest: mcp.discovery.toolkitSchemaDigest,
                      maxToolCount: demoMcpRequest.maxToolCount,
                      maxDiscoveryBytes: demoMcpRequest.maxDiscoveryBytes,
                    }),
                  );

                  const sandboxRequest = SandboxRequest.make({
                    runtime: SandboxRuntime.make({
                      kind: "unisolated-process",
                      identity: "local-process",
                    }),
                    command: "/bin/echo",
                    args: ["itinerary-check: schema-valid"],
                    cwd: "/tmp",
                    environment: SandboxEnvironment.make({ allow: [] }),
                    mounts: [],
                    network: NetworkDisabled.make({}),
                    limits: SandboxLimits.make({
                      maxOutputBytes: 2_048,
                      maxWallTime: Duration.seconds(2),
                    }),
                    secretHandles: [],
                    artifactRules: [],
                  });

                  yield* sandbox
                    .execute(sandboxRequest)
                    .pipe(
                      Stream.runForEach((event) =>
                        eventBase.pipe(
                          Effect.flatMap((base) =>
                            emit(DemoSandboxObserved.make({ ...base, event })),
                          ),
                        ),
                      ),
                    );
                })
              : Effect.void;

          const declared = yield* Ref.make<ReadonlyArray<RunEvent & { _tag: "ToolCallDeclared" }>>(
            [],
          );

          const completed = yield* Ref.make<
            ReadonlyArray<RunEvent & { _tag: "ToolCallSucceeded" }>
          >([]);

          const committed = yield* Ref.make(false);

          const inspectRunEvent = (event: RunEvent) =>
            Effect.gen(function* () {
              if (
                event._tag === "RunCompleted" ||
                event._tag === "RunFailed" ||
                event._tag === "RunInterrupted"
              ) {
                yield* Ref.set(acceptingCommands, false);
              }
              if (event._tag === "ToolCallDeclared") {
                yield* Ref.update(declared, (events) => [...events, event]);
              } else if (event._tag === "ToolCallSucceeded") {
                yield* Ref.update(completed, (events) => [...events, event]);
                if (
                  event.toolName === "search_flights" ||
                  event.toolName === "search_lodging" ||
                  event.toolName === "search_activities"
                ) {
                  yield* Deferred.succeed(gates.observed[event.toolName], undefined);
                }
              } else if (event._tag === "ModelStarted" && event.turn === 2) {
                const alreadyCommitted = yield* Ref.getAndSet(committed, true);

                if (!alreadyCommitted) {
                  yield* emit(
                    DemoToolBatchCommitted.make({
                      ...(yield* eventBase),
                      declaredOrder: (yield* Ref.get(declared)).map(
                        (declaredEvent) => declaredEvent.toolCallId,
                      ),
                      completionOrder: (yield* Ref.get(completed)).map(
                        (completedEvent) => completedEvent.toolCallId,
                      ),
                    }),
                  );
                }
              }
              yield* emit(event);
            });

          const runOptions: RunOptions<DemoHookError, ApprovalResolver | ApprovalAudit | Redactor> =
            {
              ...threadOptions,
              input: inputHook,
              context: contextHook,
              budget: budgetHook,
              estimateCostMicrousd: () => Effect.succeed(500),
              approval: approvalHook,
            };

          const releaseGuidedTools =
            !live && request.scenario === "guided"
              ? Effect.gen(function* () {
                  yield* Effect.all(
                    [
                      Deferred.await(gates.started.search_flights),
                      Deferred.await(gates.started.search_lodging),
                      Deferred.await(gates.started.search_activities),
                    ],
                    { concurrency: "unbounded" },
                  );
                  yield* queueCommand(
                    QueueRunCommandRequest.make({
                      handle,
                      kind: "steering",
                      content: guidedSteering,
                    }),
                  );
                  yield* queueCommand(
                    QueueRunCommandRequest.make({
                      handle,
                      kind: "follow-up",
                      content: guidedFollowUp,
                    }),
                  );
                  yield* Deferred.succeed(gates.release.search_activities, undefined);
                  yield* Deferred.await(gates.observed.search_activities);
                  yield* Deferred.succeed(gates.release.search_lodging, undefined);
                  yield* Deferred.await(gates.observed.search_lodging);
                  yield* Deferred.succeed(gates.release.search_flights, undefined);
                  yield* Deferred.await(gates.observed.search_flights);
                })
              : Effect.void;

          yield* Effect.forkChild(releaseGuidedTools);

          /**
           * The producer boundary observes the full Cause: a defect (for
           * example a dying Tool handler) must still fail the output queue and
           * release the single-run registry slot, or the browser waits forever
           * on a stream that will never terminalize.
           */
          const terminalizeProducerFailure = (cause: Cause.Cause<unknown>) =>
            Effect.gen(function* () {
              yield* Ref.set(acceptingCommands, false);
              const error = Cause.squash(cause);

              if (Schema.is(BudgetExceeded)(error)) {
                yield* emit(
                  DemoBudgetRejected.make({
                    ...(yield* eventBase),
                    scopeLevel: error.scopeLevel,
                    scopeId: error.scopeId,
                    limit: error.limit,
                    limitValue: error.limitValue,
                    observedValue: error.observedValue,
                  }),
                );
              }
              yield* cleanup;
              yield* Queue.fail(output, toDemoRunFailure(error));
            }).pipe(Effect.asVoid);

          const producer = Effect.gen(function* () {
            yield* runPrelude;
            if (live) {
              if (liveClient === null) {
                return yield* Effect.die(
                  new Error("Live travel started without an OpenAI client."),
                );
              }
              const message = "message" in request ? request.message : phase1Trip.request;

              const input = TripRequest.make({
                request: message,
                origin: phase1Trip.origin,
                destination: phase1Trip.destination,
                departOn: phase1Trip.departOn,
                nights: phase1Trip.nights,
                travelers: phase1Trip.travelers,
                budgetCents: phase1Trip.budgetCents,
                currency: phase1Trip.currency,
              });

              yield* AgentRuntime.stream(
                makeRealTravelPlannerAgent("settings" in request ? request.settings : undefined),
                input,
                runOptions,
              ).pipe(
                Stream.runForEach(inspectRunEvent),
                Effect.provide(liveRuntimeLayer),
                Effect.provideService(OpenAiClient.OpenAiClient, liveClient),
              );
            } else {
              yield* AgentRuntime.stream(operationalPlanAgent(turns), phase1Trip, runOptions).pipe(
                Stream.runForEach(inspectRunEvent),
                Effect.provide(scriptedRuntimeLayer),
              );
            }
            yield* Queue.end(output);
          }).pipe(
            Effect.onExit((exit) =>
              Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)
                ? Effect.void
                : terminalizeProducerFailure(exit.cause),
            ),
          );

          yield* Effect.forkChild(producer);

          return Stream.fromQueue(output);
        }).pipe(Effect.mapError(toDemoRunFailure)),
      );

    const start = (
      request: StartOperationalRunRequest,
    ): Stream.Stream<DemoOperationalEvent, DemoRunFailure> =>
      startWithProfile(request, "scripted", null);

    const startLiveTravel = (
      request: StartLiveTravelChatRequest,
    ): Stream.Stream<DemoOperationalEvent, DemoRunFailure, OpenAiClient.OpenAiClient> =>
      Stream.unwrap(
        Effect.map(OpenAiClient.OpenAiClient, (client) =>
          startWithProfile(request, "openai", client),
        ),
      );

    return DemoInteractiveRuntime.of({
      start,
      startLiveTravel,
      queueCommand,
      resolveApproval,
    });
  }),
);

const DemoInteractiveRuntimeDependencies = Layer.mergeAll(
  EphemeralThreadsLive,
  LocalSandboxLayer,
  DemoMcpConnectorLayer,
  NodeCrypto.layer,
);

export const DemoInteractiveRuntimeLive = InteractiveRuntimeLive.pipe(
  Layer.provide(DemoInteractiveRuntimeDependencies),
);
