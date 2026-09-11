import { WorkerError } from "@effect-agent/core/Worker";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { SubmissionLedger, SubmissionLookupById } from "@effect-agent/thread/SubmissionLedger";
import {
  WorkerBudgetAuthorizer,
  WorkerHostAuthorizer,
  WorkerHostConfig,
  WorkerPolicyResolver,
} from "@effect-agent/thread/WorkerHost";
import { Duration, Effect, Layer, Option, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";
import { Toolkit } from "effect/unstable/ai";

import { DeliverResponse } from "../agent.ts";
import { PlannerError } from "../domain.ts";
import { researchCoordinatorIds, expandedCoordinatorIds } from "../research/contracts.ts";
import {
  ResearchScout,
  researchScout,
  progressResearchScout,
  recoverableResearchScout,
} from "../research/scout.ts";
import { activeWorkerLimit, editorPolicy, scoutPolicy } from "../server/agent-limits.ts";
import { PlannerAttempt, ProgressStore, trackTool } from "../server/progress.ts";
import { ownerOfThread, storageOwner } from "../server/tenancy.ts";
import { tripRepositoryForOwner } from "../server/trip-rpc.ts";
import { TripRepository } from "../server/trips.ts";
import { AppEditor, appEditor, coordinatorId, EditorInput, EditorReadTrip } from "./editor.ts";
import { appRepositoryForOwner } from "./remote.ts";
import { AppRepository } from "./repository.ts";
import {
  addTripAppMap,
  createTripApp,
  editTripApp,
  readTripAppFiles,
  requireAppTrip,
  restoreTripApp,
} from "./service.ts";
import { AppTools } from "./tools.ts";

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "The app editor's admitted task could not be verified.",
  });

export const editorAttemptLayer = (context: {
  readonly threadId: string;
  readonly submissionId: SubmissionLookupById["submissionId"];
  readonly attemptId: string;
}) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const progress = yield* ProgressStore;
      const env = yield* WorkerEnvironment;
      const identity = yield* ThreadObjectIdentity;

      const input = yield* Effect.cached(
        Effect.gen(function* () {
          const found = yield* ledger
            .lookup(SubmissionLookupById.make({ submissionId: context.submissionId }))
            .pipe(Effect.mapError(unavailable));

          if (Option.isNone(found)) return yield* unavailable();
          const submission = found.value;
          const origin = submission.workerAdmission?.origin;

          const captured = yield* Schema.decodeUnknownEffect(EditorInput)(
            submission.inputPayload,
          ).pipe(Effect.mapError(unavailable));

          if (
            submission.threadId !== context.threadId ||
            origin === undefined ||
            origin.worker.threadId !== context.threadId ||
            origin.worker.targetAgentId !== appEditor.id ||
            origin.worker.delegationId !== AppEditor.delegationId ||
            ![coordinatorId, ...researchCoordinatorIds].includes(origin.source.agentId) ||
            captured.sourceThreadId !== origin.source.threadId ||
            origin.depth !== 1
          )
            return yield* unavailable();

          return captured;
        }),
      );

      const writer = yield* Effect.acquireRelease(
        progress.begin(context.submissionId, context.attemptId),
        (writer) => writer.finish,
      );

      const scoped = <A, R>(tripId: string, operation: Effect.Effect<A, PlannerError, R>) =>
        input.pipe(
          Effect.flatMap((captured) => {
            if (tripId !== captured.tripId)
              return Effect.fail(
                new PlannerError({
                  code: "invalid",
                  message: "This editor can change only its assigned trip.",
                }),
              );
            const owner = ownerOfThread(captured.sourceThreadId);

            return operation.pipe(
              Effect.provideService(ThreadObjectIdentity, {
                ...identity,
                threadId: captured.sourceThreadId,
              }),
              Effect.provideService(TripRepository, tripRepositoryForOwner(env, owner)),
              Effect.provideService(AppRepository, appRepositoryForOwner(env, owner)),
              Effect.provideService(WorkerEnvironment, env),
            );
          }),
        );

      const native = Toolkit.merge(AppTools, Toolkit.make(EditorReadTrip, DeliverResponse)).toLayer(
        {
          get_trip: ({ tripId }) => scoped(tripId, requireAppTrip(tripId)),
          deliver_response: (response) => Effect.succeed(response),
          create_trip_app: ({ tripId }, context) =>
            trackTool(
              context.toolCallId ?? "create",
              "Creating your trip app",
              scoped(tripId, createTripApp(tripId)),
            ),
          get_trip_app: ({ tripId }) =>
            scoped(
              tripId,
              Effect.gen(function* () {
                yield* requireAppTrip(tripId);

                return yield* Effect.flatMap(AppRepository, (apps) => apps.get(tripId));
              }),
            ),
          read_trip_app_files: ({ tripId, paths }, context) =>
            trackTool(
              context.toolCallId ?? "read",
              "Reading your app source",
              scoped(tripId, readTripAppFiles(tripId, paths)),
            ),
          edit_trip_app: (request, context) =>
            trackTool(
              context.toolCallId ?? "edit",
              "Saving app changes",
              scoped(request.tripId, editTripApp(request)),
            ),
          add_trip_app_map: ({ tripId }, context) =>
            trackTool(
              context.toolCallId ?? "map",
              "Adding your journey map",
              scoped(tripId, addTripAppMap(tripId)),
            ),
          restore_trip_app: ({ tripId, commitId }, context) =>
            trackTool(
              context.toolCallId ?? "restore",
              "Restoring your app",
              scoped(tripId, restoreTripApp(tripId, commitId)),
            ),
          set_trip_places: ({ tripId, places }) =>
            scoped(
              tripId,
              Effect.gen(function* () {
                const trip = yield* requireAppTrip(tripId);
                const trips = yield* TripRepository;

                return yield* trips.save(
                  { ...trip, tripId, expectedRevision: trip.revision, places },
                  yield* trips.conversationId(tripId),
                );
              }),
            ),
        },
      );

      return native.pipe(
        Layer.provideMerge(
          Layer.succeed(PlannerAttempt, {
            billingOwner: Effect.map(input, (input) => ownerOfThread(input.sourceThreadId)),
            settings: Effect.map(input, (input) => input.settings),
            progress: writer,
          }),
        ),
      );
    }),
  );

const sourceAllowed = (threadId: string, principal: string) => {
  if (!/^[a-zA-Z0-9-]{1,240}$/.test(threadId)) return false;
  const owner = ownerOfThread(threadId);

  return principal === (owner === storageOwner ? "travel-planner-owner" : owner);
};

/** Native caller identity and canonical lineage, never a model-supplied account, confer access. */
export const EditorHostLive = Layer.mergeAll(
  Layer.succeed(WorkerPolicyResolver, {
    resolveSource: () => Effect.succeed(Option.none()),
    resolveTarget: (request) => {
      if (
        ![
          appEditor.id,
          researchScout.id,
          progressResearchScout.id,
          recoverableResearchScout.id,
        ].includes(request.definition.id)
      )
        return Effect.succeed(Option.none());
      // A worker's allowance is immutable. Follow-ups and recovery retain it, including
      // workers started by earlier coordinators; v11 and later admissions opt in.
      if (request._tag === "RetainedWorker")
        return Effect.succeed(Option.some(request.origin.policy));
      if (!expandedCoordinatorIds.includes(request.source.agentId))
        return Effect.succeed(Option.none());
      if (
        request.sourceSubmission?.agentId !== request.source.agentId ||
        request.sourceSubmission.threadId !== request.source.threadId
      )
        return Effect.fail(WorkerError.make({ operation: "start", reason: "unavailable" }));

      return Effect.succeed(
        Option.some(
          [researchScout.id, progressResearchScout.id, recoverableResearchScout.id].includes(
            request.definition.id,
          )
            ? scoutPolicy
            : editorPolicy,
        ),
      );
    },
  }),
  Layer.succeed(WorkerHostAuthorizer, {
    authorize: (request) =>
      sourceAllowed(request.sourceThreadId, request.principal) &&
      (request.worker === undefined ||
        (request.worker.delegationId === AppEditor.delegationId &&
          request.worker.targetAgentId === appEditor.id) ||
        (request.worker.delegationId === ResearchScout.delegationId &&
          [researchScout.id, progressResearchScout.id, recoverableResearchScout.id].includes(
            request.worker.targetAgentId,
          )))
        ? Effect.succeed(request.principal)
        : Effect.fail(WorkerError.make({ operation: request.operation, reason: "denied" })),
  }),
  Layer.succeed(WorkerBudgetAuthorizer, {
    authorize: (request) => {
      const expanded = expandedCoordinatorIds.includes(request.source.agentId);

      const editor =
        [coordinatorId, ...researchCoordinatorIds].includes(request.source.agentId) &&
        request.worker.delegationId === AppEditor.delegationId &&
        request.worker.targetAgentId === appEditor.id;

      const scout =
        researchCoordinatorIds.includes(request.source.agentId) &&
        request.worker.delegationId === ResearchScout.delegationId &&
        [researchScout.id, progressResearchScout.id, recoverableResearchScout.id].includes(
          request.worker.targetAgentId,
        );

      const ceiling = editor
        ? expanded
          ? editorPolicy
          : appEditor.policy
        : expanded
          ? scoutPolicy
          : researchScout.policy;

      return sourceAllowed(request.source.threadId, request.principal) &&
        (editor || scout) &&
        request.policy.maxTurns <= ceiling.maxTurns &&
        request.policy.maxToolCalls <= ceiling.maxToolCalls &&
        request.policy.toolConcurrency <= ceiling.toolConcurrency &&
        Duration.isLessThanOrEqualTo(request.policy.maxDuration, ceiling.maxDuration)
        ? Effect.void
        : Effect.fail(WorkerError.make({ operation: "start", reason: "denied" }));
    },
  }),
  Layer.succeed(WorkerHostConfig, {
    maxWorkersPerSource: 100,
    maxActiveWorkersPerSource: activeWorkerLimit,
    maxInputsPerWorker: 256,
    maxPendingInputsPerWorker: 16,
    lifetimeMillis: 7 * 24 * 60 * 60 * 1000,
  }),
);
