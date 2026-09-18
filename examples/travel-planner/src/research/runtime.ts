import { Effect, Layer, Option, Schema } from "effect";
import { Messaging, Subagent } from "effect-agent";
import { MessagingError } from "effect-agent/messaging";
import { PeerRoutes, PeerAuthorizer } from "effect-agent/messaging-host";
import type { Principal } from "effect-agent/receipt";
import { Receipt, IdempotencyKey } from "effect-agent/receipt";
import { RunToolAuthorization } from "effect-agent/run-options";
import { SubmissionLedger, SubmissionLookupById } from "effect-agent/submission-ledger";
import { ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { Toolkit } from "effect/unstable/ai";

import { PlannerError, PlannerInput } from "../domain.ts";
import {
  previousRecoverablePlanner,
  previousProgressPlanner,
  previousDelegatingPlanner,
} from "../server/planner.ts";
import { PlannerAttempt, ProgressStore } from "../server/progress.ts";
import { publicationAuthorization } from "../server/security.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { AppEditor, EditorInput } from "../trip-app/editor.ts";
import { CheckedFinishResearchLive } from "./completion.ts";
import type { ScoutFindings } from "./contracts.ts";
import {
  CoordinatorInput,
  ConversationInput,
  researchCoordinatorIds,
  ScoutInput,
  LiveConversationInput,
  progressCoordinatorIds,
} from "./contracts.ts";
import {
  FinishResearch,
  ResearchScout,
  ProgressResearchScout,
  ReportResearchProgress,
  progressResearchScout,
  recoverableResearchScout,
  researchScoutIds,
  RecoverableResearchScout,
} from "./scout.ts";

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "The admitted research task could not be verified.",
  });

/** Load the canonical owner input, including for failed Runs and joined steering receipts. */
export const readScoutInput = Effect.fn("readScoutInput")(function* (
  submissionId: SubmissionLookupById["submissionId"],
) {
  const ledger = yield* SubmissionLedger;

  const found = yield* ledger
    .lookup(SubmissionLookupById.make({ submissionId }))
    .pipe(Effect.mapError(unavailable));

  if (Option.isNone(found)) return yield* unavailable();
  const submission = found.value;
  const origin = submission.workerAdmission?.origin;

  const input = yield* Schema.decodeUnknownEffect(ScoutInput)(submission.inputPayload).pipe(
    Effect.mapError(unavailable),
  );

  if (
    origin === undefined ||
    origin.worker.threadId !== submission.threadId ||
    !researchScoutIds.includes(origin.worker.targetAgentId) ||
    origin.worker.delegationId !== ResearchScout.delegationId ||
    !researchCoordinatorIds.includes(origin.source.agentId) ||
    origin.source.threadId !== input.sourceThreadId ||
    origin.depth !== 1
  )
    return yield* unavailable();

  return { input, submission, origin };
});

const prepareResearchScoutReport = Effect.fn("prepareResearchScoutReport")(function* (
  report: Subagent.WorkerReport<typeof ScoutFindings>,
) {
  const captured = yield* readScoutInput(report.receipt.submissionId);

  if (
    captured.submission.threadId !== report.worker.threadId ||
    captured.submission.receiptId !== report.receipt.receiptId
  )
    return yield* unavailable();

  return {
    _tag: "ResearchScoutReport" as const,
    worker: report.worker,
    receipt: Receipt.make(report.receipt),
    runId: report.runId,
    settlementId: report.settlementId,
    title: captured.input.title,
    settings: captured.input.settings,
    outcome: report.outcome,
    findings: report.outcome === "completed" ? report.result : null,
  };
});

export const researchScoutReport = Subagent.reporting(ResearchScout, {
  input: CoordinatorInput,
  failure: PlannerError,
  prepare: prepareResearchScoutReport,
});

export const conversationScoutReport = Subagent.reporting(ResearchScout, {
  input: ConversationInput,
  failure: PlannerError,
  prepare: prepareResearchScoutReport,
});

export const liveScoutReport = Subagent.reporting(ProgressResearchScout, {
  input: LiveConversationInput,
  failure: PlannerError,
  prepare: prepareResearchScoutReport,
});

export const recoverableScoutReport = Subagent.reporting(RecoverableResearchScout, {
  input: LiveConversationInput,
  failure: PlannerError,
  prepare: prepareResearchScoutReport,
});

export const editorReport = Subagent.reporting(AppEditor, {
  input: LiveConversationInput,
  failure: PlannerError,
  prepare: Effect.fn("prepareEditorReport")(function* (report) {
    const ledger = yield* SubmissionLedger;

    const found = yield* ledger
      .lookup(SubmissionLookupById.make({ submissionId: report.receipt.submissionId }))
      .pipe(Effect.mapError(unavailable));

    if (Option.isNone(found)) return yield* unavailable();
    const origin = found.value.workerAdmission?.origin;

    const input = yield* Schema.decodeUnknownEffect(EditorInput)(found.value.inputPayload).pipe(
      Effect.mapError(unavailable),
    );

    if (
      !origin ||
      origin.worker.threadId !== report.worker.threadId ||
      origin.source.threadId !== input.sourceThreadId ||
      !progressCoordinatorIds.includes(origin.source.agentId) ||
      found.value.receiptId !== report.receipt.receiptId
    )
      return yield* unavailable();

    return {
      _tag: "AppEditorReport" as const,
      worker: report.worker,
      settings: input.settings,
      outcome: report.outcome,
      summary: report.outcome === "completed" ? report.result.output : null,
    };
  }),
});

const conversationPeer = Messaging.peer("travel_conversation", {
  target: previousRecoverablePlanner,
});

const previousConversationPeer = Messaging.peer("travel_conversation", {
  target: previousProgressPlanner,
});

const previousDelegatingConversationPeer = Messaging.peer("travel_conversation", {
  target: previousDelegatingPlanner,
});

/** Only the canonical scout origin can select the receiving conversation and account. */
export const ScoutMessagingLive = Layer.unwrap(
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    const destination = Effect.fn("scoutMessageDestination")(function* (
      source: { readonly threadId: ThreadExportRequest["threadId"]; readonly agentId: string },
      principal: Principal,
    ) {
      const denied = () => MessagingError.make({ operation: "send", reason: "denied" });

      const history = yield* store
        .export(ThreadExportRequest.make({ threadId: source.threadId }))
        .pipe(Effect.mapError(denied));

      const origin = history.records.find(
        ({ record }) => record.payload._tag === "WorkerOriginRecorded",
      )?.record.payload;

      if (
        origin?._tag !== "WorkerOriginRecorded" ||
        (source.agentId !== progressResearchScout.id &&
          source.agentId !== recoverableResearchScout.id) ||
        origin.origin.worker.threadId !== source.threadId ||
        origin.origin.worker.targetAgentId !== source.agentId ||
        origin.origin.worker.delegationId !== ResearchScout.delegationId ||
        !progressCoordinatorIds.includes(origin.origin.source.agentId) ||
        origin.origin.depth !== 1
      )
        return yield* denied();
      const owner = ownerOfThread(origin.origin.source.threadId);

      if (principal !== owner) return yield* denied();

      return origin.origin.source;
    });

    return Layer.mergeAll(
      Layer.succeed(PeerRoutes, {
        resolve: (request) =>
          Effect.gen(function* () {
            const target = yield* destination(request.source, request.principal);

            if (
              request.peerName !== conversationPeer.name ||
              request.targetAgentId !== target.agentId
            )
              return yield* MessagingError.make({ operation: "send", reason: "denied" });

            return target.threadId;
          }),
      }),
      Layer.succeed(PeerAuthorizer, {
        authorize: (request) =>
          Effect.gen(function* () {
            const target = yield* destination(request.source, request.principal);

            if (
              (request.access !== "context" && request.access !== "send") ||
              (request.peerName !== undefined && request.peerName !== conversationPeer.name) ||
              (request.destination &&
                (request.destination.threadId !== target.threadId ||
                  request.destination.agentId !== target.agentId))
            )
              return yield* MessagingError.make({ operation: request.operation, reason: "denied" });

            return request.principal;
          }),
      }),
    );
  }),
);

export const scoutAttemptLayer = (
  context: {
    readonly threadId: string;
    readonly submissionId: SubmissionLookupById["submissionId"];
    readonly attemptId: string;
  },
  recoverable = false,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const progress = yield* ProgressStore;
      const ledger = yield* SubmissionLedger;

      const input = yield* Effect.cached(
        readScoutInput(context.submissionId).pipe(
          Effect.provideService(SubmissionLedger, ledger),
          Effect.flatMap((captured) =>
            captured.submission.threadId === context.threadId
              ? Effect.succeed(captured.input)
              : Effect.fail(unavailable()),
          ),
        ),
      );

      const writer = yield* Effect.acquireRelease(
        progress.begin(context.submissionId, context.attemptId),
        (writer) => writer.finish,
      );

      return Layer.mergeAll(
        Toolkit.make(ReportResearchProgress).toLayer({
          report_research_progress: (finding, tool) =>
            Effect.gen(function* () {
              const captured = yield* readScoutInput(context.submissionId).pipe(
                Effect.provideService(SubmissionLedger, ledger),
                Effect.mapError(() => MessagingError.make({ operation: "send", reason: "denied" })),
              );

              const key = yield* Schema.decodeEffect(IdempotencyKey)(
                `progress:${context.submissionId}:${tool.toolCallId}`,
              ).pipe(
                Effect.mapError(() =>
                  MessagingError.make({ operation: "send", reason: "invalid-input" }),
                ),
              );

              return yield* Messaging.send(
                captured.origin.source.agentId === previousProgressPlanner.id
                  ? previousConversationPeer
                  : captured.origin.source.agentId === previousDelegatingPlanner.id
                    ? previousDelegatingConversationPeer
                    : conversationPeer,
                {
                  _tag: "ResearchScoutProgress",
                  worker: captured.origin.worker,
                  title: captured.input.title,
                  settings: captured.input.settings,
                  finding,
                },
                { idempotencyKey: key },
              );
            }),
        }),
        recoverable
          ? CheckedFinishResearchLive
          : Toolkit.make(FinishResearch).toLayer({
              finish_research: (findings) => Effect.succeed(findings),
            }),
        Layer.succeed(PlannerAttempt, {
          billingOwner: Effect.map(input, (input) => ownerOfThread(input.sourceThreadId)),
          settings: Effect.map(input, (input) => input.settings),
          progress: writer,
        }),
      );
    }),
  );

/** Reports joined into a user Run cannot inherit delegation or publication authority. */
export const ResearchAuthorizationLive = Layer.effect(
  RunToolAuthorization,
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return RunToolAuthorization.of({
      authorize: (request) => {
        if (
          ![
            "research_scout_start",
            "research_scout_follow_up",
            "previous_research_scout_follow_up",
            "previous_progress_research_scout_follow_up",
            "previous_recoverable_research_scout_follow_up",
            "app_editor_start",
            "app_editor_follow_up",
            "publish_trip_site",
          ].includes(request.call.toolName)
        )
          return publicationAuthorization.authorize(request);

        const denied = {
          _tag: "denied" as const,
          reason:
            request.call.toolName === "publish_trip_site"
              ? "Ask the user to select this trip and explicitly publish its current revision."
              : "Report the completed research. A new user request is required to start or steer another research pass.",
        };

        return store.export(ThreadExportRequest.make({ threadId: request.threadId })).pipe(
          Effect.flatMap((history): ReturnType<typeof publicationAuthorization.authorize> => {
            const response = history.records.findIndex(
              ({ record }) =>
                record.payload._tag === "ModelResponseRecorded" &&
                record.payload.runId === request.runId &&
                record.payload.turnId === request.turnId &&
                record.payload.turn === request.turn,
            );

            if (response < 0) return Effect.succeed(denied);

            // Later admissions cannot authorize an already-declared call. In particular, a
            // joined report supersedes the original user input even within the same Run.
            const input = history.records
              .slice(0, response)
              .findLast(
                ({ record }) =>
                  record.payload._tag === "UserInputRecorded" &&
                  record.payload.runId === request.runId,
              )?.record.payload;

            return input?._tag === "UserInputRecorded" &&
              input.messageAdmission === undefined &&
              Schema.is(PlannerInput)(input.input)
              ? publicationAuthorization.authorize({
                  ...request,
                  input: input.input,
                  frameworkMessage: undefined,
                })
              : Effect.succeed(denied);
          }),
          Effect.orElseSucceed(() => denied),
        );
      },
    });
  }),
);
