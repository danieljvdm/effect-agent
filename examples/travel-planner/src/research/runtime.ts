import * as Subagent from "@effect-agent/capabilities/Subagent";
import { Receipt } from "@effect-agent/core/Receipt";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { SubmissionLedger, SubmissionLookupById } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { Effect, Layer, Option, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";

import { PlannerError, PlannerInput } from "../domain.ts";
import { PlannerAttempt, ProgressStore } from "../server/progress.ts";
import { publicationAuthorization } from "../server/security.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import type { ScoutFindings } from "./contracts.ts";
import {
  CoordinatorInput,
  ConversationInput,
  researchCoordinatorIds,
  ScoutInput,
  ScoutReportInput,
} from "./contracts.ts";
import { FinishResearch, ResearchScout, researchScout } from "./scout.ts";

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
    origin.worker.targetAgentId !== researchScout.id ||
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

export const scoutAttemptLayer = (context: {
  readonly threadId: string;
  readonly submissionId: SubmissionLookupById["submissionId"];
  readonly attemptId: string;
}) =>
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
        Toolkit.make(FinishResearch).toLayer({
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

/** A completion report cannot create another research generation without a new canonical user input. */
export const ResearchAuthorizationLive = Layer.effect(
  RunToolAuthorization,
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return RunToolAuthorization.of({
      authorize: (request) => {
        if (
          Option.isNone(Schema.decodeUnknownOption(ScoutReportInput)(request.input)) ||
          ![
            "research_scout_start",
            "research_scout_follow_up",
            "app_editor_start",
            "app_editor_follow_up",
          ].includes(request.call.toolName)
        )
          return publicationAuthorization.authorize(request);

        const denied = {
          _tag: "denied" as const,
          reason:
            "Report the completed research. A new user request is required to start or steer another research pass.",
        };

        return store.export(ThreadExportRequest.make({ threadId: request.threadId })).pipe(
          Effect.map((history) =>
            history.records.some(
              ({ record }) =>
                record.payload._tag === "UserInputRecorded" &&
                record.payload.runId === request.runId &&
                Schema.is(PlannerInput)(record.payload.input),
            )
              ? { _tag: "allowed" as const }
              : denied,
          ),
          Effect.orElseSucceed(() => denied),
        );
      },
    });
  }),
);
