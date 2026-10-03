import { RunToolAuthorization } from "@yielded/agent/run-options";
import { SubmissionLedger, SubmissionLookupById } from "@yielded/agent/submission-ledger";
import { ThreadExportRequest, ThreadStore } from "@yielded/agent/thread-store";
import { Effect, Layer, Option, Schema } from "effect";

import { PlannerError, PlannerInput } from "../domain.ts";
import { PlannerAttempt, ProgressStore } from "../server/progress.ts";
import { publicationAuthorization } from "../server/security.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { CheckedFinishResearchLive } from "./completion.ts";
import { researchCoordinatorId, ScoutInput } from "./contracts.ts";
import { UpdatingResearchScout, updatingResearchScout } from "./scout.ts";

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
    origin.worker.targetAgentId !== updatingResearchScout.id ||
    origin.worker.delegationId !== UpdatingResearchScout.delegationId ||
    origin.source.agentId !== researchCoordinatorId ||
    origin.source.threadId !== input.sourceThreadId ||
    origin.depth !== 1
  )
    return yield* unavailable();

  return { input, submission, origin };
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
        CheckedFinishResearchLive,
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
