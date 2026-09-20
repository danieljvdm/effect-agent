import { Effect, Schema } from "effect";

import { utf8ByteLength } from "../../core/internal/utf8.ts";
import { Receipt } from "../../core/Receipt.ts";
import { type WorkerReport, WorkerCompletion } from "../../core/Worker.ts";
import { WorkerReportPreparationFailure, type WorkerRunReport } from "../../engine/SubagentHost.ts";
import { type Declaration } from "./subagent-background.ts";
import { SubagentExecutionFailure } from "./subagent-contract.ts";

export { WorkerReport } from "../../core/Worker.ts";

const projectReport = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
) =>
  Effect.fn("Subagent.projectReport")(function* (report: WorkerRunReport) {
    const { worker, observation } = report;
    const invalid = () => WorkerReportPreparationFailure.make({ stage: "projection" });

    if (
      worker.delegationId !== declaration.delegationId ||
      worker.targetAgentId !== declaration.target.id ||
      observation.receipt.threadId !== worker.threadId
    )
      return yield* invalid();

    const base = {
      _tag: "Settled" as const,
      worker,
      receipt: yield* Schema.decodeEffect(Receipt)(observation.receipt).pipe(
        Effect.mapError(invalid),
      ),
      runId: observation.runId,
      settlementId: observation.settlementId,
    };

    let projected: WorkerReport<Success>;
    let encodedResult: Schema.Json = null;

    if (observation.outcome === "completed") {
      const parameters = yield* Schema.decodeUnknownEffect(declaration.parameters)(
        observation.encodedParameters,
      ).pipe(Effect.mapError(invalid));

      const output = yield* Schema.decodeUnknownEffect(declaration.target.output)(
        observation.encodedResult,
      ).pipe(Effect.mapError(invalid));

      const result = yield* declaration.projectResult(
        output,
        { budgetExhausted: observation.budgetExhausted },
        parameters,
      );

      const encoded = yield* Schema.encodeEffect(declaration.success)(result).pipe(
        Effect.mapError(invalid),
      );

      const json = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
        Effect.mapError(invalid),
      );

      if (
        utf8ByteLength(JSON.stringify(json)) >
        (declaration.policy?.maxResultBytes ?? declaration.target.policy.toolResultBounds.maxBytes)
      )
        return yield* invalid();
      projected = { ...base, outcome: "completed", result };
      encodedResult = json;
    } else {
      projected = {
        ...base,
        outcome: observation.outcome,
        failure: SubagentExecutionFailure.make({
          delegationId: declaration.delegationId,
          targetAgentId: declaration.target.id,
          classification: observation.outcome === "failed" ? "child-failed" : "child-aborted",
          childThreadId: worker.threadId,
          childSubmissionId: observation.receipt.submissionId,
          childRunId: observation.runId,
          errorTag: observation.outcome === "failed" ? "WorkerFailed" : "WorkerAborted",
          message:
            observation.outcome === "failed" ? "Worker input failed" : "Worker input was aborted",
        }),
      };
    }

    return { projected, encodedResult };
  });

/** Project a canonical child outcome into the bounded standard completion message. */
export const automaticReporting = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
) => ({
  delegationId: declaration.delegationId,
  target: declaration.target,
  prepare: Effect.fn("Subagent.automaticReport")(function* (report: WorkerRunReport) {
    const { projected, encodedResult } = yield* projectReport(declaration)(report);

    const encoded =
      projected.outcome === "completed"
        ? { ...projected, result: encodedResult }
        : {
            ...projected,
            failure: yield* Schema.encodeEffect(SubagentExecutionFailure)(projected.failure).pipe(
              Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" })),
            ),
          };

    const message = yield* Schema.decodeEffect(WorkerCompletion)({
      _tag: "WorkerCompletion",
      schemaVersion: 1,
      report: encoded,
      budgetExhausted: report.observation.budgetExhausted,
    }).pipe(Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" })));

    return { message };
  }),
});
