import { Receipt } from "@effect-agent/core/Receipt";
import { type WorkerReport, WorkerCompletion } from "@effect-agent/core/Worker";
import {
  WorkerReportPreparationFailure,
  type WorkerReporting,
  type WorkerRunReport,
} from "@effect-agent/engine/SubagentHost";
import { Effect, Schema } from "effect";

import { type Declaration } from "./subagent-background.ts";
import { SubagentExecutionFailure } from "./subagent-contract.ts";
import { utf8ByteLength } from "./utf8.ts";

export { WorkerReport } from "@effect-agent/core/Worker";

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

/** Framework reporting shares the same bounded declaration projection as custom mapping. */
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
  mode: "standard" as const,
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

    return { encodedInput: null, message };
  }),
});

/**
 * Define an optional application-specific conversion for `background({ reportToParent: report })`.
 * Existing Agent Registration `reporting` arrays remain supported. The input Schema must be the coordinator Definition's exact input Schema.
 * Preparation is bounded by the durable host; a retained prepared envelope is never reprojected
 * during delivery retries. The callback should be deterministic and have no external side effects.
 */
export const reporting = <
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
  CoordinatorInput extends Schema.Top,
  ReportFailure extends Schema.Top = typeof Schema.Never,
  ReportRequirements = never,
>(
  declaration: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
  options: {
    readonly input: CoordinatorInput;
    readonly failure?: ReportFailure;
    readonly prepare: (
      report: WorkerReport<Success>,
    ) => Effect.Effect<CoordinatorInput["Type"], ReportFailure["Type"], ReportRequirements>;
  },
): WorkerReporting<
  Failure["Type"] | ReportFailure["Type"],
  | Parameters["DecodingServices"]
  | Output["DecodingServices"]
  | Success["EncodingServices"]
  | CoordinatorInput["EncodingServices"]
  | Project
  | ReportRequirements
> => {
  const prepare = Effect.fn("Subagent.reporting.prepare")(function* (report: WorkerRunReport) {
    const { projected } = yield* projectReport(declaration)(report);
    const input = yield* options.prepare(projected);

    const encoded = yield* Schema.encodeEffect(options.input)(input).pipe(
      Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "input" })),
    );

    const encodedInput = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
      Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "input" })),
    );

    return { encodedInput };
  });

  return Object.freeze({
    delegationId: declaration.delegationId,
    target: declaration.target,
    input: options.input,
    prepare,
  });
};

/**
 * Adapt a report expressed in a receiving worker declaration's Parameters to its Agent input.
 * Use this on that worker's registration when it can itself launch reporting workers. This
 * explicit conversion preserves custom Parameters and charges the next input to its ancestor.
 */
export const reportingToWorker = <
  E,
  R,
  const Name extends string,
  Input extends Schema.Top,
  Output extends Schema.Top,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  Failure extends Schema.Top,
  Prepare,
  Project,
>(
  report: WorkerReporting<E, R>,
  destination: Declaration<Name, Input, Output, Parameters, Success, Failure, Prepare, Project>,
): WorkerReporting<
  E | Failure["Type"],
  R | Prepare | Parameters["DecodingServices"] | Input["EncodingServices"]
> => {
  if (report.input !== destination.parameters) {
    throw new TypeError(
      "Worker reporting must use the destination declaration's Parameters Schema",
    );
  }

  return Object.freeze({
    delegationId: report.delegationId,
    target: report.target,
    input: destination.target.input,
    destination: { delegationId: destination.delegationId, target: destination.target },
    prepare: Effect.fn("Subagent.reportingToWorker.prepare")(function* (run: WorkerRunReport) {
      const encodedParameters = (yield* report.prepare(run)).encodedInput;
      const invalid = () => WorkerReportPreparationFailure.make({ stage: "input" });

      const parameters = yield* Schema.decodeEffect(destination.parameters)(encodedParameters).pipe(
        Effect.mapError(invalid),
      );

      const input = yield* destination.prepareInput(parameters, {
        source: "programmatic",
        delegationId: destination.delegationId,
        parent: { agentId: run.context.source.agentId, threadId: run.context.source.threadId },
      });

      const encoded = yield* Schema.encodeEffect(destination.target.input)(input).pipe(
        Effect.mapError(invalid),
      );

      const encodedInput = yield* Schema.decodeUnknownEffect(Schema.Json)(encoded).pipe(
        Effect.mapError(invalid),
      );

      return { encodedInput, encodedParameters };
    }),
  });
};
