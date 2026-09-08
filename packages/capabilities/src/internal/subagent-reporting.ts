import type { RunId, SettlementId } from "@effect-agent/core/Identifiers";
import type { Receipt } from "@effect-agent/core/Receipt";
import type { WorkerRef } from "@effect-agent/core/Worker";
import {
  WorkerReportPreparationFailure,
  type WorkerReporting,
  type WorkerRunReport,
} from "@effect-agent/engine/SubagentHost";
import { Effect, Schema } from "effect";

import { type Declaration } from "./subagent-background.ts";
import { SubagentExecutionFailure } from "./subagent-contract.ts";
import { utf8ByteLength } from "./utf8.ts";

/** One actual worker Run's projected terminal outcome, including its portable worker identity. */
export type WorkerReport<Success extends Schema.Top> = {
  readonly _tag: "Settled";
  readonly worker: WorkerRef;
  readonly receipt: Receipt;
  readonly runId: RunId;
  readonly settlementId: SettlementId;
} & (
  | { readonly outcome: "completed"; readonly result: Success["Type"] }
  | { readonly outcome: "failed" | "aborted"; readonly failure: SubagentExecutionFailure }
);

/**
 * Declare the conversion to coordinator input once, on its existing Agent Registration's
 * `reporting` array. The input Schema must be the coordinator Definition's exact input Schema.
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
      receipt: observation.receipt,
      runId: observation.runId,
      settlementId: observation.settlementId,
    };

    let projected: WorkerReport<Success>;

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

      const parameters = yield* Schema.decodeUnknownEffect(destination.parameters)(
        encodedParameters,
      ).pipe(Effect.mapError(invalid));

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
