import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
import type { Redacted } from "effect";
import { Clock, Effect, Exit, FileSystem, Path, Schedule, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { EvaluationError } from "./contracts.ts";
import type { PerformanceIdentity } from "./performance-contracts.ts";
import {
  PerformanceOutput,
  PerformanceSnapshot,
  expectedPerformanceOutput,
} from "./performance-contracts.ts";

const Timing = Schema.Record(Schema.String, Schema.NullOr(Schema.Finite));

const AuditedRequest = Schema.Struct({
  input: Schema.Array(
    Schema.Struct({
      type: Schema.optionalKey(Schema.String),
      role: Schema.optionalKey(Schema.String),
      output: Schema.optionalKey(Schema.String),
      content: Schema.optionalKey(
        Schema.Union([
          Schema.String,
          Schema.Array(
            Schema.Struct({ type: Schema.String, text: Schema.optionalKey(Schema.String) }),
          ),
        ]),
      ),
    }),
  ),
});

const requestParts = (json: string) => {
  const request = Schema.decodeOption(Schema.fromJsonString(AuditedRequest))(json);

  return request._tag === "Some" ? request.value.input : [];
};

export const PerformancePhaseResult = Schema.Struct({
  phase: Schema.Int,
  condition: Schema.Literals(["fresh-thread", "warm-retained-history", "committed-tool-recovery"]),
  runId: Schema.String,
  passed: Schema.Boolean,
  failures: Schema.Array(Schema.String),
  timing: Timing,
});

export const PerformanceSample = Schema.Struct({
  target: Schema.String,
  sample: Schema.Int,
  passed: Schema.Boolean,
  failure: Schema.NullOr(Schema.String),
  initializationProbeMillis: Schema.NullOr(Schema.Finite),
  phases: Schema.Array(PerformancePhaseResult),
  closeRequested: Schema.Boolean,
});

/** Host durations use only the DO clock; client durations use only the runner clock. Never sum these overlapping intervals. */
export const performanceTiming = (snapshot: typeof PerformanceSnapshot.Type, phase: number) => {
  const events = snapshot.events.filter((event) => event.phase === phase);
  const first = (kind: string) => events.find((event) => event.kind === kind)?.atMillis;
  const last = (kind: string) => events.findLast((event) => event.kind === kind)?.atMillis;
  const firstDispatch = events.find((event) => event.kind === "provider-http-dispatch");

  const matchingFirstDelta =
    firstDispatch === undefined
      ? undefined
      : events.find(
          (event) =>
            event.kind === "first-provider-delta" &&
            event.request === firstDispatch.request &&
            event.incarnation === firstDispatch.incarnation,
        );

  const between = (start: number | undefined, end: number | undefined) =>
    start === undefined ||
    end === undefined ||
    !Number.isFinite(start) ||
    !Number.isFinite(end) ||
    end < start
      ? null
      : end - start;

  return {
    admissionMillis: between(first("ledger:admit:before"), first("ledger:admit:after")),
    queueToClaimMillis: between(first("ledger:admit:after"), first("claim:after-claim")),
    preparationBeforePreflightMillis: between(first("attempt-start"), first("preflight-start")),
    inputTokenPreflightMillis: between(first("preflight-start"), first("preflight-end")),
    preparationAfterPreflightMillis: between(
      first("preflight-end"),
      first("provider-http-dispatch"),
    ),
    ingressToProviderDispatchMillis: between(
      first("submission-ingress"),
      first("provider-http-dispatch"),
    ),
    providerDispatchToFirstDeltaMillis: between(
      firstDispatch?.atMillis,
      matchingFirstDelta?.atMillis,
    ),
    priceToolMillis: between(first("tool:read_price:start"), first("tool:read_price:end")),
    stockToolMillis: between(first("tool:read_stock:start"), first("tool:read_stock:end")),
    toolBatchWallMillis: between(
      Math.min(
        first("tool:read_price:start") ?? Infinity,
        first("tool:read_stock:start") ?? Infinity,
      ),
      Math.max(last("tool:read_price:end") ?? -Infinity, last("tool:read_stock:end") ?? -Infinity),
    ),
    toolResultsCommitMillis: between(
      Math.max(last("tool:read_price:end") ?? -Infinity, last("tool:read_stock:end") ?? -Infinity),
      first("turn:after-results-append"),
    ),
    finalProviderToCanonicalCompletionMillis: between(
      last("provider-completed"),
      last("terminalize:after-canonical-append"),
    ),
    ingressToCanonicalCompletionMillis: between(
      first("submission-ingress"),
      last("terminalize:after-canonical-append"),
    ),
  };
};

export const gradePerformancePhase = Effect.fn("Performance.grade")(function* (
  snapshot: typeof PerformanceSnapshot.Type,
  phase: number,
  receipt: Pick<Receipt, "submissionId">,
  previousIncarnation: number,
) {
  const failures: Array<string> = [];
  const runId = runIdForSubmission(receipt.submissionId);
  const records = snapshot.records.map(({ record }) => record.payload);

  const settled = records.filter(
    (record) => record._tag === "SubmissionSettled" && record.submissionId === receipt.submissionId,
  );

  const completed = records.filter(
    (record) => record._tag === "RunCompleted" && record.runId === runId,
  );

  if (
    settled.length !== 1 ||
    settled[0]?._tag !== "SubmissionSettled" ||
    settled[0].outcome !== "completed"
  )
    failures.push("Exactly one completed canonical settlement required");
  if (
    completed.length !== 1 ||
    completed[0]?._tag !== "RunCompleted" ||
    completed[0].finishReason !== undefined
  )
    failures.push("Exactly one unexhausted canonical completion required");

  const output =
    completed[0]?._tag === "RunCompleted"
      ? yield* Schema.decodeUnknownEffect(PerformanceOutput)(completed[0].output).pipe(
          Effect.option,
        )
      : undefined;

  const encodedExpected = Schema.encodeSync(Schema.fromJsonString(PerformanceOutput))(
    expectedPerformanceOutput(phase),
  );

  if (
    output === undefined ||
    output._tag === "None" ||
    Schema.encodeSync(Schema.fromJsonString(PerformanceOutput))(output.value) !== encodedExpected
  )
    failures.push(
      "Schema-valid order must contain computed totals, current tool evidence, and previous order total",
    );
  for (const name of ["read_price", "read_stock"]) {
    const results = records.filter(
      (record) =>
        record._tag === "ToolCallSettled" &&
        record.runId === runId &&
        record.toolName === name &&
        !record.isFailure,
    );

    if (results.length !== 1) failures.push(`Expected one successful canonical ${name} result`);
    const canonicalResult = results[0];

    const resultSchema =
      name === "read_price"
        ? Schema.Struct({
            unitPriceCents: Schema.Literal(3_700),
            evidence: Schema.Literal(`price-${phase}-q7`),
          })
        : Schema.Struct({
            availableUnits: Schema.Literal(12),
            evidence: Schema.Literal(`stock-${phase}-m9`),
          });

    if (
      canonicalResult?._tag !== "ToolCallSettled" ||
      !Schema.is(resultSchema)(canonicalResult.result)
    )
      failures.push(`Canonical ${name} payload differs from the fixture`);
    if (
      snapshot.events.filter(
        (event) => event.phase === phase && event.kind === `tool:${name}:start`,
      ).length !== 1
    )
      failures.push(`Expected one ${name} execution, including across recovery`);
  }

  const requests = snapshot.audits.filter(
    (audit) => audit.phase === phase && audit.kind === "request",
  );

  const expected = expectedPerformanceOutput(phase);

  if (phase > 0) {
    const previous = records.findLast(
      (record) => record._tag === "RunCompleted" && record.runId !== runId,
    );

    const previousOutput =
      previous?._tag === "RunCompleted"
        ? Schema.decodeUnknownOption(PerformanceOutput)(previous.output)
        : undefined;

    const initialAssistantOutputs = requestParts(requests[0]?.json ?? "{}").flatMap((item) =>
      item.type === "message" && item.role === "assistant"
        ? typeof item.content === "string"
          ? [item.content]
          : (item.content ?? []).flatMap((part) => (part.text === undefined ? [] : [part.text]))
        : [],
    );

    const preserved =
      previousOutput?._tag === "Some" &&
      initialAssistantOutputs.some((text) => {
        const decoded = Schema.decodeOption(Schema.fromJsonString(PerformanceOutput))(text);

        return (
          decoded._tag === "Some" &&
          Schema.encodeSync(Schema.fromJsonString(PerformanceOutput))(decoded.value) ===
            Schema.encodeSync(Schema.fromJsonString(PerformanceOutput))(previousOutput.value)
        );
      });

    if (!preserved)
      failures.push(
        "Initial provider request must retain the previous canonical assistant order output",
      );
  }

  const later = requests.slice(1).some((request) => {
    const outputs = requestParts(request.json).flatMap((item) =>
      item.type === "function_call_output" && item.output !== undefined ? [item.output] : [],
    );

    return (
      outputs.some(
        (output) =>
          Schema.decodeOption(
            Schema.fromJsonString(
              Schema.Struct({
                unitPriceCents: Schema.Literal(3_700),
                evidence: Schema.Literal(expected.priceEvidence),
              }),
            ),
          )(output)._tag === "Some",
      ) &&
      outputs.some(
        (output) =>
          Schema.decodeOption(
            Schema.fromJsonString(
              Schema.Struct({
                availableUnits: Schema.Literal(12),
                evidence: Schema.Literal(expected.stockEvidence),
              }),
            ),
          )(output)._tag === "Some",
      )
    );
  });

  if (!later)
    failures.push("Subsequent real provider request must consume both current tool results");
  if (
    requests[0]?.json.includes(expected.priceEvidence) ||
    requests[0]?.json.includes(expected.stockEvidence)
  )
    failures.push("Tool evidence leaked into initial request");
  if (
    snapshot.usage.calls !== snapshot.usage.completedCalls ||
    snapshot.usage.reservedCostMicrousd !== 0 ||
    snapshot.failure !== null
  )
    failures.push("Provider usage must be completely accounted for");
  const events = snapshot.events.filter((event) => event.phase === phase);

  if (
    !events.some((event) => event.kind === "provider-http-dispatch") ||
    !events.some((event) => event.kind === "first-provider-delta")
  )
    failures.push("Provider dispatch and first delta evidence required");
  if (phase === 1 && snapshot.incarnation !== previousIncarnation)
    failures.push("Warm case changed incarnation; retain sample as failed warm condition");
  if (
    phase === 2 &&
    !(
      snapshot.incarnation > previousIncarnation &&
      events.filter((event) => event.kind === "planned-abort-after-tool-commit").length === 1
    )
  )
    failures.push("Recovery requires one confirmed planned abort and a new incarnation");
  if (
    !events.some(
      (event) => event.kind === "attempt-finalized" && event.incarnation === snapshot.incarnation,
    )
  )
    failures.push("Successful attempt finalizer evidence required");

  return {
    phase,
    condition:
      phase === 0
        ? "fresh-thread"
        : phase === 1
          ? "warm-retained-history"
          : "committed-tool-recovery",
    runId,
    passed: failures.length === 0,
    failures,
    timing: performanceTiming(snapshot, phase),
  } satisfies typeof PerformancePhaseResult.Type;
});

export const runPerformanceSample = Effect.fn("Performance.sample")(function* (options: {
  readonly url: string;
  readonly token: Redacted.Redacted<string>;
  readonly identity: typeof PerformanceIdentity.Type;
  readonly target: string;
  readonly sample: number;
  readonly outputDirectory: string;
}) {
  const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk);
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  let result: typeof PerformanceSample.Type = {
    target: options.target,
    sample: options.sample,
    passed: false,
    failure: null,
    initializationProbeMillis: null,
    phases: [],
    closeRequested: false,
  };

  const get = (route: string) =>
    client.execute(
      HttpClientRequest.get(`${options.url}${route}?sample=${options.sample}`).pipe(
        HttpClientRequest.bearerToken(options.token),
      ),
    );

  const post = (route: string, body: { phase?: number }) =>
    client.execute(
      HttpClientRequest.post(`${options.url}${route}?sample=${options.sample}`).pipe(
        HttpClientRequest.bearerToken(options.token),
        HttpClientRequest.bodyJsonUnsafe(body),
      ),
    );

  const save = () =>
    fs.writeFileString(
      path.join(options.outputDirectory, "sample.json"),
      Schema.encodeSync(Schema.fromJsonString(PerformanceSample))(result),
    );

  const snapshot = get("/snapshot").pipe(
    Effect.flatMap(HttpClientResponse.schemaBodyJson(PerformanceSnapshot)),
    Effect.timeout("15 seconds"),
  );

  const observe = snapshot.pipe(
    Effect.retry({ times: 3, schedule: Schedule.spaced("250 millis") }),
  );

  yield* fs.makeDirectory(options.outputDirectory, { recursive: true });
  yield* save();

  const execution = Effect.gen(function* () {
    const probeStart = yield* Clock.currentTimeMillis;
    const before = yield* observe;

    result = {
      ...result,
      initializationProbeMillis: (yield* Clock.currentTimeMillis) - probeStart,
    };
    if (before.records.length > 0)
      return yield* EvaluationError.make({
        stage: "fixture",
        message: "Sample thread already contains canonical history; use a new deployment",
      });
    let previousIncarnation = before.incarnation;

    for (const phase of [0, 1, 2]) {
      const start = yield* Clock.currentTimeMillis;

      const receipt = yield* post("/submit", { phase }).pipe(
        Effect.flatMap(HttpClientResponse.schemaBodyJson(Receipt)),
        Effect.timeout("30 seconds"),
      );

      const receiptMillis = (yield* Clock.currentTimeMillis) - start;
      let firstFeedbackMillis: number | null = null;
      let polls = 0;

      const phaseResult = yield* Effect.gen(function* () {
        for (;;) {
          const current = yield* observe;
          const observedAt = yield* Clock.currentTimeMillis;

          polls++;
          if (JSON.stringify(current.identity) !== JSON.stringify(options.identity))
            return yield* EvaluationError.make({
              stage: "source",
              message: "Deployed identity changed during sample",
            });
          yield* fs.writeFileString(
            path.join(options.outputDirectory, `phase-${phase}-snapshot.json`),
            yield* Schema.encodeEffect(Schema.fromJsonString(PerformanceSnapshot))(current),
          );
          const runId = runIdForSubmission(receipt.submissionId);

          if (
            firstFeedbackMillis === null &&
            current.records.some(
              ({ record }) =>
                record.payload._tag === "ModelResponseRecorded" && record.payload.runId === runId,
            )
          )
            firstFeedbackMillis = observedAt - start;
          if (current.failure !== null)
            return yield* EvaluationError.make({ stage: "provider", message: current.failure });
          if (
            current.records.some(
              ({ record }) =>
                record.payload._tag === "SubmissionSettled" &&
                record.payload.submissionId === receipt.submissionId,
            )
          ) {
            // Settlement is visible before the Attempt scope necessarily closes.
            // Keep observing within the same deadline until its finalizer is confirmed.
            if (
              !current.events.some(
                (event) =>
                  event.phase === phase &&
                  event.incarnation === current.incarnation &&
                  event.kind === "attempt-finalized",
              )
            ) {
              yield* Effect.sleep("100 millis");
              continue;
            }
            const final = current;
            const grade = yield* gradePerformancePhase(final, phase, receipt, previousIncarnation);
            const deliveredAt = yield* Clock.currentTimeMillis;

            yield* fs.writeFileString(
              path.join(options.outputDirectory, `phase-${phase}-snapshot.json`),
              yield* Schema.encodeEffect(Schema.fromJsonString(PerformanceSnapshot))(final),
            );
            previousIncarnation = final.incarnation;

            return {
              ...grade,
              timing: {
                ...grade.timing,
                clientAdmissionReceiptMillis: receiptMillis,
                clientFirstCanonicalFeedbackMillis: firstFeedbackMillis,
                clientValidatedCompletionDeliveryMillis: deliveredAt - start,
                observationPolls: polls,
              },
            };
          }
          yield* Effect.sleep("100 millis");
        }
      }).pipe(Effect.timeout("150 seconds"));

      result = { ...result, phases: [...result.phases, phaseResult] };
      yield* save();
      if (!phaseResult.passed)
        return yield* EvaluationError.make({
          stage: "outcome",
          message: "Order or lifecycle assertions failed",
        });
    }
    result = { ...result, passed: true };
  }).pipe(Effect.timeout("8 minutes"));

  yield* execution.pipe(
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        if (Exit.isFailure(exit))
          result = {
            ...result,
            passed: false,
            failure: exit.cause.reasons
              .flatMap((reason) =>
                reason._tag === "Fail" && Schema.is(EvaluationError)(reason.error)
                  ? [`${reason.error.stage}: ${reason.error.message}`]
                  : [reason._tag],
              )
              .join("; "),
          };
        // Preserve one final snapshot even after failure, before shutdown loses the host.
        yield* snapshot.pipe(
          Effect.flatMap((value) =>
            fs.writeFileString(
              path.join(options.outputDirectory, "final-snapshot.json"),
              Schema.encodeSync(Schema.fromJsonString(PerformanceSnapshot))(value),
            ),
          ),
          Effect.ignore,
        );
        const close = yield* post("/close", {}).pipe(Effect.timeout("15 seconds"), Effect.exit);

        result = { ...result, closeRequested: Exit.isSuccess(close) };
        yield* save();
      }),
    ),
    Effect.catchCause(() => Effect.void),
  );

  return result;
});
