import { Effect, Schema } from "effect";

import { requireReplayCpu } from "./replay-cpu-build.ts";
import { ReplayCpuOperation, ReplayCpuRole } from "./replay-cpu-contracts.ts";

const optionalString = Schema.optionalKey(Schema.NullOr(Schema.String));
const optionalNumber = Schema.optionalKey(Schema.NullOr(Schema.Number));

const TelemetryResponse = Schema.Struct({
  success: Schema.Literal(true),
  result: Schema.Struct({
    events: Schema.Struct({
      count: Schema.Natural,
      events: Schema.Array(
        Schema.Struct({
          timestamp: Schema.optionalKey(Schema.Union([Schema.String, Schema.Number])),
          $metadata: Schema.Struct({
            id: optionalString,
            type: optionalString,
            traceId: optionalString,
          }),
          $workers: Schema.Struct({
            cpuTimeMs: optionalNumber,
            wallTimeMs: optionalNumber,
            executionModel: optionalString,
            eventType: optionalString,
            durableObjectId: optionalString,
            scriptName: optionalString,
            requestId: optionalString,
            outcome: optionalString,
            scriptVersion: Schema.optionalKey(Schema.Struct({ id: Schema.String })),
            truncated: Schema.optionalKey(Schema.Boolean),
            event: Schema.optionalKey(
              Schema.Struct({
                rpcMethod: optionalString,
                rpcMethods: Schema.optionalKey(Schema.Array(Schema.String)),
                rpcCallCount: optionalNumber,
              }),
            ),
          }),
        }),
      ),
    }),
  }),
});

/** Drop headers, URLs and payloads at the API boundary; retain only invocation attribution. */
export const sanitizeReplayCpuTelemetry = Effect.fnUntraced(function* (input: unknown) {
  const parsed = yield* Schema.decodeUnknownEffect(TelemetryResponse)(input);
  const { count, events } = parsed.result.events;

  yield* requireReplayCpu(count === events.length && count < 2000, "Telemetry export is truncated");

  const sanitized = events.map(({ timestamp, $metadata: metadata, $workers: worker }) => {
    const methods = [...new Set(worker.event?.rpcMethods ?? [])];

    return {
      timestamp,
      id: metadata.id,
      metadataType: metadata.type,
      traceId: metadata.traceId,
      cpuTimeMs: worker.cpuTimeMs,
      wallTimeMs: worker.wallTimeMs,
      executionModel: worker.executionModel,
      eventType: worker.eventType,
      durableObjectId: worker.durableObjectId,
      scriptName: worker.scriptName,
      requestId: worker.requestId,
      outcome: worker.outcome,
      scriptVersion: worker.scriptVersion,
      truncated: worker.truncated,
      rpcMethod: worker.event?.rpcMethod ?? (methods.length === 1 ? methods[0] : null),
      rpcMethods: worker.event?.rpcMethods ?? [],
      rpcCallCount: worker.event?.rpcCallCount,
    };
  });

  yield* requireReplayCpu(
    new Set(sanitized.map((event) => event.id)).size === sanitized.length,
    "Duplicate telemetry events",
  );

  return sanitized;
});

export type ReplayCpuTelemetry = Effect.Success<ReturnType<typeof sanitizeReplayCpuTelemetry>>;

export const ReplayCpuSample = Schema.Struct({
  block: Schema.Natural,
  cohort: Schema.Natural,
  role: ReplayCpuRole,
  seedRecords: Schema.Literals([10, 1000]),
  operation: ReplayCpuOperation,
  objectId: Schema.String,
  invocationId: Schema.String,
  traceId: Schema.String,
  deploymentVersion: Schema.String,
  cpuMs: Schema.Number,
  workerWallMs: Schema.Number,
  ingressCpuMs: Schema.Number,
  clientElapsedMs: Schema.Number,
  promptSha256: Schema.String,
});

export const telemetryFor = (events: ReplayCpuTelemetry, objectId: string, method: string) =>
  events.filter(
    (event) =>
      event.metadataType === "cf-worker-event" &&
      event.executionModel === "durableObject" &&
      event.durableObjectId === objectId &&
      event.rpcMethod === method,
  );

const median = (values: ReadonlyArray<number>) => {
  const sorted = [...values].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);

  return sorted.length % 2 === 1
    ? Schema.decodeUnknownSync(Schema.Number)(sorted[middle])
    : (Schema.decodeUnknownSync(Schema.Number)(sorted[middle - 1]) +
        Schema.decodeUnknownSync(Schema.Number)(sorted[middle])) /
        2;
};

const stats = (values: ReadonlyArray<number>) => ({
  n: values.length,
  median: median(values),
  min: Math.min(...values),
  max: Math.max(...values),
  values,
});

export const summarizeReplayCpu = (samples: ReadonlyArray<typeof ReplayCpuSample.Type>) => {
  const rows = [];

  for (const seedRecords of [10, 1000]) {
    for (const cycle of ["initial", "warmed"] as const) {
      const phases = cycle === "initial" ? [1, 4] : [6, 9];

      const values = (role: typeof ReplayCpuRole.Type, block?: number) => {
        const groups = new Map<string, number>();

        for (const sample of samples.filter(
          (s) =>
            s.role === role &&
            s.seedRecords === seedRecords &&
            phases.includes(s.operation.phase) &&
            (block === undefined || s.block === block),
        )) {
          const key = `${sample.block}:${sample.cohort}`;

          groups.set(key, (groups.get(key) ?? 0) + sample.cpuMs);
        }

        return [...groups.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([, n]) => n);
      };

      const baseline = values("baseline");
      const candidate = values("candidate");
      const control = values("control");

      const blocks = [0, 1, 2].map((block) => {
        const b = values("baseline", block);
        const c = values("candidate", block);
        const a = values("control", block);

        return {
          block,
          candidateRatio: median(
            c.map((n, i) => n / Schema.decodeUnknownSync(Schema.Number)(b[i])),
          ),
          controlRatio: median(a.map((n, i) => n / Schema.decodeUnknownSync(Schema.Number)(b[i]))),
        };
      });

      rows.push({
        seedRecords,
        cycle,
        baseline: stats(baseline),
        candidate: stats(candidate),
        control: stats(control),
        pairedCandidateRatios: stats(
          candidate.map((n, i) => n / Schema.decodeUnknownSync(Schema.Number)(baseline[i])),
        ),
        pairedControlRatios: stats(
          control.map((n, i) => n / Schema.decodeUnknownSync(Schema.Number)(baseline[i])),
        ),
        blocks,
        comparisonCriterionMet: blocks.every(
          (b) => b.candidateRatio <= 0.9 && 1 - b.candidateRatio > Math.abs(1 - b.controlRatio),
        ),
      });
    }
  }

  return rows;
};
