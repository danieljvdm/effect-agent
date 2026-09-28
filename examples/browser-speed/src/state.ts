import { Cause, Effect, Option, Predicate, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { AsyncResult, Atom, AtomHttpApi, Reactivity } from "effect/unstable/reactivity";

import { LabApi, Report, type BrowserEngine, type ModelId, type RunInput } from "./contract.ts";

export class LabClient extends AtomHttpApi.Service<LabClient>()("browser-speed/client", {
  api: LabApi,
  httpClient: FetchHttpClient.layer,
  baseUrl: "",
}) {}

const sessionId = crypto.randomUUID();

export const watchingAtom = Atom.make(false);
const stopRequestedAtom = Atom.make(false);

export const progressAtom = Atom.make({ current: 0, total: 0 });
export const submittedAtom = Atom.make<string | null>(null);
export const ClientSample = Schema.Struct({ report: Report, clientElapsedMillis: Schema.Number });
export type ClientSample = typeof ClientSample.Type;
export const historyAtom = Atom.make<ReadonlyArray<ClientSample>>([]);
export const selectedAtom = Atom.make<string | null>(null);

const headers = { "x-lab-session": sessionId };

const snapshotQuery = LabClient.query("lab", "snapshot", {
  headers,
  reactivityKeys: ["lab"],
  timeToLive: "5 seconds",
});

const pollingSnapshot = snapshotQuery.pipe(Atom.withRefresh("300 millis"));

export const snapshotAtom = Atom.make((get) =>
  get(get(watchingAtom) ? pollingSnapshot : snapshotQuery),
);

export const runAtom = LabClient.runtime.fn<
  Omit<RunInput, "id"> & {
    readonly repetitions: number;
    readonly compareModels?: ReadonlyArray<ModelId>;
    readonly compareEngines?: ReadonlyArray<typeof BrowserEngine.Type>;
  }
>()(
  Effect.fnUntraced(function* (input, get) {
    const client = yield* LabClient;

    get.set(watchingAtom, true);
    get.set(stopRequestedAtom, false);
    get.set(selectedAtom, null);
    get.set(submittedAtom, input.prompt);

    return yield* Effect.gen(function* () {
      const models =
        input.wikiDriver === "jev"
          ? [undefined]
          : input.compareModels?.length
            ? input.compareModels
            : [input.model];

      const engines = input.compareEngines?.length
        ? input.compareEngines
        : [input.engine ?? "chromium"];

      const variants = models.flatMap((model) => engines.map((engine) => ({ model, engine })));

      // Rotate the first configuration each round; each run retains the same task and limits.
      const attempts = Array.from({ length: input.repetitions }, (_, round) =>
        variants.map((variant, index) => variants[(index + round) % variants.length] ?? variant),
      ).flat();

      for (const [i, { model, engine }] of attempts.entries()) {
        if (get(stopRequestedAtom)) break;
        get.set(progressAtom, { current: i + 1, total: attempts.length });
        const start = performance.now();

        const {
          repetitions: _,
          compareModels: __,
          model: ___,
          engine: _engine,
          compareEngines: _compareEngines,
          reasoning,
          serviceTier,
          ...request
        } = input;

        const report = yield* Reactivity.mutation(
          client.lab.run({
            headers,
            payload: {
              ...request,
              engine,
              ...(input.wikiDriver !== "jev" &&
              input.mode !== "scripted" &&
              !model?.startsWith("@cf/")
                ? {
                    ...(reasoning === undefined ? {} : { reasoning }),
                    ...(serviceTier === undefined ? {} : { serviceTier }),
                  }
                : {}),
              ...(model === undefined ? {} : { model }),
              id: crypto.randomUUID(),
            },
          }),
          ["lab"],
        );

        get.set(
          historyAtom,
          [...get(historyAtom), { report, clientElapsedMillis: performance.now() - start }].slice(
            -100,
          ),
        );
        if (
          report.status === "cancelled" ||
          report.cleanup === "failed" ||
          (report.status === "failed" && variants.length === 1)
        )
          break;
      }
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => get.set(watchingAtom, false)).pipe(
          Effect.andThen(Reactivity.invalidate(["lab"])),
        ),
      ),
    );
  }),
);

export const controlAtom = LabClient.runtime.fn<"stop" | "close">()(
  Effect.fnUntraced(function* (action, get) {
    const client = yield* LabClient;
    const request = { headers };

    if (action === "stop") {
      get.set(stopRequestedAtom, true);

      return yield* Reactivity.mutation(client.lab.stop(request), ["lab"]);
    }
    yield* Reactivity.mutation(client.lab.close(request), ["lab"]);
  }),
);

export const exportAtom = Atom.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const samples = get(historyAtom);

    yield* Effect.sync(() => {
      const json = Schema.encodeSync(Schema.fromJsonString(Schema.Array(ClientSample)))(samples);
      const url = URL.createObjectURL(new Blob([json], { type: "application/json" }));
      const link = document.createElement("a");

      link.href = url;
      link.download = "browser-speed-runs.json";
      link.click();
      URL.revokeObjectURL(url);
    });
  }),
);

export const errorMessage = <A, E>(result: AsyncResult.AsyncResult<A, E>) => {
  if (!AsyncResult.isFailure(result)) return null;

  const error = Option.flatMap(
    Cause.findErrorOption(result.cause),
    Schema.decodeUnknownOption(Schema.Struct({ message: Schema.String })),
  );

  if (Option.isSome(error)) return error.value.message;

  return "The connection to the Worker was interrupted. Check the browser status before running again.";
};

const sameWorkload = (value: Report, report: Report) =>
  value.input.scenario !== "custom" &&
  value.fixture === report.fixture &&
  value.input.scenario === report.input.scenario &&
  value.input.wikipedia?.start === report.input.wikipedia?.start &&
  value.input.wikipedia?.target === report.input.wikipedia?.target &&
  value.input.mode === report.input.mode &&
  value.timing === report.timing &&
  (value.timing === "page-ready-v1" || value.input.temperature === report.input.temperature) &&
  value.input.screenshots === report.input.screenshots &&
  value.input.liveView === report.input.liveView;

/** Timestamps stay admission-relative; displayed flow durations use the explicit ready boundary. */
export const fromReady = (report: Report, at: number | null | undefined): number | null =>
  Predicate.isNullish(at) ||
  (report.timing === "page-ready-v1" && Predicate.isNullish(report.readyAt))
    ? null
    : Math.max(0, at - (report.timing === "page-ready-v1" ? (report.readyAt ?? 0) : 0));

export const flowStarted = (report: Report) =>
  report.timing !== "page-ready-v1" || Predicate.isNotNullish(report.readyAt);

export const verifiedMillis = (report: Report) => fromReady(report, report.verifiedAt);

export const runOutcome = (report: Report) =>
  !flowStarted(report)
    ? report.status === "running"
      ? "preparing"
      : report.status === "cancelled"
        ? "preparation cancelled"
        : "preparation failed"
    : report.status;

/** Never pool page-ready timings with legacy admission timings. */
export const cohort = (samples: ReadonlyArray<ClientSample>, report: Report) =>
  samples.filter(
    ({ report: value }) =>
      sameWorkload(value, report) &&
      value.model === report.model &&
      settingsKey(value) === settingsKey(report) &&
      (value.input.grounding ?? "direct") === (report.input.grounding ?? "direct"),
  );

const servedTiers = (reports: ReadonlyArray<Report>) =>
  [
    ...new Set(
      reports.flatMap((report) => {
        const spans = report.spans.filter((span) => span.phase === "model");

        return spans.length ? spans.map((span) => span.serviceTier ?? "unknown") : ["unknown"];
      }),
    ),
  ]
    .sort()
    .join("+") || "unknown";

const settingsKey = (report: Report) =>
  `${report.input.engine ?? "chromium"}/${report.browserVersion ?? "unrecorded"}/${report.browserRevision ?? "unrecorded"}/${report.commandTimeoutMillis ?? 15_000}/${report.input.wikiDriver ?? "model"}/${report.input.reasoning ?? "provider-default"}/${report.input.serviceTier ?? "provider-default"}`;

export const comparisons = (samples: ReadonlyArray<ClientSample>, report: Report) => {
  const groups = new Map<string, Array<Report>>();

  for (const { report: candidate } of samples) {
    if (!sameWorkload(candidate, report)) continue;
    const key = `${candidate.model}/${candidate.input.grounding ?? "direct"}/${settingsKey(candidate)}`;
    const group = groups.get(key) ?? [];

    group.push(candidate);
    groups.set(key, group);
  }

  return [...groups.entries()].map(([key, reports]) => ({
    key,
    engine: reports[0]?.input.engine ?? "chromium",
    browserVersion: reports[0]?.browserVersion,
    browserRevision: reports[0]?.browserRevision,
    model: reports[0]?.model ?? "none",
    wikiDriver: reports[0]?.input.wikiDriver ?? "model",
    grounding: reports[0]?.input.grounding ?? "direct",
    reasoning:
      reports[0]?.input.wikiDriver === "jev"
        ? "n/a"
        : (reports[0]?.input.reasoning ??
          (reports[0]?.model.startsWith("@cf/") ? "n/a" : "provider-default")),
    serviceTier:
      reports[0]?.input.wikiDriver === "jev"
        ? "n/a"
        : (reports[0]?.input.serviceTier ?? "provider-default"),
    servedTier: reports[0]?.input.wikiDriver === "jev" ? "n/a" : servedTiers(reports),
    count: reports.length,
    started: reports.filter(flowStarted).length,
    preparationFailed: reports.filter((value) => !flowStarted(value) && value.status !== "running")
      .length,
    passed: reports.filter((value) => value.status === "passed").length,
    hops: percentile(
      reports.flatMap((value) =>
        value.status === "passed" && value.race ? [value.race.path.length - 1] : [],
      ),
      0.5,
    ),
    median: percentile(
      reports.flatMap((value) => {
        const time = verifiedMillis(value);

        return value.status === "passed" && time !== null ? [time] : [];
      }),
      0.5,
    ),
  }));
};

export const percentile = (values: ReadonlyArray<number>, fraction: number): number | null => {
  const sorted = [...values].sort((a, b) => a - b);

  if (fraction === 0.5 && sorted.length > 0) {
    const middle = Math.floor(sorted.length / 2);

    return ((sorted[Math.ceil(sorted.length / 2) - 1] ?? 0) + (sorted[middle] ?? 0)) / 2;
  }

  return sorted[Math.max(0, Math.ceil(sorted.length * fraction) - 1)] ?? null;
};
