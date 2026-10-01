import { isDeepStrictEqual } from "node:util";

import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import {
  Cause,
  Clock,
  Config,
  Effect,
  Exit,
  FileSystem,
  Option,
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { Evidence, type Receipt, RunId, WorkerFailure } from "../src/proof.ts";
import { checkoutStack } from "../src/stack.ts";

const lifecycle = Test.make({ providers: Cloudflare.providers(), dev: false });
const Commit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));
const Failure = Schema.Struct({ stage: Schema.String, status: Schema.NullOr(Schema.Int) });

class GateFailure extends Schema.TaggedError<GateFailure>()(
  "CheckoutGateFailure",
  Failure.fields,
) {}
const fail = (stage: string, status: number | null = null) => GateFailure.make({ stage, status });

const safeFailure = <E>(cause: Cause.Cause<E>) => {
  const error = Cause.findErrorOption(cause);

  return Option.isSome(error) && Schema.is(GateFailure)(error.value)
    ? error.value
    : fail("interrupted-or-unexpected");
};

const Report = Schema.Struct({
  version: Schema.Literal(3),
  runId: RunId,
  workerName: Schema.String,
  sourceCommit: Commit,
  model: Schema.String,
  dispatched: Schema.Boolean,
  passed: Schema.Boolean,
  evidence: Schema.NullOr(Evidence),
  failure: Schema.NullOr(Failure),
  cleanup: Schema.Literals(["pending", "browsers-closed", "confirmed", "failed"]),
  timings: Schema.Struct({
    totalMillis: Schema.Natural,
    checkoutMillis: Schema.Natural,
    cleanupMillis: Schema.Natural,
  }),
});

// Independent oracle: never derive expected values from the receiver or its receipt schema.
const expectedReceipt = {
  buyer: "buyer@example.test",
  product: "everyday-shirt",
  color: "blue",
  size: "M",
  quantity: 1,
  address: "123 Test Street, San Francisco, CA 94107, US",
  shipping: "standard",
  subtotal: 3400,
  shippingCents: 500,
  tax: 312,
  total: 4212,
  currency: "USD",
  paid: true,
} satisfies typeof Receipt.Type;

const receiptMatches = (evidence: typeof Evidence.Type) =>
  isDeepStrictEqual(evidence.receipt, expectedReceipt);

// Persist only known fixture values. A malformed receipt or free-form failure is never published.
const publicEvidence = (evidence: typeof Evidence.Type): typeof Evidence.Type => ({
  phase: evidence.phase,
  started: evidence.started,
  attempts: evidence.attempts,
  receipt: receiptMatches(evidence) ? evidence.receipt : null,
  closed: evidence.closed,
  scrapeAttempts: evidence.scrapeAttempts,
  failure:
    evidence.failure === null
      ? null
      : Option.getOrElse(
          Schema.decodeUnknownOption(WorkerFailure)(evidence.failure),
          () => "worker-failure",
        ),
});

const proof = Effect.gen(function* () {
  const totalStart = yield* Clock.monotonicTimeNanos;
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const client = yield* HttpClient.HttpClient;

  const run = yield* Config.schema(RunId, "CHECKOUT_RUN_ID").pipe(
    Effect.mapError(() => fail("configuration:CHECKOUT_RUN_ID")),
  );

  const cleanupOnly = yield* Config.Boolean("CHECKOUT_CLEANUP").pipe(
    Config.withDefault(false),
    Effect.mapError(() => fail("configuration:CHECKOUT_CLEANUP")),
  );

  const expectedCommit = yield* Config.schema(Commit, "CHECKOUT_EXPECTED_SHA").pipe(
    Effect.mapError(() => fail("configuration:CHECKOUT_EXPECTED_SHA")),
  );

  const git = Effect.fnUntraced(function* (args: ReadonlyArray<string>) {
    return (yield* spawner
      .string(ChildProcess.make("git", args))
      .pipe(Effect.mapError(() => fail("revision:git")))).trim();
  });

  const sourceCommit = yield* git(["rev-parse", "HEAD"]);

  if (sourceCommit !== expectedCommit) return yield* fail("revision:sha-mismatch");
  if ((yield* git(["status", "--porcelain"])) !== "") return yield* fail("revision:dirty");
  if (yield* Config.Boolean("ALCHEMY_TEST_DEV").pipe(Config.withDefault(false)))
    return yield* fail("configuration:hosted-required");

  // Validate bindings before Alchemy provisions anything, including during recovery.
  for (const name of [
    "CHECKOUT_MODEL",
    "CHECKOUT_TOKEN",
    "CHECKOUT_PASSWORD",
    "OPENAI_API_KEY",
    "CLOUDFLARE_ACCOUNT_ID",
    "CLOUDFLARE_API_TOKEN",
    "BROWSER_RENDERING_API_TOKEN",
  ]) {
    const value = yield* Config.Redacted(name).pipe(
      Effect.mapError(() => fail("configuration:" + name)),
    );

    if (Redacted.value(value).length === 0) return yield* fail("configuration:" + name);
  }

  const subdomain = yield* Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/)),
    "CLOUDFLARE_WORKERS_SUBDOMAIN",
  ).pipe(Effect.mapError(() => fail("configuration:CLOUDFLARE_WORKERS_SUBDOMAIN")));

  const model = yield* Config.NonEmptyString("CHECKOUT_MODEL");
  const token = yield* Config.Redacted("CHECKOUT_TOKEN");
  const cloudflareToken = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");
  const account = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
  const workerName = "ea-checkout-" + run;
  const origin = "https://" + workerName + "." + subdomain + ".workers.dev";
  const directory = ".checkout-proof/" + run;
  const reportPath = directory + "/report.json";
  let report: typeof Report.Type;

  const workerStatus = client
    .execute(
      HttpClientRequest.get(
        "https://api.cloudflare.com/client/v4/accounts/" +
          account +
          "/workers/scripts/" +
          workerName,
      ).pipe(HttpClientRequest.bearerToken(cloudflareToken)),
    )
    .pipe(
      Effect.map((response) => response.status),
      Effect.timeout("30 seconds"),
      Effect.mapError(() => fail("worker-status")),
    );

  const save = Effect.fnUntraced(
    function* () {
      const json = yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report);

      yield* fs.writeFileString(reportPath + ".tmp", json, { mode: 0o600 });
      yield* fs.rename(reportPath + ".tmp", reportPath);
    },
    Effect.mapError(() => fail("report-write")),
  );

  const measure = Effect.fnUntraced(function* <A, E, R>(
    phase: "checkoutMillis" | "cleanupMillis",
    effect: Effect.Effect<A, E, R>,
  ) {
    if (cleanupOnly) return yield* effect;
    const start = yield* Clock.monotonicTimeNanos;

    return yield* effect.pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          const millis = Number((yield* Clock.monotonicTimeNanos) - start) / 1_000_000;

          report = { ...report, timings: { ...report.timings, [phase]: Math.round(millis) } };
          yield* save();
        }).pipe(Effect.orDie),
      ),
    );
  });

  const request = Effect.fnUntraced(function* (operation: "run" | "evidence" | "close") {
    const response = yield* client
      .execute(
        HttpClientRequest.make(operation === "evidence" ? "GET" : "POST")(
          origin + "/" + operation,
        ).pipe(HttpClientRequest.bearerToken(token)),
      )
      .pipe(
        Effect.timeout(operation === "run" ? "8 minutes" : "1 minute"),
        Effect.mapError(() => fail(operation + ":transport-or-timeout")),
      );

    if (response.status !== 200 && !(operation === "run" && response.status === 502))
      return yield* fail(operation, response.status);

    const body = yield* response.text.pipe(
      Effect.mapError(() => fail(operation + ":transport-or-timeout", response.status)),
    );

    const evidence = yield* Schema.decodeEffect(Schema.fromJsonString(Evidence))(body).pipe(
      Effect.mapError(() => fail(operation + ":invalid-evidence", response.status)),
    );

    return { status: response.status, evidence };
  });

  if (cleanupOnly) {
    report = yield* fs.readFileString(reportPath).pipe(
      Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))),
      Effect.mapError(() => fail("recovery:report")),
    );
    if (
      report.runId !== run ||
      report.workerName !== workerName ||
      report.sourceCommit !== sourceCommit
    )
      return yield* fail("recovery:identity-mismatch");
  } else {
    if (yield* fs.exists(reportPath)) return yield* fail("run-id:already-recorded");
    const status = yield* workerStatus;

    if (status !== 404) return yield* fail("worker-name:unavailable", status);
    yield* fs.makeDirectory(directory, { recursive: true });
    report = {
      version: 3,
      runId: run,
      workerName,
      sourceCommit,
      model,
      dispatched: false,
      passed: false,
      evidence: null,
      failure: null,
      cleanup: "pending",
      timings: { totalMillis: 0, checkoutMillis: 0, cleanupMillis: 0 },
    };
    yield* save();
  }

  const retire = measure(
    "cleanupMillis",
    Effect.gen(function* () {
      const status = yield* workerStatus;

      if (status === 200) {
        // Closing is idempotent; unlike /run, explicit recovery may repeat it.
        const closed = yield* request("close");

        report = { ...report, evidence: publicEvidence(closed.evidence) };
        yield* save();
        if (!closed.evidence.closed) return yield* fail("cleanup:closure-unconfirmed");
        const observed = yield* request("evidence");

        report = { ...report, evidence: publicEvidence(observed.evidence) };
        yield* save();
        if (!observed.evidence.closed) return yield* fail("cleanup:closure-unconfirmed");
      } else if (status !== 404) {
        return yield* fail("cleanup:worker-status", status);
      } else if (report.dispatched && report.evidence?.closed !== true) {
        return yield* fail("cleanup:owner-absent-without-closure");
      }
      // Persist closure before teardown. A failed close preserves the owner and its alarm.
      report = { ...report, cleanup: "browsers-closed" };
      yield* save();
      yield* lifecycle
        .destroy(checkoutStack, { stage: run })
        .pipe(Effect.mapError(() => fail("cleanup:destroy")));
      const absent = yield* workerStatus;

      if (absent !== 404) return yield* fail("cleanup:worker-remains", absent);
      report = { ...report, cleanup: "confirmed" };
      yield* save();
    }).pipe(
      Effect.timeout("5 minutes"),
      Effect.catchCause((cause) =>
        Effect.gen(function* () {
          const failure = safeFailure(cause);

          report = {
            ...report,
            passed: false,
            cleanup: "failed",
            failure: report.failure ?? failure,
          };
          yield* save();

          return yield* failure;
        }),
      ),
    ),
  );

  const checkout = Effect.gen(function* () {
    const deployed = yield* lifecycle
      .deploy(checkoutStack, { stage: run })
      .pipe(Effect.mapError(() => fail("deployment")));

    if (deployed.workerName !== workerName || deployed.url !== origin)
      return yield* fail("deployment:origin-mismatch");
    // Wait for the fresh namespace's authenticated receiver before the one-shot dispatch.
    yield* request("evidence").pipe(
      Effect.filterOrFail(
        ({ evidence }) =>
          !evidence.started &&
          evidence.attempts === 0 &&
          evidence.receipt === null &&
          evidence.closed &&
          evidence.scrapeAttempts === 0 &&
          evidence.failure === null,
        () => fail("readiness:owner-not-pristine"),
      ),
      Effect.timeout("10 seconds"),
      Effect.retry({
        schedule: Schedule.spaced("2 seconds"),
        while: (error) =>
          error._tag === "TimeoutError" ||
          (error._tag === "CheckoutGateFailure" &&
            (error.stage === "evidence:transport-or-timeout" ||
              (error.stage === "evidence" &&
                error.status !== null &&
                (error.status === 404 || (error.status >= 500 && error.status <= 599))))),
      }),
      Effect.timeout("1 minute"),
      Effect.mapError((error) =>
        fail("readiness", error._tag === "CheckoutGateFailure" ? error.status : null),
      ),
    );
    // Persist before sending. A lost response must never cause another /run.
    report = { ...report, dispatched: true };
    yield* save();
    const executed = yield* measure("checkoutMillis", request("run")).pipe(Effect.exit);
    const observed = yield* request("evidence");

    report = { ...report, evidence: publicEvidence(observed.evidence) };
    yield* save();
    if (Exit.isFailure(executed)) return yield* safeFailure(executed.cause);
    if (executed.value.status !== 200)
      return yield* fail(
        `run:${observed.evidence.phase}:${publicEvidence(observed.evidence).failure}`,
        executed.value.status,
      );
    const evidence = observed.evidence;

    if (
      !evidence.started ||
      !evidence.closed ||
      evidence.attempts !== 1 ||
      !receiptMatches(evidence)
    )
      return yield* fail("receipt:mismatch");
    if (evidence.scrapeAttempts < 1 || evidence.scrapeAttempts > 3 || evidence.failure !== null)
      return yield* fail("checkout:worker-failure");
  });

  yield* checkout.pipe(
    Effect.when(Effect.succeed(!cleanupOnly)),
    Effect.onExit((exit) =>
      Exit.isFailure(exit)
        ? Effect.gen(function* () {
            report = { ...report, passed: false, failure: safeFailure(exit.cause) };
            yield* save();
          }).pipe(Effect.orDie)
        : Effect.void,
    ),
    Effect.ensuring(retire.pipe(Effect.orDie)),
    Effect.ensuring(
      Effect.gen(function* () {
        if (!cleanupOnly) {
          const millis = Number((yield* Clock.monotonicTimeNanos) - totalStart) / 1_000_000;

          report = { ...report, timings: { ...report.timings, totalMillis: Math.round(millis) } };
        }
        yield* save();
      }).pipe(Effect.orDie),
    ),
  );
  if (!cleanupOnly) {
    const evidence = report.evidence;

    if (
      report.cleanup !== "confirmed" ||
      evidence === null ||
      !evidence.closed ||
      !evidence.started ||
      evidence.attempts !== 1 ||
      !receiptMatches(evidence) ||
      evidence.failure !== null
    ) {
      const failure = fail("checkout:incomplete");

      report = { ...report, failure };
      yield* save();

      return yield* failure;
    }
    report = { ...report, passed: true };
    yield* save();
  }
}).pipe(
  Effect.catchCause((cause) => Effect.fail(safeFailure(cause))),
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
);

lifecycle.test("submits once and verifies the independent receipt and browser closure", proof);
