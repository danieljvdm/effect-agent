import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import { Cause, Clock, Config, Console, Effect, Exit, FileSystem, Layer, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  type CheckoutFlow,
  type CheckoutScenario,
  AgentOutput,
  CheckoutConcurrency,
  CheckoutController,
  failure,
  policy,
  Report,
  RunEvidence,
  StartIntervalMillis,
  type Timings,
} from "../src/checkout-contract.ts";
import {
  CheckoutReport,
  makeCaseAdmission,
  retirementPlan,
  withRetirement,
} from "../src/checkout-lifecycle.ts";
import { checkoutStack } from "../src/checkout-stack.ts";
import { assertPurchase, expectedQuote, sameQuote } from "../src/checkout-store.ts";
import { BrowserRunWorkerProofResult } from "../src/contract.ts";

// Only prove:live collects this file. The same stage is reusable only for explicit retirement.
const lifecycle = Test.make({ providers: Cloudflare.providers(), dev: false });

const config = Config.all({
  run: Config.schema(Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/)), "CHECKOUT_RUN_ID"),
  model: Config.NonEmptyString("CHECKOUT_MODEL"),
  controller: Config.schema(CheckoutController, "CHECKOUT_CONTROLLER").pipe(
    Config.withDefault("baseline"),
  ),
  token: Config.Redacted("CHECKOUT_TOKEN"),
  repetitions: Config.Int("CHECKOUT_REPETITIONS").pipe(Config.withDefault(1)),
  concurrency: Config.schema(CheckoutConcurrency, "CHECKOUT_CONCURRENCY").pipe(
    Config.withDefault(4),
  ),
  startIntervalMillis: Config.schema(StartIntervalMillis, "CHECKOUT_START_INTERVAL_MS").pipe(
    Config.withDefault(1_000),
  ),
  human: Config.Boolean("CHECKOUT_HUMAN").pipe(Config.withDefault(false)),
  cleanupOnly: Config.Boolean("CHECKOUT_CLEANUP").pipe(Config.withDefault(false)),
});

let shopUrl: string | undefined;
let names: ReadonlyArray<string> = [];
const started: Array<string> = [];
let ownsStage = false;

const currentReport = Effect.gen(function* () {
  return yield* (yield* CheckoutReport).get;
});

const updateReport = Effect.fnUntraced(function* (
  f: (report: typeof Report.Type) => typeof Report.Type,
) {
  yield* (yield* CheckoutReport).update((report) => (report === undefined ? undefined : f(report)));
});

const measure = Effect.fnUntraced(function* <A, E, R>(
  phase: keyof typeof Timings.Type,
  effect: Effect.Effect<A, E, R>,
) {
  if ((yield* config).cleanupOnly) return yield* effect;
  const start = yield* Clock.monotonicTimeNanos;

  return yield* effect.pipe(
    Effect.ensuring(
      Effect.gen(function* () {
        const elapsed = Math.max(
          0,
          Math.round(Number((yield* Clock.monotonicTimeNanos) - start) / 1_000_000),
        );

        yield* updateReport((report) => ({
          ...report,
          timings: { ...report.timings, [phase]: elapsed },
        }));
      }).pipe(Effect.orDie),
    ),
  );
});

const workerStatus = Effect.fnUntraced(function* (name: string) {
  const client = yield* HttpClient.HttpClient;
  const account = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
  const token = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");

  const response = yield* client.execute(
    HttpClientRequest.get(
      `https://api.cloudflare.com/client/v4/accounts/${account}/workers/scripts/${name}`,
    ).pipe(HttpClientRequest.bearerToken(token)),
  );

  return response.status;
});

const call = Effect.fn("CheckoutProof.request")(function* <
  S extends Schema.Top & { readonly DecodingServices: never },
>(key: string, operation: string, schema: S, body?: unknown) {
  const { token } = yield* config;
  const client = yield* HttpClient.HttpClient;

  if (shopUrl === undefined) return yield* failure("deployment", "The shop URL is unavailable");

  const request = HttpClientRequest.make(body === undefined ? "GET" : "POST")(
    `${shopUrl}/_control/${key}/${operation}`,
  ).pipe(HttpClientRequest.bearerToken(token));

  const response = yield* client.execute(
    body === undefined ? request : HttpClientRequest.bodyJsonUnsafe(request, body),
  );

  if (response.status !== 200)
    return yield* failure(operation, `HTTP ${response.status}; mutation was not retried`);

  return yield* response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));
}, Effect.timeout("6 minutes"));

const initialize = Effect.gen(function* () {
  const settings = yield* config;

  if (settings.repetitions < 1 || settings.repetitions > 5)
    return yield* failure("configuration", "CHECKOUT_REPETITIONS must be 1..5");
  if (yield* Config.Boolean("ALCHEMY_TEST_DEV").pipe(Config.withDefault(false)))
    return yield* failure("configuration", "The hosted proof refuses ALCHEMY_TEST_DEV");
  names = [
    `ea-checkout-${settings.run}-shop`,
    `ea-checkout-${settings.run}-pay`,
    `ea-checkout-${settings.run}-binding`,
  ];
  const fs = yield* FileSystem.FileSystem;
  const reportPath = `.checkout-proof/${settings.run}/report.json`;

  if (settings.cleanupOnly) {
    const report = yield* fs
      .readFileString(reportPath)
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))));

    yield* (yield* CheckoutReport).update(() => report);
    const subdomain = yield* Config.NonEmptyString("CLOUDFLARE_WORKERS_SUBDOMAIN");

    shopUrl = `https://${names[0]}.${subdomain}.workers.dev`;
    started.push(...report.results.map((result) => result.key));
    ownsStage = true;

    return;
  }
  if (yield* fs.exists(reportPath))
    return yield* failure(
      "configuration",
      "Run ID already has evidence. Use a fresh ID or CHECKOUT_CLEANUP=true; never overwrite a failed run.",
    );
  for (const name of names) {
    const status = yield* workerStatus(name);

    if (status !== 404)
      return yield* failure("deployment", `Worker name ${name} is unavailable (HTTP ${status})`);
  }
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const sourceCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"]),
  )).trim();

  const dirty =
    (yield* spawner.string(ChildProcess.make("git", ["status", "--porcelain"]))).trim().length > 0;

  yield* fs.makeDirectory(`.checkout-proof/${settings.run}`, { recursive: true });
  yield* (yield* CheckoutReport).update(() => ({
    version: 1,
    model: settings.model,
    sourceCommit,
    dirty,
    controller: settings.controller,
    repetitions: settings.repetitions,
    profile: settings.human ? "operator" : "automated",
    execution: {
      concurrency: settings.concurrency,
      startIntervalMillis: settings.startIntervalMillis,
    },
    timings: {},
    measurement: {
      version: 1,
      clock: "monotonic-per-request",
      queue: "excluded-from-case-included-in-matrix",
      caseBoundary: "before-seed-through-exact-browser-closure",
      browserProtocolCalls: "unavailable",
      cost: "unpriced",
      maxOutputTokens: 4_096,
    },
    bindingProof: false,
    suiteFailure: null,
    configuration: policy,
    results: [],
    completed: 0,
    attempted: 0,
    completionRate: 0,
    cleanup: "pending",
    providerCompatibility: "not-established",
  }));
  ownsStage = true;
});

const task = (key: string, scenario: typeof CheckoutScenario.Type) =>
  `Buy one blue medium Everyday Shirt from ${shopUrl}/s/${key}/. Use my saved account and card, ship to Alex Example, 123 Test Street, San Francisco, CA 94107, US, choose standard shipping, and keep the total below $50 USD. Ask before placing the order. If the primary card is explicitly declined, use my backup card and ask again. ${scenario === "handoff" ? "Ask me to take over when the wallet asks for its verification code; do not enter that code yourself." : "You may enter the dummy verification code shown by the test wallet."}`;

const awaitHuman = Effect.fnUntraced(function* (key: string) {
  const view = yield* call(key, "human", Schema.Struct({ liveView: Schema.String }), {});
  const fs = yield* FileSystem.FileSystem;
  const { run } = yield* config;
  const viewPath = `.checkout-proof/${run}/live-view.txt`;

  yield* fs.writeFileString(viewPath, view.liveView, { mode: 0o600 });
  yield* Console.log(
    `Human needed: open the private URL in ${viewPath}, enter 246810 and verify within five minutes; select Done if Live View offers it.`,
  );

  return yield* Effect.gen(function* () {
    for (let poll = 0; poll < 60; poll++) {
      yield* Effect.sleep("5 seconds");

      // This idempotent ownership check cannot submit a payment or replay an agent request.
      const returned = yield* call(
        key,
        "return",
        Schema.Struct({ returned: Schema.Boolean }),
        {},
      ).pipe(Effect.exit);

      if (Exit.isSuccess(returned)) return;
    }

    return yield* failure(
      "handoff",
      "The verified human handoff did not become resumable within five minutes",
    );
  }).pipe(Effect.ensuring(fs.remove(viewPath, { force: true }).pipe(Effect.orDie)));
});

const scenario = Effect.fn("CheckoutProof.scenario")(function* (
  key: string,
  flow: typeof CheckoutFlow.Type,
  kind: typeof CheckoutScenario.Type,
) {
  // A lost seed response still belongs to this attempt and must be reconciled during retirement.
  started.push(key);
  yield* call(key, "seed", Schema.Struct({ seeded: Schema.Boolean }), {
    key,
    flow,
    scenario: kind,
  });
  let message = task(key, kind);
  let approvals = 0;
  let handoffs = 0;

  for (let request = 0; request < 6; request++) {
    const output = yield* call(key, "run", AgentOutput, { message });
    const evidence = yield* call(key, "evidence", RunEvidence);

    if (output.status === "approval-required") {
      const pending = evidence.control.pendingApproval;

      if (
        pending === null ||
        evidence.control.controller !== "approval" ||
        evidence.shop.orders.length !== 0 ||
        !sameQuote(pending, expectedQuote)
      )
        return yield* failure(
          "approval",
          "The paused checkout is not the requested unpurchased order",
        );
      // The agent cannot grant its own approval. A separate authenticated request grants this exact quote.
      yield* call(key, "approve", Schema.Struct({ approved: Schema.Boolean }), { quote: pending });
      approvals++;
      message = `${task(key, kind)} I approve the order you just showed me for $42.12 USD. Continue from the existing browser, inspect its current state, and place this approved order once. If its outcome is uncertain, inspect order history instead of submitting again.`;
      continue;
    }
    if (output.status === "human-required") {
      if (
        kind !== "handoff" ||
        evidence.control.controller !== "human" ||
        evidence.shop.walletVerified ||
        !evidence.shop.authenticated ||
        evidence.shop.cart === null ||
        evidence.shop.orders.length !== 0
      )
        return yield* failure("handoff", "Expected an authenticated, unverified human checkout");
      yield* awaitHuman(key);
      handoffs++;
      message = `${task(key, kind)} I completed verification in your existing browser and returned control. Continue, inspect the current page, and ask before placing the order.`;
      continue;
    }
    if (output.status !== "complete")
      return yield* failure("agent", `${output.status}: ${output.message}`);
    yield* assertPurchase(evidence.shop);
    if (
      approvals !== (kind === "correction" ? 2 : 1) ||
      !evidence.browserIdentityUnchanged ||
      (kind === "handoff" && (handoffs !== 1 || !evidence.control.humanReturned))
    )
      return yield* failure("boundaries", "Approval or browser continuity evidence is incomplete");

    return;
  }

  return yield* failure("budget", "Too many agent continuations");
});

const proof = Effect.gen(function* () {
  yield* initialize;
  const { run, repetitions, human, cleanupOnly, concurrency, startIntervalMillis } = yield* config;

  if (cleanupOnly) return;

  const deployed = yield* measure(
    "deploymentMillis",
    lifecycle.deploy(checkoutStack, { stage: run }),
  );

  if (!deployed.shopUrl || !deployed.processorUrl || !deployed.bindingUrl)
    return yield* failure("deployment", "Three HTTPS origins are required");
  shopUrl = deployed.shopUrl;
  const bindingUrl = deployed.bindingUrl;

  yield* measure("readinessMillis", Effect.sleep("15 seconds"));
  const client = yield* HttpClient.HttpClient;

  yield* measure(
    "bindingProofMillis",
    Effect.gen(function* () {
      const bindingResponse = yield* client.get(bindingUrl).pipe(Effect.timeout("150 seconds"));

      if (bindingResponse.status !== 200)
        return yield* failure(
          "binding-proof",
          `HTTP ${bindingResponse.status}; invocation was not retried`,
        );
      yield* bindingResponse.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(BrowserRunWorkerProofResult)),
      );
      yield* updateReport((report) => ({ ...report, bindingProof: true }));
    }),
  );

  const cases = [
    { flow: "embedded-card", scenario: "success" },
    { flow: "embedded-card", scenario: "correction" },
    { flow: "embedded-card", scenario: "ambiguous" },
    { flow: "accelerated", scenario: "success" },
    { flow: "accelerated", scenario: "correction" },
    { flow: "accelerated", scenario: "ambiguous" },
  ] as const;

  const scheduled = [
    ...(human ? [{ flow: "accelerated", scenario: "handoff", repetition: 1 } as const] : []),
    ...Array.from({ length: repetitions }, (_, index) => index + 1).flatMap((repetition) =>
      cases.map((item) => ({ ...item, repetition })),
    ),
  ];

  const runCase = Effect.fnUntraced(function* ({
    repetition,
    ...item
  }: (typeof scheduled)[number]) {
    const key = `${run}-${item.flow}-${item.scenario}-${repetition}`;
    const start = yield* Clock.monotonicTimeNanos;

    yield* Console.log(`Checkout ${key}`);
    // Persist the denominator before dispatch: interruption cannot erase an unsuccessful attempt.
    yield* updateReport((report) => ({
      ...report,
      results: [
        ...report.results,
        {
          key,
          ...item,
          passed: false,
          failure: "Interrupted or unresolved attempt",
          evidence: null,
        },
      ].sort((left, right) => left.key.localeCompare(right.key)),
      attempted: report.attempted + 1,
      completionRate: report.completed / (report.attempted + 1),
    }));
    yield* scenario(key, item.flow, item.scenario).pipe(
      Effect.onExit((result) =>
        Effect.gen(function* () {
          const closed = yield* call(key, "close", Schema.NullOr(RunEvidence), {}).pipe(
            Effect.exit,
          );

          const evidence = Exit.isSuccess(closed)
            ? closed.value
            : yield* call(key, "evidence", RunEvidence).pipe(Effect.orElseSucceed(() => null));

          const passed =
            Exit.isSuccess(result) &&
            Exit.isSuccess(closed) &&
            closed.value !== null &&
            closed.value.control.closed;

          const elapsedMillis = Math.max(
            0,
            Math.round(Number((yield* Clock.monotonicTimeNanos) - start) / 1_000_000),
          );

          yield* updateReport((report) => ({
            ...report,
            results: report.results.map((previous) =>
              previous.key !== key
                ? previous
                : {
                    key,
                    ...item,
                    passed,
                    failure: Exit.isFailure(result)
                      ? Cause.pretty(result.cause)
                      : Exit.isFailure(closed)
                        ? "Browser cleanup failed"
                        : null,
                    evidence,
                    elapsedMillis,
                  },
            ),
            completed: report.completed + Number(passed),
            completionRate: (report.completed + Number(passed)) / report.attempted,
          }));
          yield* Console.log(`${key}: ${passed ? "passed" : "FAILED"} (${elapsedMillis}ms)`);
        }).pipe(Effect.orDie),
      ),
      Effect.exit,
    );
  });

  // Operator takeover is opt-in and stays outside the automated concurrent batch.
  for (const item of scheduled.filter((item) => item.scenario === "handoff")) yield* runCase(item);
  const admit = yield* makeCaseAdmission(startIntervalMillis);

  yield* measure(
    "matrixMillis",
    Effect.forEach(
      scheduled.filter((item) => item.scenario !== "handoff"),
      (item) => admit.pipe(Effect.andThen(runCase(item))),
      { concurrency, discard: true },
    ),
  );
  const report = yield* currentReport;

  if (report?.completed !== report?.attempted)
    return yield* failure(
      "assertion",
      "Checkout failures retained in .checkout-proof; no attempts were retried",
    );
}).pipe(
  Effect.tapCause((cause) =>
    updateReport((report) => ({ ...report, suiteFailure: Cause.pretty(cause) })).pipe(Effect.orDie),
  ),
);

const retire = Effect.gen(function* () {
  if (!ownsStage) return;
  let cleanupFailed = false;
  const { run } = yield* config;
  const report = yield* currentReport;

  if (shopUrl !== undefined) {
    const status = yield* workerStatus(`ea-checkout-${run}-shop`).pipe(Effect.exit);

    const plan = Exit.isSuccess(status)
      ? retirementPlan(
          status.value,
          report?.cleanup ?? "pending",
          report?.results.map((result) => result.evidence?.control.closed === true) ?? [false],
        )
      : "blocked";

    if (plan === "blocked") cleanupFailed = true;
    if (plan === "close")
      for (const key of started) {
        const closed = yield* call(key, "close", Schema.NullOr(RunEvidence), {}).pipe(Effect.exit);

        if (Exit.isFailure(closed) || (closed.value !== null && !closed.value.control.closed))
          cleanupFailed = true;
        if (Exit.isSuccess(closed))
          yield* updateReport((report) => ({
            ...report,
            results: report.results.map((result) =>
              result.key === key ? { ...result, evidence: closed.value } : result,
            ),
          }));
      }
  }

  // Preserve the durable owner if exact-session closure is unconfirmed; it holds the recovery reference.
  if (!cleanupFailed) {
    // Commit closure acknowledgements before destruction can make the owner unreachable.
    yield* updateReport((report) => ({ ...report, cleanup: "browsers-closed" }));
    const destroyed = yield* lifecycle.destroy(checkoutStack, { stage: run }).pipe(Effect.exit);

    if (Exit.isFailure(destroyed)) cleanupFailed = true;
  }
  for (const name of names) {
    const status = yield* workerStatus(name).pipe(Effect.exit);

    if (Exit.isFailure(status) || status.value !== 404) cleanupFailed = true;
  }
  yield* updateReport((report) => ({
    ...report,
    cleanup: !cleanupFailed
      ? "confirmed"
      : report.cleanup === "browsers-closed"
        ? "browsers-closed"
        : "failed",
  }));
  if (cleanupFailed)
    return yield* failure(
      "cleanup",
      "Closure or teardown is unconfirmed. Preserve .alchemy and rerun this stage with CHECKOUT_CLEANUP=true.",
    );
}).pipe(Effect.timeout("5 minutes"));

lifecycle.test(
  "the binding and real buyer complete the hosted proof",
  measure("totalMillis", withRetirement(proof, measure("retirementMillis", retire))).pipe(
    Effect.provide(
      Layer.unwrap(
        config.pipe(
          Effect.map(({ run }) => CheckoutReport.layer(`.checkout-proof/${run}/report.json`)),
        ),
      ),
    ),
  ),
);
