import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import { Cause, Config, Console, Effect, Exit, FileSystem, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import type { CheckoutFlow, CheckoutScenario } from "../src/checkout-contract.ts";
import { AgentOutput, failure, policy, Report, RunEvidence } from "../src/checkout-contract.ts";
import { withRetirement } from "../src/checkout-lifecycle.ts";
import { checkoutStack } from "../src/checkout-stack.ts";
import { assertPurchase, expectedQuote, sameQuote } from "../src/checkout-store.ts";
import { BrowserRunWorkerProofResult } from "../src/contract.ts";

// Only prove:live collects this file. The same stage is reusable only for explicit retirement.
const lifecycle = Test.make({ providers: Cloudflare.providers(), dev: false });

const config = Config.all({
  run: Config.schema(Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,24}$/)), "CHECKOUT_RUN_ID"),
  model: Config.NonEmptyString("CHECKOUT_MODEL"),
  token: Config.Redacted("CHECKOUT_TOKEN"),
  repetitions: Config.Int("CHECKOUT_REPETITIONS").pipe(Config.withDefault(1)),
  human: Config.Boolean("CHECKOUT_HUMAN").pipe(Config.withDefault(false)),
  cleanupOnly: Config.Boolean("CHECKOUT_CLEANUP").pipe(Config.withDefault(false)),
});

let report: typeof Report.Type | undefined;
let shopUrl: string | undefined;
let names: ReadonlyArray<string> = [];
const started: Array<string> = [];
let ownsStage = false;

const writeReport = Effect.gen(function* () {
  if (report === undefined) return;
  const fs = yield* FileSystem.FileSystem;
  const { run } = yield* config;
  const directory = `.checkout-proof/${run}`;

  yield* fs.makeDirectory(directory, { recursive: true });
  yield* fs.writeFileString(
    `${directory}/report.json`,
    yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report),
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
    report = yield* fs
      .readFileString(reportPath)
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Report))));
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

  report = {
    version: 1,
    model: settings.model,
    sourceCommit,
    dirty,
    repetitions: settings.repetitions,
    profile: settings.human ? "operator" : "automated",
    bindingProof: false,
    suiteFailure: null,
    configuration: policy,
    results: [],
    completed: 0,
    attempted: 0,
    completionRate: 0,
    cleanup: "pending",
    providerCompatibility: "not-established",
  };
  yield* writeReport;
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
  const { run, repetitions, human, cleanupOnly } = yield* config;

  if (cleanupOnly) return;
  const deployed = yield* lifecycle.deploy(checkoutStack, { stage: run });

  if (!deployed.shopUrl || !deployed.processorUrl || !deployed.bindingUrl)
    return yield* failure("deployment", "Three HTTPS origins are required");
  shopUrl = deployed.shopUrl;
  yield* Effect.sleep("15 seconds");
  const client = yield* HttpClient.HttpClient;

  const bindingResponse = yield* client
    .get(deployed.bindingUrl)
    .pipe(Effect.timeout("150 seconds"));

  if (bindingResponse.status !== 200)
    return yield* failure(
      "binding-proof",
      `HTTP ${bindingResponse.status}; invocation was not retried`,
    );
  yield* bindingResponse.json.pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(BrowserRunWorkerProofResult)),
  );
  if (report !== undefined) report = { ...report, bindingProof: true };
  yield* writeReport;

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

  for (const { repetition, ...item } of scheduled) {
    const key = `${run}-${item.flow}-${item.scenario}-${repetition}`;

    yield* Console.log(`Checkout ${key}`);
    if (report === undefined) return yield* failure("evidence", "Report was not initialized");
    // Persist the denominator before dispatch: interruption cannot erase an unsuccessful attempt.
    report = {
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
      ],
      attempted: report.attempted + 1,
      completionRate: report.completed / (report.attempted + 1),
    };
    yield* writeReport;
    const result = yield* scenario(key, item.flow, item.scenario).pipe(Effect.exit);
    const closed = yield* call(key, "close", Schema.NullOr(RunEvidence), {}).pipe(Effect.exit);

    const evidence = Exit.isSuccess(closed)
      ? closed.value
      : yield* call(key, "evidence", RunEvidence).pipe(Effect.orElseSucceed(() => null));

    const passed =
      Exit.isSuccess(result) &&
      Exit.isSuccess(closed) &&
      closed.value !== null &&
      closed.value.control.closed;

    report = {
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
            },
      ),
      completed: report.completed + Number(passed),
    };
    report = { ...report, completionRate: report.completed / report.attempted };
    yield* writeReport;
    yield* Console.log(`${key}: ${passed ? "passed" : "FAILED"}`);
  }
  if (report?.completed !== report?.attempted)
    return yield* failure(
      "assertion",
      "Checkout failures retained in .checkout-proof; no attempts were retried",
    );
}).pipe(
  Effect.tapCause((cause) => {
    if (report !== undefined) report = { ...report, suiteFailure: Cause.pretty(cause) };

    return writeReport.pipe(Effect.orDie);
  }),
);

const retire = Effect.gen(function* () {
  if (!ownsStage) return;
  let cleanupFailed = false;

  if (shopUrl !== undefined)
    for (const key of started) {
      const closed = yield* call(key, "close", Schema.NullOr(RunEvidence), {}).pipe(Effect.exit);

      if (Exit.isFailure(closed) || (closed.value !== null && !closed.value.control.closed))
        cleanupFailed = true;
      if (Exit.isSuccess(closed) && report !== undefined)
        report = {
          ...report,
          results: report.results.map((result) =>
            result.key === key ? { ...result, evidence: closed.value } : result,
          ),
        };
    }
  const { run } = yield* config;

  // Preserve the durable owner if exact-session closure is unconfirmed; it holds the recovery reference.
  if (!cleanupFailed) {
    const destroyed = yield* lifecycle.destroy(checkoutStack, { stage: run }).pipe(Effect.exit);

    if (Exit.isFailure(destroyed)) cleanupFailed = true;
  }
  for (const name of names) {
    const status = yield* workerStatus(name).pipe(Effect.exit);

    if (Exit.isFailure(status) || status.value !== 404) cleanupFailed = true;
  }
  if (report !== undefined) report = { ...report, cleanup: cleanupFailed ? "failed" : "confirmed" };
  yield* writeReport;
  if (cleanupFailed)
    return yield* failure(
      "cleanup",
      "Closure or teardown is unconfirmed. Preserve .alchemy and rerun this stage with CHECKOUT_CLEANUP=true.",
    );
}).pipe(Effect.timeout("5 minutes"));

lifecycle.test(
  "the binding and real buyer complete the hosted proof",
  withRetirement(proof, retire),
);
