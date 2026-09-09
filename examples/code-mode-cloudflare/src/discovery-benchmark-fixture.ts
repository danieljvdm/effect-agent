import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as ToolDiscovery from "@effect-agent/capabilities/ToolDiscovery";
import * as Agent from "@effect-agent/core/Agent";
import { PinnedTool, ToolNamespace } from "@effect-agent/core/ToolExposure";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { ScriptedModel, type ScriptedTurnInput } from "@effect-agent/testing/ScriptedModel";
import { Context, Effect, Schema } from "effect";
import { AiError, Model, Tool, Toolkit } from "effect/unstable/ai";

import { Output, type Request } from "./discovery-benchmark-contracts.ts";

export const Names = {
  balance: "accounts_read_balance",
  holdback: "treasury_read_holdback",
  invoice: "settlement_read_invoice",
  fee: "settlement_read_fee",
  reserve: "settlement_read_reserve",
} as const;

const Parameters = Schema.Struct({ key: Schema.NonEmptyString });

const Reading = Schema.Struct({
  value: Schema.Int,
  unit: Schema.Literals(["cents", "basis-points"]),
  nextKey: Schema.String,
  receipt: Schema.String,
});

class ReadError extends Schema.TaggedError<ReadError>()("BenchmarkReadError", {
  message: Schema.String,
}) {}

const namespaces = [
  "accounts",
  "treasury",
  "settlement",
  "contacts",
  "inventory",
  "projects",
  "calendar",
  "shipping",
  "support",
  "contracts",
  "analytics",
  "documents",
];

const real = [
  [Names.balance, "accounts", "Read the current account balance in cents using the account key."],
  [Names.holdback, "treasury", "Read the treasury holdback amount in cents using the account key."],
  [
    Names.invoice,
    "settlement",
    "Read the settlement invoice gross amount in cents using the account key. nextKey identifies its fee schedule.",
  ],
  [
    Names.fee,
    "settlement",
    "Read the settlement fee in basis-points using only the nextKey returned by the invoice. nextKey identifies the reserve.",
  ],
  [
    Names.reserve,
    "settlement",
    "Read the settlement reserve in cents using only the nextKey returned by the fee schedule.",
  ],
] as const;

const makeTool = (name: string, namespace: string, description: string) =>
  Tool.make(name, {
    description: `${description} Return the current reading and its receipt; preserve the receipt as evidence. This operation is read-only.`,
    parameters: Parameters,
    success: Reading,
    failure: ReadError,
    failureMode: "return",
  })
    .annotate(ToolNamespace, namespace)
    .annotate(ToolExecutionClass, "readonly")
    .annotate(PinnedTool, name === Names.balance);

/** 120 fixed business tools; catalogue contents and order never depend on the measured arm. */
export const businessTools = namespaces.flatMap((namespace) => {
  const selected = real
    .filter((item) => item[1] === namespace)
    .map(([name, group, description]) => makeTool(name, group, description));

  return [
    ...selected,
    ...Array.from({ length: 10 - selected.length }, (_, index) =>
      makeTool(
        `${namespace}_metric_${index + 1}`,
        namespace,
        `Read ${namespace} operational metric ${index + 1} using a resource key. This metric is unrelated to financial balances, holdbacks or settlement calculations.`,
      ),
    ),
  ];
});

export const businessToolkit = Toolkit.make(...businessTools);
const balance = businessTools.find((tool) => tool.name === Names.balance);

if (balance === undefined) throw new Error("Benchmark catalogue lacks its pinned balance tool");

const codeTools = Object.fromEntries(
  namespaces.map((namespace) => [
    namespace,
    Object.fromEntries(
      businessTools
        .filter((tool) => tool.name.startsWith(`${namespace}_`))
        .map((tool) => [tool.name.slice(namespace.length + 1), tool]),
    ),
  ]),
);

export const codeMode = CodeMode.make("run_javascript", {
  description:
    "Execute a small JavaScript computation over discovered read-only methods. Discover methods first to learn their namespace, method and JSON schemas.",
  tools: codeTools,
  includeDeclarations: false,
});

export const discovery = ToolDiscovery.make({ maxResults: 4, maxResultBytes: 16_384 });
const nativeToolkit = Toolkit.make(...businessTools, discovery.tool);
const codeToolkit = Toolkit.make(balance, discovery.tool, codeMode.tool);

export class FixtureObserver extends Context.Service<
  FixtureObserver,
  {
    readonly seed: number;
    readonly start: (name: string) => Effect.Effect<void>;
    readonly end: (name: string) => Effect.Effect<void>;
    readonly fail: (name: string) => Effect.Effect<void>;
  }
>()("example/ToolBenchmark/FixtureObserver") {}

export const receipt = (name: string, seed: number) => {
  let hash = 2_166_136_261;

  for (const character of `fixture-${seed}-${name}-receipt`)
    hash = Math.imul(hash ^ character.charCodeAt(0), 16_777_619);

  return `r_${(hash >>> 0).toString(16).padStart(8, "0")}`;
};

const lookupKey = (kind: "fee" | "reserve", seed: number) => `k_${receipt(`${kind}-lookup`, seed)}`;

export const expected = (request: Pick<Request, "workload" | "seed">): typeof Output.Type => ({
  amountCents:
    request.workload === "common"
      ? 246_810 + request.seed
      : request.workload === "uncommon"
        ? 9_750 + request.seed
        : 130_880,
  receipts: (request.workload === "common"
    ? [Names.balance]
    : request.workload === "uncommon"
      ? [Names.holdback]
      : [Names.invoice, Names.fee, Names.reserve]
  ).map((name) => receipt(name, request.seed)),
});

export const requiredNames = (request: Pick<Request, "workload">): ReadonlyArray<string> =>
  request.workload === "common"
    ? [Names.balance]
    : request.workload === "uncommon"
      ? [Names.holdback]
      : [Names.invoice, Names.fee, Names.reserve];

export const handlers = businessToolkit.toLayer(
  Effect.gen(function* () {
    const observer = yield* FixtureObserver;
    const completed = new Set<string>();

    return Object.fromEntries(
      businessTools.map((tool) => [
        tool.name,
        Effect.fn(`benchmark.${tool.name}`)(function* ({ key }: typeof Parameters.Type) {
          const expectedKey =
            tool.name === Names.fee
              ? lookupKey("fee", observer.seed)
              : tool.name === Names.reserve
                ? lookupKey("reserve", observer.seed)
                : `acct_${observer.seed}`;

          if (
            key !== expectedKey ||
            (tool.name === Names.fee && !completed.has(Names.invoice)) ||
            (tool.name === Names.reserve && !completed.has(Names.fee))
          ) {
            yield* observer.fail(tool.name);

            return yield* ReadError.make({
              message: "Use the key from the preceding read or task input.",
            });
          }

          yield* observer.start(tool.name);

          const value =
            tool.name === Names.balance
              ? 246_810 + observer.seed
              : tool.name === Names.holdback
                ? 9_750 + observer.seed
                : tool.name === Names.invoice
                  ? 132_500
                  : tool.name === Names.fee
                    ? 120
                    : tool.name === Names.reserve
                      ? 30
                      : observer.seed;

          yield* observer.end(tool.name);
          completed.add(tool.name);

          return {
            value,
            unit: tool.name === Names.fee ? ("basis-points" as const) : ("cents" as const),
            nextKey:
              tool.name === Names.invoice
                ? lookupKey("fee", observer.seed)
                : tool.name === Names.fee
                  ? lookupKey("reserve", observer.seed)
                  : "",
            receipt: receipt(tool.name, observer.seed),
          };
        }),
      ]),
    );
  }),
);

export const question = (request: Pick<Request, "workload" | "seed">) => {
  const task =
    request.workload === "common"
      ? "Read the current account balance."
      : request.workload === "uncommon"
        ? "Read the current treasury holdback."
        : "Calculate the net settlement: read the invoice gross amount, use its returned key to read the fee basis-points, and use that returned key to read the reserve cents. Net is gross minus gross * fee / 10000 minus reserve.";

  return `Account key: acct_${request.seed}. ${task} Return amountCents and all required read receipts in operation order. Never invent readings, keys or receipts.`;
};

export const agent = (mode: Request["mode"]) =>
  Agent.make("discovery-benchmark", {
    input: Schema.String,
    output: Output,
    instructions:
      "Answer using current tool evidence. Tools accept a key and return a reading, unit, nextKey and receipt. If the needed tool is absent, use discover_tools with one or two short relevant terms. Settlement methods can be discovered together with query settlement_read. When a run_javascript tool is available, use discovered namespace methods to compose dependent reads in one program. Preserve every required receipt in operation order. Return only the structured answer.",
    toolkit: mode === "eager" ? businessToolkit : mode === "native" ? nativeToolkit : codeToolkit,
    ...(mode === "eager"
      ? {}
      : { toolExposure: { initialToolNames: [], maxTools: 8, maxSchemaBytes: 24_576 } }),
    policy: {
      maxTurns: 8,
      maxToolCalls: 16,
      maxDuration: "2 minutes",
      toolConcurrency: 1,
      contextTokenLimit: 128_000,
      onExhaustion: "fail",
    },
  });

export const scripted = (request: Request, finalize: Effect.Effect<void>) => {
  const calls: Array<{ name: string; params: Schema.Json }> = [];

  if (request.mode !== "eager" && request.workload !== "common")
    calls.push({
      name: "discover_tools",
      params: { query: request.workload === "uncommon" ? "read_holdback" : "settlement_read" },
    });
  if (request.mode === "code-mode" && request.workload !== "common") {
    const code =
      request.workload === "uncommon"
        ? `async () => { const r = await treasury.read_holdback({key:"acct_${request.seed}"}); return {amountCents:r.value,receipts:[r.receipt]}; }`
        : `async () => { const i = await settlement.read_invoice({key:"acct_${request.seed}"}); const f = await settlement.read_fee({key:i.nextKey}); const r = await settlement.read_reserve({key:f.nextKey}); return {amountCents:i.value-i.value*f.value/10000-r.value,receipts:[i.receipt,f.receipt,r.receipt]}; }`;

    calls.push({ name: "run_javascript", params: { code } });
  } else
    for (const name of requiredNames(request))
      calls.push({
        name,
        params: {
          key:
            name === Names.fee
              ? lookupKey("fee", request.seed)
              : name === Names.reserve
                ? lookupKey("reserve", request.seed)
                : `acct_${request.seed}`,
        },
      });

  const error = (description: string) =>
    AiError.make({
      module: "ToolBenchmark",
      method: "script",
      reason: AiError.InvalidRequestError.make({ description }),
    });

  const turns: Array<ScriptedTurnInput> = calls.map((call, index) => ({
    _tag: "Stream",
    termination: { _tag: "Complete" },
    onStreamFinalize: finalize,
    assertRequest: (options) =>
      options.tools.some((tool) => tool.name === call.name)
        ? Effect.void
        : Effect.fail(error(`Expected exposed tool ${call.name}`)),
    parts: [
      { type: "tool-call", id: `call-${index}`, ...call, providerExecuted: false },
      { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
    ],
  }));

  turns.push({
    _tag: "Stream",
    termination: { _tag: "Complete" },
    onStreamFinalize: finalize,
    assertRequest: (options) =>
      expected(request).receipts.every((value) => JSON.stringify(options.prompt).includes(value))
        ? Effect.void
        : Effect.fail(error("Provider did not consume every required receipt")),
    parts: [
      { type: "text-start", id: "answer" },
      { type: "text-delta", id: "answer", delta: JSON.stringify(expected(request)) },
      { type: "text-end", id: "answer" },
      { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
    ],
  });

  return Model.make("scripted", "tool-benchmark", ScriptedModel.layer(turns));
};
