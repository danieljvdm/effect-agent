import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as Agent from "@effect-agent/core/Agent";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { dynamicWorkerCodeExecutorLayer } from "@effect-agent/platform-cloudflare/CloudflareCodeMode";
import { CodeExecutionLimits } from "@effect-agent/sandbox/CodeExecutor";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import {
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Response as AiResponse,
} from "effect/unstable/ai";

import { Mode, Workload } from "./benchmark-contract.ts";

const Project = Tool.make("createProject", {
  parameters: Schema.Struct({ name: Schema.String }),
  success: Schema.Struct({ id: Schema.String }),
});

const Task = Tool.make("createTask", {
  parameters: Schema.Struct({ projectId: Schema.String, title: Schema.String }),
  success: Schema.Struct({ title: Schema.String }),
});

const Customer = Tool.make("getCustomer", {
  parameters: Schema.Struct({ id: Schema.String }),
  success: Schema.Struct({ name: Schema.String }),
});

const Invoices = Tool.make("listInvoices", {
  parameters: Schema.Struct({ customerId: Schema.String }),
  success: Schema.Array(Schema.Struct({ paid: Schema.Boolean, amountCents: Schema.Int })),
});

const native = Toolkit.make(Project, Task, Customer, Invoices);
const titles = ["Design", "Build", "Ship"];
const usage = { inputTokens: {}, outputTokens: {} };

const toolCall = (id: string, name: string, params: Schema.Json): AiResponse.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
});

const benchmark = Effect.fn("benchmark")(function* (
  mode: typeof Mode.Type,
  workload: typeof Workload.Type,
  loader: WorkerLoader,
) {
  let active = 0;
  let peakConcurrency = 0;
  let toolCalls = 0;
  let modelCalls = 0;
  let projectCreated = false;

  const io = <A>(result: () => A) =>
    Effect.gen(function* () {
      active++;
      toolCalls++;
      peakConcurrency = Math.max(peakConcurrency, active);
      yield* Effect.sleep("20 millis");

      return result();
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          active--;
        }),
      ),
    );

  const handlers = native.toLayer({
    createProject: () =>
      io(() => {
        projectCreated = true;

        return { id: "launch" };
      }),
    createTask: ({ projectId, title }) =>
      io(() => {
        if (!projectCreated || projectId !== "launch")
          throw new Error("Task ran before its project");

        return { title };
      }),
    getCustomer: () => io(() => ({ name: "Acme" })),
    listInvoices: () =>
      io(() => [
        { paid: false, amountCents: 1_200 },
        { paid: true, amountCents: 700 },
        { paid: false, amountCents: 800 },
      ]),
  });

  const codeMode = CodeMode.make("run_code", {
    description: "Perform the workload and return its compact answer",
    tools: {
      tools: {
        createProject: Project,
        createTask: Task,
        getCustomer: Customer,
        listInvoices: Invoices,
      },
    },
    limits: CodeExecutionLimits.make({
      maxSourceBytes: 16_384,
      maxWallTime: Duration.seconds(10),
      maxLogBytes: 1_024,
      maxResultBytes: 16_384,
      maxHostCalls: 8,
      maxHostCallConcurrency: mode === "sequential" ? 1 : 3,
      maxHostCallArgumentBytes: 1_024,
      maxHostCallResultBytes: 16_384,
    }),
  });

  const code =
    workload === "writes"
      ? `async () => {
    const project = await tools.createProject({ name: "Launch" });
    const tasks = ${
      mode === "sequential"
        ? '[await tools.createTask({ projectId: project.id, title: "Design" }), await tools.createTask({ projectId: project.id, title: "Build" }), await tools.createTask({ projectId: project.id, title: "Ship" })]'
        : 'await Promise.all(["Design", "Build", "Ship"].map(title => tools.createTask({ projectId: project.id, title })))'
    };
    return { projectId: project.id, tasksCreated: tasks.length };
  }`
      : `async () => {
    const [customer, invoices] = ${
      mode === "sequential"
        ? '[await tools.getCustomer({ id: "acme" }), await tools.listInvoices({ customerId: "acme" })]'
        : 'await Promise.all([tools.getCustomer({ id: "acme" }), tools.listInvoices({ customerId: "acme" })])'
    };
    return { customer: customer.name, unpaidCents: invoices.filter(i => !i.paid).reduce((sum, i) => sum + i.amountCents, 0) };
  }`;

  const model = Model.make(
    "scripted",
    "20ms-model",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const turn = modelCalls++;

              yield* Effect.sleep("20 millis");

              const results = prompt.content
                .filter((message) => message.role === "tool")
                .flatMap((message) => message.content)
                .filter((part) => part.type === "tool-result");

              if (results.some((result) => result.isFailure))
                return yield* Effect.die("Workload tool failed");
              let calls: ReadonlyArray<AiResponse.StreamPartEncoded> | undefined;

              if (turn === 0) {
                calls =
                  mode !== "native"
                    ? [toolCall("program", "run_code", { code })]
                    : workload === "writes"
                      ? [toolCall("project", "createProject", { name: "Launch" })]
                      : [
                          toolCall("customer", "getCustomer", { id: "acme" }),
                          toolCall("invoices", "listInvoices", { customerId: "acme" }),
                        ];
              } else if (mode === "native" && workload === "writes" && turn === 1) {
                const project = Schema.decodeUnknownSync(Project.successSchema)(
                  results.find((r) => r.name === "createProject")?.result,
                );

                calls = titles.map((title) =>
                  toolCall(title, "createTask", { projectId: project.id, title }),
                );
              }
              if (calls !== undefined)
                return Stream.fromIterable<AiResponse.StreamPartEncoded>([
                  ...calls,
                  { type: "finish", reason: "tool-calls", usage },
                ]);
              let answer: Schema.Json;

              if (mode !== "native") {
                answer = Schema.decodeUnknownSync(CodeMode.CodeModeSuccess)(
                  results[0]?.result,
                ).result;
              } else if (workload === "writes") {
                const project = Schema.decodeUnknownSync(Project.successSchema)(
                  results.find((r) => r.name === "createProject")?.result,
                );

                const tasks = results
                  .filter((r) => r.name === "createTask")
                  .map((r) => Schema.decodeUnknownSync(Task.successSchema)(r.result));

                answer = { projectId: project.id, tasksCreated: tasks.length };
              } else {
                const customer = Schema.decodeUnknownSync(Customer.successSchema)(
                  results.find((r) => r.name === "getCustomer")?.result,
                );

                const invoices = Schema.decodeUnknownSync(Invoices.successSchema)(
                  results.find((r) => r.name === "listInvoices")?.result,
                );

                answer = {
                  customer: customer.name,
                  unpaidCents: invoices
                    .filter((i) => !i.paid)
                    .reduce((sum, i) => sum + i.amountCents, 0),
                };
              }

              return Stream.fromIterable<AiResponse.StreamPartEncoded>([
                { type: "text-start", id: "answer" },
                { type: "text-delta", id: "answer", delta: JSON.stringify(answer) },
                { type: "text-end", id: "answer" },
                { type: "finish", reason: "stop", usage },
              ]);
            }),
          ),
      }),
    ),
  );

  const definition = Agent.make("benchmark", {
    input: Schema.String,
    output: Schema.Json,
    instructions: "Execute and summarize.",
    toolkit: mode === "native" ? native : Toolkit.make(codeMode.tool),
    policy: { maxTurns: 3, maxToolCalls: 8, maxDuration: "15 seconds", toolConcurrency: 3 },
  });

  const services = Layer.merge(
    handlers,
    codeMode.handlers.pipe(Layer.provide([handlers, dynamicWorkerCodeExecutorLayer({ loader })])),
  );

  const result = yield* AgentRuntime.run(Agent.withModel(definition, model), workload).pipe(
    Effect.provide(services),
    Effect.provide([
      IdGenerator.layer,
      ThreadHistory.layerTransient,
      RunContextPreparationPassthrough,
    ]),
    Effect.scoped,
  );

  return { answer: result.output, modelCalls, toolCalls, peakConcurrency, activeAfterRun: active };
});

export default {
  async fetch(request: Request, env: { readonly LOADER: WorkerLoader }): Promise<Response> {
    const url = new URL(request.url);

    const program = Effect.gen(function* () {
      const mode = yield* Schema.decodeUnknownEffect(Mode)(url.searchParams.get("mode"));

      const workload = yield* Schema.decodeUnknownEffect(Workload)(
        url.searchParams.get("workload"),
      );

      return yield* benchmark(mode, workload, env.LOADER);
    });

    return await Effect.runPromise(
      program.pipe(
        Effect.map((result) => Response.json(result)),
        Effect.catchCause((cause) =>
          Effect.succeed(Response.json({ failure: String(cause) }, { status: 500 })),
        ),
      ),
    );
  },
};
