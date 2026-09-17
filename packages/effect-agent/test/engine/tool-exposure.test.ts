import { DecisionModel } from "@effect-agent/ai-decision";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Result,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ContextRolloverTool } from "effect-agent/context-window";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { SubagentGrant } from "effect-agent/subagent-contract";
import { ThreadHistory } from "effect-agent/thread-history";
import * as ToolDiscovery from "effect-agent/tool-discovery";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  IncludesCatalogDocumentation,
  PinnedTool,
  Selection,
  Snapshot,
  CurrentToolCatalog,
  RunToolVisibility,
} from "effect-agent/tool-exposure";
import * as ToolSelector from "effect-agent/tool-selector";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() => ThreadId.make(`exposure-thread-${++threadSequence}`)),
  nextRunId: Effect.succeed(RunId.make("exposure-run")),
  nextTurnId: Effect.succeed(TurnId.make("exposure-turn")),
});

const finish = {
  type: "finish",
  reason: "tool-calls",
  usage: { inputTokens: {}, outputTokens: {} },
} satisfies Response.StreamPartEncoded;

const call = (id: string, name: string, params: unknown = {}): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
});

const done: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const scripted = (
  responses: ReadonlyArray<ReadonlyArray<Response.StreamPartEncoded>>,
  requests: Array<ReadonlyArray<string>>,
  choices?: Array<LanguageModel.ToolChoice<string>>,
) => {
  let index = 0;

  return Model.make(
    "test",
    "tool-exposure",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) => {
          requests.push(options.tools.map((tool) => tool.name));
          choices?.push(options.toolChoice);

          return Stream.fromIterable(responses[index++] ?? done);
        },
      }),
    ),
  );
};

const Search = Tool.make("discover", {
  parameters: Schema.Struct({ select: Schema.String }),
  success: Schema.Struct({
    // Selection uses decoded names even when the model-visible wire format differs.
    toolNames: Schema.Array(
      Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transform((name) => name.toLowerCase()),
          encode: SchemaGetter.transform((name) => name.toUpperCase()),
        }),
      ),
    ),
    padding: Schema.String,
  }),
})
  .annotate(DiscoveryTool, true)
  .annotate(ToolExecutionClass, "readonly")
  .addDependency(CurrentToolCatalog);

const Read = Tool.make("read", { parameters: Schema.Struct({}), success: Schema.String });
const Write = Tool.make("write", { parameters: Schema.Struct({}), success: Schema.String });

const Status = Tool.make("status", {
  parameters: Schema.Struct({}),
  success: Schema.String,
}).annotate(PinnedTool, true);

const tools = Toolkit.make(Search, Read, Write, Status);

const definition = Agent.make("exposure", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use Tools.",
  toolkit: tools,
  toolExposure: { maxTools: 3 },
  policy: {
    maxTurns: 6,
    maxToolCalls: 10,
    toolConcurrency: 2,
    toolResultBounds: { maxBytes: 256 },
  },
});

const failure = <E>(exit: Exit.Exit<unknown, E>) =>
  Exit.isFailure(exit) ? Option.getOrUndefined(Cause.findErrorOption(exit.cause)) : undefined;

layer(Layer.mergeAll(identifiers, ThreadHistory.layer))("native Tool exposure", (it) => {
  it.effect(
    "selects Code Mode aliases once per owning native tool and breaks relevance ties by ID",
    () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];

        const code = Tool.make("run_code", { success: Schema.String }).annotate(
          AdditionalToolCatalog,
          [
            { tool: Read, namespace: "records", method: "first" },
            { tool: Read, namespace: "records", method: "second" },
          ],
        );

        const native = Toolkit.make(code, Read, Status);

        const agent = Agent.make("selector-alias", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Answer",
          toolkit: native,
        });

        const decision = yield* DecisionModel.make({
          evaluate: () =>
            Effect.succeed({
              provider: "test",
              model: "tie",
              usage: { inputTokens: null, outputTokens: null },
              answers: {
                candidate_0: { type: "probability", probability: 0.9 },
                candidate_1: { type: "probability", probability: 0.9 },
                candidate_2: { type: "probability", probability: 0.9 },
                candidate_3: { type: "probability", probability: 0.9 },
                candidate_4: { type: "probability", probability: 0.1 },
              },
            }),
        });

        const selector = ToolSelector.fromDecisionModel({
          state: () => Effect.succeed("go"),
          minimumRelevance: 0.8,
          maxTools: 2,
        });

        let ranked: ReadonlyArray<string> | undefined;

        yield* AgentRuntime.run(Agent.withModel(agent, scripted([done], requests)), "go", {
          toolSelector: {
            ...selector,
            select: (request) =>
              selector.select(request).pipe(
                Effect.tap((ids) =>
                  Effect.sync(() => {
                    ranked = ids;
                  }),
                ),
              ),
          },
        }).pipe(
          Effect.provideService(DecisionModel.DecisionModel, decision),
          Effect.provide(
            native.toLayer({
              run_code: () => Effect.die("unused"),
              read: () => Effect.die("unused"),
              status: () => Effect.die("unused"),
            }),
          ),
        );
        expect(ranked).toEqual([
          "code-mode:run_code:records.first",
          "code-mode:run_code:records.second",
          "native:read",
          "native:run_code",
        ]);
        expect(requests).toEqual([["run_code", "read", "status"]]);
      }),
  );

  it.effect(
    "clears non-pinned tools on a decision no-match and refuses oversized state before evaluation",
    () =>
      Effect.gen(function* () {
        for (const mode of ["clear", "state-limit"] as const) {
          const requests: Array<ReadonlyArray<string>> = [];
          let evaluations = 0;

          const decision = yield* DecisionModel.make({
            evaluate: () =>
              Effect.sync(() => {
                evaluations++;

                return {
                  provider: "test",
                  model: "none",
                  usage: { inputTokens: null, outputTokens: null },
                  answers: {
                    candidate_0: { type: "probability", probability: 0.1 },
                    candidate_1: { type: "probability", probability: 0.1 },
                    candidate_2: { type: "probability", probability: 0.1 },
                    candidate_3: { type: "probability", probability: 0.1 },
                  },
                };
              }),
          });

          const exit = yield* AgentRuntime.run(
            Agent.withModel(definition, scripted([done], requests)),
            "go",
            {
              toolSelection: Selection.make({ toolNames: ["read"] }),
              toolSelector: ToolSelector.fromDecisionModel({
                state: () => Effect.succeed("é"),
                minimumRelevance: 0.8,
                onNoMatch: "clear",
                maxStateBytes: mode === "clear" ? 4 : 3,
              }),
            },
          ).pipe(
            Effect.provideService(DecisionModel.DecisionModel, decision),
            Effect.provide(
              tools.toLayer({
                discover: () => Effect.die("unused"),
                read: () => Effect.die("unused"),
                write: () => Effect.die("unused"),
                status: () => Effect.die("unused"),
              }),
            ),
            Effect.exit,
          );

          if (mode === "clear") {
            expect(Exit.isSuccess(exit)).toBe(true);
            expect(requests).toEqual([["discover", "status"]]);
            expect(evaluations).toBe(1);
          } else {
            expect(failure(exit)).toMatchObject({
              _tag: "AiError",
              reason: { _tag: "InvalidRequestError" },
            });
            expect(requests).toEqual([]);
            expect(evaluations).toBe(0);
          }
        }
      }),
  );

  it.effect(
    "shortlists before the first model turn with filtered metadata, batched relevance, and retained pins",
    () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];
        let evaluations = 0;
        let observed = 0;

        const decision = yield* DecisionModel.make({
          evaluate: (request) =>
            Effect.sync(() => {
              evaluations++;
              expect(request.state).toBe("go");
              expect(Object.keys(request.questions)).toEqual([
                "candidate_0",
                "candidate_1",
                "candidate_2",
              ]);
              expect(JSON.stringify(request.questions)).not.toContain("write");

              return {
                provider: "test",
                model: "decisions",
                usage: { inputTokens: 12, outputTokens: 3 },
                answers: {
                  candidate_0: { type: "probability", probability: 0.1 },
                  candidate_1: { type: "probability", probability: evaluations === 1 ? 0.9 : 0.2 },
                  candidate_2: { type: "probability", probability: 0.1 },
                },
              };
            }),
        });

        const selector = ToolSelector.fromDecisionModel({
          state: ({ input }) => Schema.decodeUnknownEffect(Schema.String)(input),
          minimumRelevance: 0.8,
          maxTools: 1,
          onEvaluation: ({ usage }) =>
            Effect.sync(() => {
              observed++;
              expect(usage.inputTokens).toBe(12);
            }),
        });

        const result = yield* AgentRuntime.run(
          Agent.withModel(definition, scripted([[call("read-1", "read"), finish], done], requests)),
          "go",
          {
            toolSelector: selector,
            subagentGrant: SubagentGrant.make({
              allowedToolNames: ["discover", "read", "status"],
              maxDepth: 1,
            }),
            delegationDepth: 1,
          },
        ).pipe(
          Effect.provideService(DecisionModel.DecisionModel, decision),
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.die("Discovery should not run"),
              read: () => Effect.succeed("found"),
              write: () => Effect.die("Hidden Tool ran"),
              status: () => Effect.succeed(""),
            }),
          ),
        );

        expect(result.output).toBe("done");
        expect(evaluations).toBe(2);
        expect(observed).toBe(2);
        expect(requests).toEqual([
          ["discover", "read", "status"],
          ["discover", "read", "status"],
        ]);
      }),
  );

  it.effect(
    "applies select, keep, and clear after context preparation and closes selector resources before dispatch",
    () =>
      Effect.gen(function* () {
        let selected = 0;
        let finalized = 0;
        const requests: Array<ReadonlyArray<string>> = [];

        const model = scripted(
          [[call("r1", "read"), finish], [call("r2", "read"), finish], done],
          requests,
        );

        yield* AgentRuntime.run(Agent.withModel(definition, model), "go", {
          context: {
            prepare: ({ source }) =>
              Effect.sync(() => {
                expect(finalized).toBe(selected);

                return { prompt: source };
              }),
          },
          toolSelector: {
            select: ({ source, catalogue }) =>
              Effect.gen(function* () {
                expect(source.content.length).toBeGreaterThan(0);
                expect(catalogue.map((entry) => entry.id)).toEqual([
                  "native:discover",
                  "native:read",
                  "native:status",
                ]);
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    finalized++;
                  }),
                );
                selected++;

                return selected === 1 ? ["native:read"] : selected === 2 ? undefined : [];
              }),
          },
        }).pipe(
          Effect.scoped,
          Effect.provideService(RunToolVisibility, {
            visible: ({ toolNames }) =>
              Effect.succeed(toolNames.filter((name) => name !== "write")),
          }),
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.die("unused"),
              read: () =>
                Effect.sync(() => {
                  expect(finalized).toBe(selected);

                  return "ok";
                }),
              write: () => Effect.die("unused"),
              status: () => Effect.succeed(""),
            }),
          ),
        );
        expect(requests).toEqual([
          ["discover", "read", "status"],
          ["discover", "read", "status"],
          ["discover", "status"],
        ]);
        expect(finalized).toBe(3);
      }),
  );

  for (const ids of [["native:read", "native:write"], ["native:read", "native:read"], ["read"]]) {
    it.effect(`rejects every invalid selector ID before applying maxTools: ${ids.join(",")}`, () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];

        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, scripted([done], requests)),
          "go",
          {
            toolSelector: { maxTools: 1, select: () => Effect.succeed(ids) },
          },
        ).pipe(
          Effect.provideService(RunToolVisibility, {
            visible: () => Effect.succeed(["discover", "read", "status"]),
          }),
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.die("unused"),
              read: () => Effect.die("unused"),
              write: () => Effect.die("unused"),
              status: () => Effect.die("unused"),
            }),
          ),
          Effect.exit,
        );

        expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
        expect(requests).toEqual([]);
      }),
    );
  }

  for (const limits of [{ maxCandidates: 1 }, { maxCatalogueBytes: 1 }]) {
    it.effect(`bounds selector input before evaluation: ${JSON.stringify(limits)}`, () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];

        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, scripted([done], requests)),
          "go",
          {
            toolSelector: { ...limits, select: () => Effect.die("Oversized catalogue evaluated") },
          },
        ).pipe(
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.die("unused"),
              read: () => Effect.die("unused"),
              write: () => Effect.die("unused"),
              status: () => Effect.die("unused"),
            }),
          ),
          Effect.exit,
        );

        expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
        expect(requests).toEqual([]);
      }),
    );
  }

  for (const mode of ["failure", "defect", "timeout", "interrupt"] as const) {
    it.effect(`preserves selector ${mode} and finalizes before any model call`, () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const requests: Array<ReadonlyArray<string>> = [];
        let finalized = false;

        const program = AgentRuntime.run(
          Agent.withModel(definition, scripted([done], requests)),
          "go",
          {
            toolSelector: {
              select: () =>
                Effect.gen(function* () {
                  yield* Effect.addFinalizer(() =>
                    Effect.sync(() => {
                      finalized = true;
                    }),
                  );
                  yield* Deferred.succeed(started, undefined);
                  if (mode === "failure")
                    return yield* Effect.fail("selector-unavailable" as const);
                  if (mode === "defect") return yield* Effect.die("selector-defect");

                  return yield* Effect.never;
                }),
            },
          },
        ).pipe(
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.die("unused"),
              read: () => Effect.die("unused"),
              write: () => Effect.die("unused"),
              status: () => Effect.die("unused"),
            }),
          ),
        );

        const fiber = yield* (
          mode === "timeout" ? program.pipe(Effect.timeout("1 second")) : program
        ).pipe(Effect.forkChild);

        yield* Deferred.await(started);
        if (mode === "timeout") yield* TestClock.adjust("1 second");
        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(finalized).toBe(true);
        expect(requests).toEqual([]);
        expect(Exit.isFailure(exit)).toBe(true);
        if (mode === "failure") expect(failure(exit)).toBe("selector-unavailable");
        if (mode === "defect" && Exit.isFailure(exit))
          expect(Result.getOrThrow(Cause.findDefect(exit.cause))).toBe("selector-defect");
        if (mode === "timeout") expect(failure(exit)).toMatchObject({ _tag: "TimeoutError" });
        if (mode === "interrupt" && Exit.isFailure(exit))
          expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      }),
    );
  }

  // Regression: https://github.com/danieljvdm/effect-agent/issues/496
  for (const budget of ["aggregate", "single"] as const) {
    it.effect(`continues after ${budget} discovery overflow with only documented selections`, () =>
      Effect.gen(function* () {
        const documentation = "東京".repeat(64);

        const ReadDocument = Tool.make("read", {
          description: "Read café 東京 😀",
          parameters: Schema.Struct({
            key: Schema.String.annotate({ description: documentation }),
          }),
          success: Schema.NumberFromString,
        });

        const WriteDocument = Tool.make("write", {
          parameters: ReadDocument.parametersSchema,
          success: Schema.String,
        });

        const HostHidden = Tool.make("host_hidden", { success: Schema.String });
        const GrantHidden = Tool.make("grant_hidden", { success: Schema.String });
        const maxResultBytes = budget === "aggregate" ? 1_024 : 256;
        let finalized = 0;

        const discovery = ToolDiscovery.make({
          maxResultBytes,
          search: (_request, catalogue) =>
            Effect.gen(function* () {
              expect(catalogue.map((entry) => entry.name)).toEqual([
                "discover_tools",
                "read",
                "status",
                "write",
              ]);
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  finalized++;
                }),
              );

              return budget === "aggregate" ? ["native:read", "native:write"] : ["native:read"];
            }),
        });

        const actions = Toolkit.make(ReadDocument, WriteDocument, Status, HostHidden, GrantHidden);

        const agent = Agent.make("bounded-discovery", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Discover and use tools.",
          toolkit: Toolkit.merge(actions, discovery.toolkit),
          toolExposure: { initialToolNames: ["write"] },
        });

        let turn = 0;
        const invoked: Array<string> = [];

        const model = Model.make(
          "test",
          "bounded-discovery",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (options) => {
                if (turn++ === 0)
                  return Stream.fromIterable([
                    call("find", "discover_tools", { query: "read" }),
                    finish,
                  ]);
                if (turn === 2) {
                  const results = options.prompt.content.flatMap((message) =>
                    message.role === "tool" ? message.content : [],
                  );

                  const result = results.find((part) => part.type === "tool-result");

                  expect(result).toMatchObject({ id: "find", isFailure: false });
                  const decoded = Schema.decodeUnknownSync(ToolDiscovery.Result)(result?.result);

                  expect(
                    new TextEncoder().encode(JSON.stringify(result?.result)).length,
                  ).toBeLessThanOrEqual(maxResultBytes);
                  expect(decoded).toMatchObject({
                    toolNames: budget === "aggregate" ? ["read"] : [],
                    notice: expect.stringMatching(/narrow.*search/i),
                  });
                  expect(decoded.matches.map((match) => match.name)).toEqual(decoded.toolNames);
                  if (budget === "aggregate")
                    expect(decoded.matches).toMatchObject([
                      {
                        parameters: {
                          properties: { key: { type: "string", description: documentation } },
                        },
                        success: { type: "string" },
                      },
                    ]);
                  expect(options.tools.map((tool) => tool.name).toSorted()).toEqual(
                    budget === "aggregate"
                      ? ["discover_tools", "read", "status"]
                      : ["discover_tools", "status"],
                  );

                  return Stream.fromIterable([
                    budget === "aggregate"
                      ? call("use", "read", { key: "record" })
                      : call("use", "status"),
                    finish,
                  ]);
                }

                return Stream.fromIterable(done);
              },
            }),
          ),
        );

        const result = yield* AgentRuntime.run(Agent.withModel(agent, model), "go", {
          subagentGrant: SubagentGrant.make({
            allowedToolNames: ["discover_tools", "read", "write", "status", "host_hidden"],
            maxDepth: 1,
          }),
          delegationDepth: 1,
        }).pipe(
          Effect.provideService(RunToolVisibility, {
            visible: ({ toolNames }) =>
              Effect.succeed(toolNames.filter((name) => name !== "host_hidden")),
          }),
          Effect.provide([
            discovery.handlers,
            actions.toLayer({
              read: () =>
                Effect.sync(() => {
                  invoked.push("read");

                  return 1;
                }),
              status: () =>
                Effect.sync(() => {
                  invoked.push("status");

                  return "ok";
                }),
              write: () => Effect.die("Undocumented tool must not execute"),
              host_hidden: () => Effect.die("Host-hidden tool must not execute"),
              grant_hidden: () => Effect.die("Grant-hidden tool must not execute"),
            }),
          ]),
        );

        expect(result.output).toBe("done");
        expect(turn).toBe(3);
        expect(invoked).toEqual([budget === "aggregate" ? "read" : "status"]);
        expect(finalized).toBe(1);
      }),
    );
  }

  for (const authority of ["host", "grant"] as const) {
    it.effect(`keeps eligible pins across replacements while ${authority} hides another pin`, () =>
      Effect.gen(function* () {
        const native = Toolkit.make(Search, Read.annotate(PinnedTool, true), Write, Status);

        const agent = Agent.make("eligible-pins", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Use authorized Tools.",
          toolkit: native,
          toolExposure: { maxTools: 3 },
        });

        const requests: Array<ReadonlyArray<string>> = [];
        let hiddenStarts = 0;

        const handlers = native.toLayer({
          discover: ({ select }) =>
            Effect.gen(function* () {
              expect((yield* CurrentToolCatalog).entries.map((entry) => entry.tool.name)).toEqual([
                "discover",
                "read",
                "write",
              ]);

              return { toolNames: select === "" ? [] : [select], padding: "" };
            }),
          read: () => Effect.succeed("read"),
          write: () => Effect.succeed("write"),
          status: () =>
            Effect.sync(() => {
              hiddenStarts++;

              return "status";
            }),
        });

        const options =
          authority === "grant"
            ? {
                subagentGrant: SubagentGrant.make({
                  allowedToolNames: ["discover", "read", "write"],
                  maxDepth: 1,
                }),
                delegationDepth: 1,
              }
            : {};

        const visibility = Layer.succeed(
          RunToolVisibility,
          authority === "host"
            ? {
                visible: ({ toolNames }) =>
                  Effect.succeed(toolNames.filter((name) => name !== "status")),
              }
            : undefined,
        );

        const result = yield* AgentRuntime.run(
          Agent.withModel(
            agent,
            scripted(
              [
                [call("select", "discover", { select: "write" }), finish],
                [call("clear", "discover", { select: "" }), finish],
                done,
              ],
              requests,
            ),
          ),
          "go",
          options,
        ).pipe(Effect.provide([handlers, visibility]));

        expect(result.output).toBe("done");
        expect(requests).toEqual([
          ["discover", "read"],
          ["discover", "read", "write"],
          ["discover", "read"],
        ]);

        const rejected = yield* AgentRuntime.run(
          Agent.withModel(agent, scripted([[call("hidden", "status"), finish]], [])),
          "go",
          options,
        ).pipe(Effect.provide([handlers, visibility]), Effect.exit);

        expect(Exit.isFailure(rejected)).toBe(true);
        expect(hiddenStarts).toBe(0);
      }),
    );
  }

  for (const mandatory of ["discovery", "rollover", "completion"] as const) {
    it.effect(`refuses an unavailable mandatory ${mandatory} Tool before model dispatch`, () =>
      Effect.gen(function* () {
        const required =
          mandatory === "discovery"
            ? Search
            : mandatory === "rollover"
              ? Read.annotate(ContextRolloverTool, true)
              : Read;

        const native = Toolkit.make(required);

        const agent = Agent.make("mandatory-tool", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Go.",
          toolkit: native,
          toolExposure: {},
          ...(mandatory === "completion"
            ? { completion: { tool: "read", required: true, project: () => "done" } }
            : {}),
        });

        const requests: Array<ReadonlyArray<string>> = [];

        const exit = yield* AgentRuntime.run(
          Agent.withModel(agent, scripted([done], requests)),
          "go",
        ).pipe(
          Effect.provideService(RunToolVisibility, { visible: () => Effect.succeed([]) }),
          Effect.provide(
            native.toLayer({
              discover: () => Effect.die("Unavailable discovery must not execute"),
              read: () => Effect.die("Unavailable mandatory Tool must not execute"),
            }),
          ),
          Effect.exit,
        );

        expect(failure(exit)).toMatchObject({
          _tag: "ModelProtocolError",
          message: "A mandatory Tool is excluded by host visibility or the inherited grant",
        });
        expect(requests).toEqual([]);
      }),
    );
  }

  it.effect(
    "replaces in declaration order, keeps pins, and projects before result truncation",
    () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];
        const secondFinished = yield* Deferred.make<void>();
        const selections: Array<Selection> = [];

        const model = scripted(
          [
            [
              call("first", "discover", { select: "read" }),
              call("second", "discover", { select: "write" }),
              finish,
            ],
            [call("clear", "discover", { select: "" }), finish],
            done,
          ],
          requests,
        );

        const events = yield* AgentRuntime.stream(Agent.withModel(definition, model), "go").pipe(
          Stream.tap((event) =>
            event._tag === "ToolCallSucceeded" && event.toolCallId === "second"
              ? Deferred.succeed(secondFinished, undefined)
              : Effect.void,
          ),
          Stream.runCollect,
          Effect.provide(
            tools.toLayer({
              discover: ({ select }) =>
                Effect.gen(function* () {
                  expect(
                    (yield* CurrentToolCatalog).entries.map((entry) => entry.tool.name),
                  ).toEqual(["discover", "read", "write", "status"]);
                  if (select === "read") yield* Deferred.await(secondFinished);

                  return { toolNames: select === "" ? [] : [select], padding: "x".repeat(1_024) };
                }),
              read: () => Effect.succeed("read"),
              write: () => Effect.succeed("write"),
              status: () => Effect.succeed("status"),
            }),
          ),
        );

        for (const event of events)
          if (event._tag === "ToolCallSucceeded" && event.toolSelection !== undefined)
            selections.push(event.toolSelection);
        expect(requests).toEqual([
          ["discover", "status"],
          ["discover", "write", "status"],
          ["discover", "status"],
        ]);
        expect(selections.map((selection) => selection.toolNames)).toEqual([
          ["write"],
          ["read"],
          [],
        ]);
        expect(
          events.some(
            (event) =>
              event._tag === "ToolCallSucceeded" &&
              event.toolSelection !== undefined &&
              JSON.stringify(event.result).includes("truncatedToolResult"),
          ),
        ).toBe(true);
      }),
  );

  it.effect("rejects hidden native calls before any Handler starts", () =>
    Effect.gen(function* () {
      let starts = 0;
      const requests: Array<ReadonlyArray<string>> = [];

      const exit = yield* AgentRuntime.run(
        Agent.withModel(definition, scripted([[call("hidden", "write"), finish]], requests)),
        "go",
      ).pipe(
        Effect.provide(
          tools.toLayer({
            discover: () => Effect.succeed({ toolNames: [], padding: "" }),
            read: () => Effect.succeed(""),
            write: () =>
              Effect.sync(() => {
                starts++;

                return "";
              }),
            status: () => Effect.succeed(""),
          }),
        ),
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      expect(starts).toBe(0);
      expect(requests).toEqual([["discover", "status"]]);
    }),
  );

  it.effect("filters catalogue metadata before discovery and accepts host context selection", () =>
    Effect.gen(function* () {
      const requests: Array<ReadonlyArray<string>> = [];

      const exit = yield* AgentRuntime.run(
        Agent.withModel(
          definition,
          scripted([[call("find", "discover", { select: "read" }), finish], done], requests),
        ),
        "go",
        {
          context: {
            prepare: ({ source }) =>
              Effect.succeed({
                prompt: source,
                toolSelection: Selection.make({ toolNames: ["read"] }),
              }),
          },
        },
      ).pipe(
        Effect.provideService(RunToolVisibility, {
          visible: ({ toolNames }) => Effect.succeed(toolNames.filter((name) => name !== "write")),
        }),
        Effect.provide(
          tools.toLayer({
            discover: () =>
              Effect.gen(function* () {
                expect((yield* CurrentToolCatalog).entries.map((entry) => entry.tool.name)).toEqual(
                  ["discover", "read", "status"],
                );

                return { toolNames: ["read"], padding: "" };
              }),
            read: () => Effect.succeed(""),
            write: () => Effect.succeed(""),
            status: () => Effect.succeed(""),
          }),
        ),
        Effect.exit,
      );

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(requests).toEqual([
        ["discover", "read", "status"],
        ["discover", "read", "status"],
      ]);
    }),
  );

  for (const configuration of [
    { initialToolNames: ["missing"] },
    { initialToolNames: ["toString"] },
    { initialToolNames: ["__proto__"] },
    { initialToolNames: ["read"], maxTools: 2 },
    { initialToolNames: ["read"], maxSchemaBytes: 1 },
  ]) {
    it.effect(
      `rejects invalid or over-limit exposure before dispatch: ${JSON.stringify(configuration)}`,
      () =>
        Effect.gen(function* () {
          const requests: Array<ReadonlyArray<string>> = [];

          const limited = Agent.make("limited", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Use Tools.",
            toolkit: tools,
            toolExposure: configuration,
          });

          const exit = yield* AgentRuntime.run(
            Agent.withModel(limited, scripted([done], requests)),
            "go",
          ).pipe(
            Effect.provide(
              tools.toLayer({
                discover: () => Effect.succeed({ toolNames: [], padding: "" }),
                read: () => Effect.succeed(""),
                write: () => Effect.succeed(""),
                status: () => Effect.succeed(""),
              }),
            ),
            Effect.exit,
          );

          expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
          expect(requests).toEqual([]);
        }),
    );
  }

  it.effect(
    "refuses static Code Mode declarations before hidden metadata can reach discovery",
    () =>
      Effect.gen(function* () {
        const outer = Tool.make("run_code", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        })
          .annotate(AdditionalToolCatalog, [{ tool: Write, namespace: "db", method: "write" }])
          .annotate(IncludesCatalogDocumentation, true);

        const native = Toolkit.make(outer);
        const requests: Array<ReadonlyArray<string>> = [];

        const agent = Agent.make("code-leak", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Go.",
          toolkit: native,
        });

        const exit = yield* AgentRuntime.run(
          Agent.withModel(agent, scripted([done], requests)),
          "go",
        ).pipe(
          Effect.provideService(RunToolVisibility, { visible: () => Effect.succeed(["run_code"]) }),
          Effect.provide(native.toLayer({ run_code: () => Effect.succeed("") })),
          Effect.exit,
        );

        expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
        expect(requests).toEqual([]);
      }),
  );

  it.effect(
    "resumes against original request exposure without calling the model or discovery",
    () =>
      Effect.gen(function* () {
        const requests: Array<ReadonlyArray<string>> = [];
        let starts = 0;

        const exit = yield* AgentRuntime.run(
          Agent.withModel(definition, scripted([done], requests)),
          "go",
          {
            toolSelector: { select: () => Effect.die("Resumed batch must not select again") },
            resume: {
              turn: 1,
              turnId: TurnId.make("original"),
              calls: [{ id: "hidden", name: "write", params: {} }],
              settled: [],
              toolExposure: Snapshot.make({
                exposedToolNames: ["discover", "status"],
                selection: Selection.make({ toolNames: [] }),
              }),
            },
            resumeUsage: {
              committedTurns: 1,
              toolCalls: 1,
              modelCalls: 1,
              inputTokens: 0,
              outputTokens: 0,
              lastInputTokens: 0,
              lastOutputTokens: 0,
              costMicrousd: 0,
              consecutiveToolFailures: 0,
              programmaticToolCalls: 0,
              finalizationUsed: false,
            },
          },
        ).pipe(
          Effect.provide(
            tools.toLayer({
              discover: () => Effect.succeed({ toolNames: [], padding: "" }),
              read: () => Effect.succeed(""),
              write: () =>
                Effect.sync(() => {
                  starts++;

                  return "";
                }),
              status: () => Effect.succeed(""),
            }),
          ),
          Effect.exit,
        );

        expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
        expect(starts).toBe(0);
        expect(requests).toEqual([]);
      }),
  );
  it.effect("reuses a hidden settled sibling while authorizing only unfinished calls", () =>
    Effect.gen(function* () {
      const native = Toolkit.make(Read, Write);

      const agent = Agent.make("resumed-visible", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer.",
        toolkit: native,
      });

      const requests: Array<ReadonlyArray<string>> = [];
      let readCalls = 0;
      let writeCalls = 0;

      const result = yield* AgentRuntime.run(
        Agent.withModel(agent, scripted([done], requests)),
        "go",
        {
          resume: {
            turn: 1,
            turnId: TurnId.make("original"),
            calls: [
              { id: "read-1", name: "read", params: {} },
              { id: "write-1", name: "write", params: {} },
            ],
            settled: [{ id: "read-1", result: "already read", isFailure: false }],
            toolExposure: Snapshot.make({
              exposedToolNames: ["read", "write"],
              selection: Selection.make({ toolNames: ["read", "write"] }),
            }),
          },
          resumeUsage: {
            committedTurns: 1,
            toolCalls: 2,
            modelCalls: 1,
            inputTokens: 0,
            outputTokens: 0,
            lastInputTokens: 0,
            lastOutputTokens: 0,
            costMicrousd: 0,
            consecutiveToolFailures: 0,
            programmaticToolCalls: 0,
            finalizationUsed: false,
          },
        },
      ).pipe(
        Effect.provideService(RunToolVisibility, { visible: () => Effect.succeed(["write"]) }),
        Effect.provide(
          native.toLayer({
            read: () =>
              Effect.sync(() => {
                readCalls++;

                return "";
              }),
            write: () =>
              Effect.sync(() => {
                writeCalls++;

                return "";
              }),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(readCalls).toBe(0);
      expect(writeCalls).toBe(1);
      expect(requests).toEqual([["write"]]);
    }),
  );

  for (const [label, snapshot] of [
    ["missing", undefined],
    [
      "retired selection",
      Snapshot.make({
        exposedToolNames: ["read"],
        selection: Selection.make({ toolNames: ["retired"] }),
      }),
    ],
    ["retired exposure", Snapshot.make({ exposedToolNames: ["read", "retired"] })],
    ["unexposed call", Snapshot.make({ exposedToolNames: ["retired"] })],
  ] as const) {
    it.effect(
      `preserves original authority with ${label} in a visibility-only resumed request`,
      () =>
        Effect.gen(function* () {
          const native = Toolkit.make(Read);

          const agent = Agent.make("invalid-resume-exposure", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Answer.",
            toolkit: native,
          });

          const requests: Array<ReadonlyArray<string>> = [];
          let starts = 0;

          const exit = yield* AgentRuntime.run(
            Agent.withModel(agent, scripted([done], requests)),
            "go",
            {
              resume: {
                turn: 1,
                turnId: TurnId.make("original"),
                calls: [{ id: "read-1", name: "read", params: {} }],
                settled: [],
                ...(snapshot === undefined ? {} : { toolExposure: snapshot }),
              },
              resumeUsage: {
                committedTurns: 1,
                toolCalls: 1,
                modelCalls: 1,
                inputTokens: 0,
                outputTokens: 0,
                lastInputTokens: 0,
                lastOutputTokens: 0,
                costMicrousd: 0,
                consecutiveToolFailures: 0,
                programmaticToolCalls: 0,
                finalizationUsed: false,
              },
            },
          ).pipe(
            Effect.provideService(RunToolVisibility, { visible: () => Effect.succeed(["read"]) }),
            Effect.provide(
              native.toLayer({
                read: () =>
                  Effect.sync(() => {
                    starts++;

                    return "";
                  }),
              }),
            ),
            Effect.exit,
          );

          if (label === "missing" || label === "unexposed call") {
            expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
            expect(starts).toBe(0);
            expect(requests).toEqual([]);
          } else {
            expect(Exit.isSuccess(exit)).toBe(true);
            expect(starts).toBe(1);
            expect(requests).toEqual([label === "retired selection" ? [] : ["read"]]);
          }
        }),
    );
  }

  it.effect("records the actual oneOf declarations in an optional-completion grace Turn", () =>
    Effect.gen(function* () {
      const Finish = Tool.make("finish", { parameters: Schema.Struct({}), success: Schema.String });
      const native = Toolkit.make(Read, Finish);

      const agent = Agent.make("grace-exposure", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Read then finish.",
        toolkit: native,
        toolExposure: { initialToolNames: ["read"] },
        policy: { maxTurns: 1, maxToolCalls: 5, onExhaustion: "final-answer" },
        completion: { tool: "finish", project: ({ result }) => result },
      });

      const requests: Array<ReadonlyArray<string>> = [];
      const staged: Array<Snapshot> = [];

      const result = yield* AgentRuntime.run(
        Agent.withModel(
          agent,
          scripted(
            [
              [call("read-1", "read"), finish],
              [call("finish-1", "finish"), finish],
            ],
            requests,
          ),
        ),
        "go",
        {
          durability: {
            noteToolExposure: (_, snapshot) =>
              Effect.sync(() => {
                staged.push(snapshot);
              }),
            commitResponse: () => Effect.void,
            prepareToolCalls: () => Effect.void,
            commitCompaction: () => Effect.void,
            noteTurnUsage: () => Effect.void,
            step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
          },
        },
      ).pipe(
        Effect.provide(
          native.toLayer({
            read: () => Effect.succeed("read"),
            finish: () => Effect.succeed("done"),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(requests).toEqual([["read"], ["finish"]]);
      expect(staged.map((snapshot) => snapshot.exposedToolNames)).toEqual(requests);
      expect(staged[1]?.selection?.toolNames).toEqual(["read"]);
    }),
  );

  for (const authority of ["host", "grant"] as const) {
    it.effect(`finishes with text when ${authority} hides an optional completion Tool`, () =>
      Effect.gen(function* () {
        const Finish = Tool.make("finish", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        }).annotate(PinnedTool, true);

        const native = Toolkit.make(Read, Finish);

        const agent = Agent.make("hidden-optional-completion", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Read then answer.",
          toolkit: native,
          toolExposure: { initialToolNames: ["read"] },
          policy: { maxTurns: 1, maxToolCalls: 5, onExhaustion: "final-answer" },
          completion: { tool: "finish", project: ({ result }) => result },
        });

        const requests: Array<ReadonlyArray<string>> = [];
        const choices: Array<LanguageModel.ToolChoice<string>> = [];
        const staged: Array<Snapshot> = [];

        const result = yield* AgentRuntime.run(
          Agent.withModel(
            agent,
            scripted([[call("read-1", "read"), finish], done], requests, choices),
          ),
          "go",
          {
            ...(authority === "grant"
              ? {
                  subagentGrant: SubagentGrant.make({ allowedToolNames: ["read"], maxDepth: 1 }),
                  delegationDepth: 1,
                }
              : {}),
            durability: {
              noteToolExposure: (_, snapshot) =>
                Effect.sync(() => {
                  staged.push(snapshot);
                }),
              commitResponse: () => Effect.void,
              prepareToolCalls: () => Effect.void,
              commitCompaction: () => Effect.void,
              noteTurnUsage: () => Effect.void,
              step: { lookup: () => Effect.succeed(Option.none()), commit: () => Effect.void },
            },
          },
        ).pipe(
          Effect.provideService(
            RunToolVisibility,
            authority === "host"
              ? {
                  visible: ({ toolNames }) =>
                    Effect.succeed(toolNames.filter((name) => name !== "finish")),
                }
              : undefined,
          ),
          Effect.provide(
            native.toLayer({
              read: () => Effect.succeed("read"),
              finish: () => Effect.die("Hidden optional completion must not execute"),
            }),
          ),
        );

        expect(result.output).toBe("done");
        expect(result.finishReason).toBe("budget-exhausted");
        expect(requests).toEqual([["read"], ["read"]]);
        expect(choices).toEqual(["auto", "none"]);
        expect(staged.map((snapshot) => snapshot.exposedToolNames)).toEqual(requests);
      }),
    );
  }

  it.effect("keeps legacy eager completion free of opt-in exposure bounds", () =>
    Effect.gen(function* () {
      const Finish = Tool.make("finish", {
        description: "x".repeat(300_000),
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const native = Toolkit.make(Read, Finish);

      const agent = Agent.make("eager-grace", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Read then finish.",
        toolkit: native,
        policy: { maxTurns: 1, maxToolCalls: 5, onExhaustion: "final-answer" },
        completion: { tool: "finish", project: ({ result }) => result },
      });

      const requests: Array<ReadonlyArray<string>> = [];

      const result = yield* AgentRuntime.run(
        Agent.withModel(
          agent,
          scripted(
            [
              [call("read-1", "read"), finish],
              [call("finish-1", "finish"), finish],
            ],
            requests,
          ),
        ),
        "go",
      ).pipe(
        Effect.provide(
          native.toLayer({
            read: () => Effect.succeed("read"),
            finish: () => Effect.succeed("done"),
          }),
        ),
      );

      expect(result.output).toBe("done");
      expect(requests).toEqual([["read", "finish"], ["finish"]]);
    }),
  );

  it.effect("refuses non-readonly discovery before model or Handler execution", () =>
    Effect.gen(function* () {
      const unsafe = Tool.make("unsafe", {
        parameters: Schema.Struct({}),
        success: Schema.Struct({ toolNames: Schema.Array(Schema.String) }),
      }).annotate(DiscoveryTool, true);

      const native = Toolkit.make(unsafe);

      const agent = Agent.make("unsafe-discovery", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Go.",
        toolkit: native,
        toolExposure: {},
      });

      const requests: Array<ReadonlyArray<string>> = [];
      let starts = 0;

      const exit = yield* AgentRuntime.run(
        Agent.withModel(agent, scripted([done], requests)),
        "go",
      ).pipe(
        Effect.provide(
          native.toLayer({
            unsafe: () =>
              Effect.sync(() => {
                starts++;

                return { toolNames: [] };
              }),
          }),
        ),
        Effect.exit,
      );

      expect(failure(exit)).toMatchObject({
        _tag: "ModelProtocolError",
        message: "Discovery requires ordinary readonly Tools",
      });
      expect(starts).toBe(0);
      expect(requests).toEqual([]);
    }),
  );
});
