import * as Agent from "@effect-agent/core/Agent";
import { RunId, ThreadId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  IncludesCatalogDocumentation,
  PinnedTool,
  Selection,
  Snapshot,
} from "@effect-agent/core/ToolExposure";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ContextRolloverTool } from "@effect-agent/engine/ContextWindow";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { CurrentToolCatalog, RunToolVisibility } from "@effect-agent/engine/ToolExposure";
import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Layer, Option, Schema, SchemaGetter, Stream } from "effect";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(ThreadId.make("exposure-thread")),
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

layer(Layer.mergeAll(identifiers, ThreadHistory.layerTransient))("native Tool exposure", (it) => {
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
        ).pipe(Effect.provide(handlers), Effect.provide(visibility));

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
        ).pipe(Effect.provide(handlers), Effect.provide(visibility), Effect.exit);

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
      "unknown selection",
      Snapshot.make({
        exposedToolNames: ["read"],
        selection: Selection.make({ toolNames: ["missing"] }),
      }),
    ],
    ["unknown exposure", Snapshot.make({ exposedToolNames: ["read", "toString"] })],
  ] as const) {
    it.effect(`rejects ${label} in a visibility-only resumed request before execution`, () =>
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

        expect(failure(exit)).toMatchObject({ _tag: "ModelProtocolError" });
        expect(starts).toBe(0);
        expect(requests).toEqual([]);
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
