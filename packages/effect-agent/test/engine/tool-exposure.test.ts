import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Option, Schema, SchemaGetter, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { SubagentGrant } from "effect-agent/subagent-contract";
import { ThreadHistory } from "effect-agent/thread-history";
import * as ToolDiscovery from "effect-agent/tool-discovery";
import {
  DiscoveryTool,
  PinnedTool,
  Selection,
  Snapshot,
  CurrentToolCatalog,
  RunToolVisibility,
} from "effect-agent/tool-exposure";
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
  // Regression: https://github.com/danieljvdm/effect-agent/issues/496
  {
    it.effect(
      "continues after discovery result-byte overflow with only documented selections",
      () =>
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
          const maxResultBytes = 1_024;
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

                return ["native:read", "native:write"];
              }),
          });

          const actions = Toolkit.make(
            ReadDocument,
            WriteDocument,
            Status,
            HostHidden,
            GrantHidden,
          );

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
                      toolNames: ["read"],
                      notice: expect.stringMatching(/narrow.*search/i),
                    });
                    expect(decoded.matches.map((match) => match.name)).toEqual(decoded.toolNames);
                    expect(decoded.matches).toMatchObject([
                      {
                        parameters: {
                          properties: { key: { type: "string", description: documentation } },
                        },
                        success: { type: "string" },
                      },
                    ]);
                    expect(options.tools.map((tool) => tool.name).toSorted()).toEqual([
                      "discover_tools",
                      "read",
                      "status",
                    ]);

                    return Stream.fromIterable([call("use", "read", { key: "record" }), finish]);
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
          expect(invoked).toEqual(["read"]);
          expect(finalized).toBe(1);
        }),
    );
  }

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
});
