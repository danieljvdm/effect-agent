import * as Agent from "@effect-agent/core/Agent";
import { AgentOutputError } from "@effect-agent/core/AgentError";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, RunId, TurnId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Logger, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, type Response, Tool, Toolkit } from "effect/unstable/ai";

import { insertOutputContract, outputSchemaContract } from "../src/internal/output-contract.ts";
import { RunContextPreparationPassthrough } from "../src/RunOptions.ts";
import { ThreadHistory } from "../src/ThreadHistory.ts";

/**
 * The model-visible final-output contract (RUN-028, TEST-016).
 *
 * These suites cover the default's three claims: the contract rides
 * every model request adjacent to the last system block; official history
 * never contains it; and a live-shaped model — one that derives its answer
 * only from the request it received, never from test-known expected values —
 * conforms because the engine communicated the Schema, and reproduces the
 * issue #41 live failure when the engine does not.
 */

const contractMarker = "Final output contract:";

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-1")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-1")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-1")),
});

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }

  return failure.value;
};

const systemText = (message: Prompt.Message): string =>
  message.role === "system" && typeof message.content === "string" ? message.content : "";

const contractMessages = (prompt: Prompt.Prompt): ReadonlyArray<string> =>
  prompt.content.map(systemText).filter((text) => text.startsWith(contractMarker));

/**
 * Build the smallest instance of a derived JSON Schema. Deliberately partial:
 * the live-shaped substitute must fail loudly on fragments it does not
 * understand rather than fabricate output the request never described.
 */
const instanceFromJsonSchema = (schema: unknown): unknown => {
  if (typeof schema !== "object" || schema === null) {
    throw new Error(`live-shaped substitute cannot instantiate ${JSON.stringify(schema)}`);
  }
  const record = schema as Record<string, unknown>;

  if (Array.isArray(record.enum)) {
    return record.enum[0];
  }
  switch (record.type) {
    case "object": {
      const properties =
        typeof record.properties === "object" && record.properties !== null
          ? (record.properties as Record<string, unknown>)
          : {};

      const required = Array.isArray(record.required) ? record.required : [];
      const instance: Record<string, unknown> = {};

      for (const key of required) {
        if (typeof key === "string") {
          instance[key] = instanceFromJsonSchema(properties[key]);
        }
      }

      return instance;
    }
    case "string":
      return "live-shaped";
    case "integer":
    case "number":
      return 1;
    case "boolean":
      return true;
    case "array":
      return [];
    case "null":
      return null;
    default:
      throw new Error(`live-shaped substitute cannot instantiate ${JSON.stringify(schema)}`);
  }
};

/**
 * A live-shaped LanguageModel: its final message derives only from the
 * request it received. When the request advertises the final-output contract
 * it emits the smallest conforming JSON instance of the advertised schema;
 * otherwise it answers in prose — the way the live path failed in issue #41.
 */
const liveShapedModel = (captured: Array<Prompt.Prompt>) =>
  Model.make(
    "live-shaped",
    "request-derived",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) => {
          captured.push(options.prompt);
          const contract = contractMessages(options.prompt).at(0);

          if (contract === undefined) {
            return Stream.fromIterable(
              finalParts("Here is a prose summary of the answer, as requested."),
            );
          }
          const advertised: unknown = JSON.parse(contract.slice(contract.indexOf("\n\n") + 2));

          return Stream.fromIterable(
            finalParts(JSON.stringify(instanceFromJsonSchema(advertised))),
          );
        },
      }),
    ),
  );

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer)("RUN-028 model-visible output contract", (it) => {
  it.effect(
    "carries the contract on every Turn's request adjacent to the last system block and never in official history",
    () => {
      const Search = Tool.make("search", {
        parameters: Schema.Struct({ query: Schema.String }),
        success: Schema.Struct({ available: Schema.Boolean }),
      });

      const tools = Toolkit.make(Search);
      const requests: Array<Prompt.Prompt> = [];
      const histories: Array<Prompt.Prompt> = [];

      const model = Model.make(
        "scripted",
        "two-turn-contract",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.gen(function* () {
            const turn = yield* Ref.make(0);

            return yield* LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (options) =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                    Effect.map((value) => {
                      requests.push(options.prompt);

                      return Stream.fromIterable<Response.StreamPartEncoded>(
                        value === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "search-1",
                                name: "search",
                                params: { query: "sea" },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts('{"answer":"A flight is available."}'),
                      );
                    }),
                  ),
                ),
            });
          }),
        ),
      );

      const definition = Agent.make("contract-two-turn", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Search before answering.",
        toolkit: tools,
        policy,
      });

      const agent = Agent.withModel(definition, model);

      const toolLayer = tools.toLayer({
        search: () => Effect.succeed({ available: true }),
      });

      return Effect.gen(function* () {
        const events = yield* AgentRuntime.stream(
          agent,
          { question: "Is a flight available?" },
          { onHistory: (history) => Effect.sync(() => void histories.push(history)) },
        ).pipe(Stream.runCollect, Effect.provide(toolLayer));

        expect(events.at(-1)).toMatchObject({
          _tag: "RunCompleted",
          output: { answer: "A flight is available." },
        });

        expect(requests).toHaveLength(2);
        const [first, second] = requests;

        // Trailing user message on each request: the derived run-status line
        // (RUN-024), another non-canonical request projection.
        expect(first?.content.map((message) => message.role)).toEqual([
          "system",
          "system",
          "user",
          "user",
        ]);
        expect(second?.content.map((message) => message.role)).toEqual([
          "system",
          "system",
          "user",
          "assistant",
          "tool",
          "user",
        ]);
        for (const request of requests) {
          expect(request === undefined ? [] : contractMessages(request)).toHaveLength(1);
          const contract = systemText(request!.content[1]!);

          expect(contract.startsWith(contractMarker)).toBe(true);
          expect(contract).toContain('"answer"');
          expect(contract).toContain('"type": "object"');
        }
        // The contract is a request-time projection: official history stays
        // clean, so canonical records and the DN/DC golden are unchanged.
        expect(histories.length).toBeGreaterThan(0);
        for (const history of histories) {
          expect(contractMessages(history)).toHaveLength(0);
        }
      });
    },
  );

  it.effect(
    "TEST-016 a live-shaped model conforms because the engine communicated the Schema it had never seen",
    () => {
      const captured: Array<Prompt.Prompt> = [];

      const definition = Agent.make("contract-live-shaped", {
        input: Schema.Struct({ question: Schema.String }),
        // Deliberately no JSON-shape prose in the instructions: conformance
        // may come only from the engine-communicated contract.
        output: Schema.Struct({ answer: Schema.String, itemCount: Schema.Int }),
        instructions: "Answer the question.",
        toolkit: Toolkit.empty,
        policy,
      });

      const agent = Agent.withModel(definition, liveShapedModel(captured));

      return Effect.gen(function* () {
        const result = yield* AgentRuntime.run(agent, { question: "How many items?" });

        expect(result.output).toEqual({ answer: "live-shaped", itemCount: 1 });
        expect(captured).toHaveLength(1);
        expect(contractMessages(captured[0]!)).toHaveLength(1);
      });
    },
  );

  it.effect(
    "advertises the completion Tool parameter contract and retains final-text JSON completion",
    () => {
      const DeliverReply = Tool.make("deliver_reply", {
        parameters: Schema.Struct({ message: Schema.String }),
        success: Schema.Struct({ answer: Schema.String, itemCount: Schema.Int }),
      });

      const tools = Toolkit.make(DeliverReply);
      const captured: Array<Prompt.Prompt> = [];
      const histories: Array<Prompt.Prompt> = [];

      const definition = Agent.make("contract-completion-tool", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String, itemCount: Schema.Int }),
        instructions: "Answer the question.",
        toolkit: tools,
        policy,
        completion: { tool: "deliver_reply", project: ({ result }) => result },
      });

      const agent = Agent.withModel(definition, liveShapedModel(captured));

      const toolLayer = tools.toLayer({
        deliver_reply: () =>
          Effect.die("A final-text response must not invoke the completion Tool"),
      });

      return Effect.gen(function* () {
        const result = yield* AgentRuntime.run(
          agent,
          { question: "How many items?" },
          { onHistory: (history) => Effect.sync(() => void histories.push(history)) },
        ).pipe(Effect.provide(toolLayer));

        expect(result.output).toEqual({ answer: "live-shaped", itemCount: 1 });
        expect(captured).toHaveLength(1);
        const request = captured[0];

        if (request === undefined) throw new Error("Missing model request");
        const contracts = contractMessages(request);

        expect(contracts).toHaveLength(1);
        expect(contracts[0]?.split("\n\n")[0]).toBe(
          'Final output contract: when the task is complete without calling the "deliver_reply" completion Tool, the final assistant message must be only ' +
            "JSON that is valid against this JSON Schema — no prose, no Markdown code fences, nothing " +
            'before or after the JSON. When calling the "deliver_reply" completion Tool, never place this private Agent output JSON in any Tool argument; follow the Tool\'s parameter schema instead. The engine projects the successful completion Tool result into the Agent output.',
        );
        expect(contracts[0]).toContain('"itemCount"');
        expect(histories.length).toBeGreaterThan(0);
        for (const history of histories) expect(contractMessages(history)).toHaveLength(0);
      });
    },
  );

  it.effect(
    "an unrenderable output Schema falls back to the prior behavior and the live-shaped model reproduces the issue #41 failure",
    () => {
      const captured: Array<Prompt.Prompt> = [];

      // A trailing rest element cannot be represented as JSON Schema by the
      // pinned Effect derivation, so no contract can be rendered.
      const definition = Agent.make("contract-unrenderable", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Int, Schema.String]),
        instructions: "Answer the question.",
        toolkit: Toolkit.empty,
        policy,
      });

      const agent = Agent.withModel(definition, liveShapedModel(captured));

      return Effect.gen(function* () {
        const exit = yield* AgentRuntime.run(agent, { question: "How many items?" }).pipe(
          Effect.exit,
        );

        const failure = failureFrom(exit);

        expect(failure).toBeInstanceOf(AgentOutputError);
        expect((failure as AgentOutputError).message).toContain("not valid JSON");
        expect(captured).toHaveLength(1);
        expect(captured[0]?.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "user",
        ]);
        expect(contractMessages(captured[0]!)).toHaveLength(0);
      });
    },
  );

  it.effect(
    "exposes the exact contract to context preparation so adapters can reserve its overhead",
    () => {
      const captured: Array<Prompt.Prompt> = [];
      const observedContracts: Array<string | undefined> = [];

      const definition = Agent.make("contract-context-visibility", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer the question.",
        toolkit: Toolkit.empty,
        policy,
      });

      const agent = Agent.withModel(definition, liveShapedModel(captured));

      return Effect.gen(function* () {
        const result = yield* AgentRuntime.run(
          agent,
          { question: "How big is the request?" },
          {
            context: {
              prepare: (request) =>
                Effect.sync(() => {
                  observedContracts.push(request.outputContract);

                  return { prompt: request.source };
                }),
            },
          },
        );

        expect(result.output).toEqual({ answer: "live-shaped" });
        expect(observedContracts).toHaveLength(1);
        // Byte-equal with the message actually appended to the request: an
        // adapter compacting toward a model-input limit can reserve exactly
        // this overhead before the engine composes the final request.
        const appended = contractMessages(captured[0]!).at(0);

        expect(appended).toBeDefined();
        expect(observedContracts[0]).toBe(appended);
      });
    },
  );

  it.effect(
    "logs the unrenderable diagnostic exactly once at Turn 1 and still completes when the final text decodes",
    () => {
      const Search = Tool.make("search", {
        parameters: Schema.Struct({ query: Schema.String }),
        success: Schema.Struct({ available: Schema.Boolean }),
      });

      const tools = Toolkit.make(Search);
      const observedContracts: Array<string | undefined> = [];
      const warnings: Array<string> = [];

      const logger = Logger.make<unknown, void>(({ logLevel, message }) => {
        const rendered = Array.isArray(message) ? message.join(" ") : String(message);

        if (logLevel === "Warn" && rendered.includes("cannot render to JSON Schema")) {
          warnings.push(rendered);
        }
      });

      const model = Model.make(
        "scripted",
        "two-turn-unrenderable",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.gen(function* () {
            const turn = yield* Ref.make(0);

            return yield* LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                    Effect.map((value) =>
                      Stream.fromIterable<Response.StreamPartEncoded>(
                        value === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "search-1",
                                name: "search",
                                params: { query: "sea" },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : finalParts('["first",1,"last"]'),
                      ),
                    ),
                  ),
                ),
            });
          }),
        ),
      );

      const definition = Agent.make("contract-unrenderable-two-turn", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.TupleWithRest(Schema.Tuple([Schema.String]), [Schema.Int, Schema.String]),
        instructions: "Search before answering.",
        toolkit: tools,
        policy,
      });

      const agent = Agent.withModel(definition, model);

      const toolLayer = tools.toLayer({
        search: () => Effect.succeed({ available: true }),
      });

      const observedOwnKeys: Array<boolean> = [];

      return Effect.gen(function* () {
        const result = yield* AgentRuntime.run(
          agent,
          { question: "Is a flight available?" },
          {
            context: {
              prepare: (request) =>
                Effect.sync(() => {
                  observedContracts.push(request.outputContract);
                  observedOwnKeys.push(Object.hasOwn(request, "outputContract"));

                  return { prompt: request.source };
                }),
            },
          },
        ).pipe(Effect.provide([toolLayer, Logger.layer([logger])]));

        // An unrenderable Schema still succeeds exactly as before when the
        // final text happens to decode; the fallback removes the hint, never
        // the Run.
        expect(result.output).toEqual(["first", 1, "last"]);
        expect(result.turns).toBe(2);
        // The diagnostic fires once for this Attempt's Turn 1 — never on a
        // later Turn. A recovering DN/DC Attempt that re-executes Turn 1 may
        // repeat it: recovery is at-least-once, and no exactly-once claim is
        // made for the log line.
        expect(warnings).toHaveLength(1);
        // The fallback request is byte-identical to the prior behavior: the
        // key is entirely absent, not present-with-undefined.
        expect(observedContracts).toEqual([undefined, undefined]);
        expect(observedOwnKeys).toEqual([false, false]);
      });
    },
  );

  it.effect("renders deterministically from the definition alone", () => {
    const definition = Agent.make("contract-deterministic", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer.",
      toolkit: Toolkit.empty,
      policy,
    });

    const first = outputSchemaContract(definition);
    const second = outputSchemaContract(definition);

    expect(second).toEqual(first);
    expect(first._tag).toBe("rendered");
    if (first._tag === "rendered") {
      expect(first.message.split("\n\n")[0]).toBe(
        "Final output contract: when the task is complete, the final assistant message must be only " +
          "JSON that is valid against this JSON Schema — no prose, no Markdown code fences, nothing " +
          "before or after the JSON.",
      );
      expect(first.message).toContain('"answer"');
    }

    return Effect.void;
  });

  it.effect("directs required native completion without a contradictory text JSON contract", () => {
    const Complete = Tool.make("complete", {
      parameters: Schema.Struct({ answer: Schema.String }),
      success: Schema.Void,
    });

    const definition = Agent.make("required-completion-contract", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Complete through the Tool.",
      toolkit: Toolkit.make(Complete),
      policy,
      completion: {
        tool: "complete",
        required: true,
        project: ({ parameters }) => parameters,
      },
    });

    const contract = outputSchemaContract(definition);

    expect(contract._tag).toBe("rendered");
    if (contract._tag === "rendered") {
      expect(contract.message).toContain('required completion Tool "complete"');
      expect(contract.message).toContain("Do not emit an ordinary final assistant text answer");
      expect(contract.message).not.toContain("final assistant message must be only JSON");
      expect(contract.message).not.toContain('"answer"');
    }

    return Effect.void;
  });

  it.effect("inserts after the last system message, extending the last contiguous block", () => {
    const contract = "contract-text";
    const system = (content: string) => Prompt.makeMessage("system", { content });

    const user = Prompt.makeMessage("user", {
      content: [Prompt.makePart("text", { text: "hi" })],
    });

    const roles = (prompt: Prompt.Prompt) => prompt.content.map((message) => message.role);
    const empty = insertOutputContract(Prompt.empty, contract);

    expect(roles(empty)).toEqual(["system"]);
    expect(systemText(empty.content[0]!)).toBe(contract);

    const userOnly = insertOutputContract(Prompt.fromMessages([user]), contract);

    expect(roles(userOnly)).toEqual(["system", "user"]);
    expect(systemText(userOnly.content[0]!)).toBe(contract);

    const doubleSystem = insertOutputContract(
      Prompt.fromMessages([system("a"), system("b"), user]),
      contract,
    );

    expect(roles(doubleSystem)).toEqual(["system", "system", "system", "user"]);
    expect(systemText(doubleSystem.content[2]!)).toBe(contract);

    // Only the last contiguous system group survives on Anthropic (a prior
    // Thread's instructions ahead of this Run's evaluated instructions,
    // for example), so the contract extends the LAST block, never an earlier
    // one.
    const resumed = insertOutputContract(
      Prompt.fromMessages([system("old-thread"), user, system("new-instructions"), user]),
      contract,
    );

    expect(roles(resumed)).toEqual(["system", "user", "system", "system", "user"]);
    expect(systemText(resumed.content[3]!)).toBe(contract);

    return Effect.void;
  });
});
