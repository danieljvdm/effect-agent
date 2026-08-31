import { Agent, AgentPolicy, type ConversationId, type IdGenerator } from "@effect-agent/core";
import type { AgentRuntimeFailure } from "@effect-agent/engine";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Schema } from "effect";
import { Toolkit, type LanguageModel, type Model } from "effect/unstable/ai";

import { PersistentConversations, type PersistentConversationError } from "../src/history.ts";
import {
  type DurableAgentRuntime,
  DurableWorkerBinding,
  type DefinitionDigests,
  type DurableWorkerRequirements,
  type ConversationStore,
  type ConversationStoreFailure,
  type RunJournalError,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

class InputProjectionFailure extends Schema.TaggedError<InputProjectionFailure>()(
  "InputProjectionFailure",
  {},
) {}

class InputProjection extends Context.Service<
  InputProjection,
  { readonly render: (question: string) => Effect.Effect<string, InputProjectionFailure> }
>()("@effect-agent/session/test/InputProjection") {}

class InstructionContext extends Context.Service<InstructionContext, { readonly text: string }>()(
  "@effect-agent/session/test/InstructionContext",
) {}

const definition = Agent.define("durable-input-projection-types", {
  input: Schema.Struct({ question: Schema.String, hostOnly: Schema.String }),
  output: Schema.String,
  instructions: () => Effect.map(InstructionContext, ({ text }) => text),
  inputPrompt: ({ question }) =>
    question === "" ? [] : Effect.flatMap(InputProjection, ({ render }) => render(question)),
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  runDisposition: {
    schema: Schema.Literal("answered"),
    fromOutput: () => "answered",
  },
});

const proveWorkerRequirements = (
  runtime: DurableAgentRuntime["Service"],
  model: Model.Model<"test", LanguageModel.LanguageModel, never>,
  conversationId: ConversationId,
  digests: DefinitionDigests,
) => {
  const agent = Agent.withModel(definition, model);
  const process = runtime.processConversation(agent, conversationId);
  const worker = runtime.runWorker(agent);
  const registered = DurableWorkerBinding.make(agent, digests);
  const transparent = DurableWorkerBinding.makeDigestTransparent(agent);
  const retained = PersistentConversations.run(
    agent,
    { question: "hello", hostOnly: "private" },
    { conversationId },
  );

  type Expected = InputProjection | InstructionContext;
  type ProcessProof = Assert<Equal<Effect.Services<typeof process>, Expected>>;
  type WorkerProof = Assert<Equal<Effect.Services<typeof worker>, Expected>>;
  type RegisteredProof = Assert<Equal<Effect.Services<typeof registered>, Expected>>;
  type TransparentProof = Assert<Equal<Effect.Services<typeof transparent>, Expected>>;
  type PublicProof = Assert<Equal<DurableWorkerRequirements<typeof agent>, Expected>>;
  type FailureProof = Assert<
    Equal<Extract<Agent.Failure<typeof agent>, InputProjectionFailure>, InputProjectionFailure>
  >;
  type HistoryRequirementsProof = Assert<
    Equal<Effect.Services<typeof retained>, Expected | ConversationStore | IdGenerator>
  >;
  type HistoryFailureProof = Assert<
    Equal<
      Effect.Error<typeof retained>,
      | AgentRuntimeFailure<typeof agent>
      | ConversationStoreFailure
      | RunJournalError
      | PersistentConversationError
    >
  >;
  type HistoryOutputProof = Assert<Equal<Effect.Success<typeof retained>["output"], string>>;
  const proofs: readonly [
    ProcessProof,
    WorkerProof,
    RegisteredProof,
    TransparentProof,
    PublicProof,
    FailureProof,
    HistoryRequirementsProof,
    HistoryFailureProof,
    HistoryOutputProof,
  ] = [true, true, true, true, true, true, true, true, true];
  return proofs;
};

describe("durable input projection types", () => {
  it("retains projection requirements when capturing a worker binding", () => {
    expect(proveWorkerRequirements).toBeInstanceOf(Function);
  });
});
