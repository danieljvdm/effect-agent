import {
  AgentRunDispositionError,
  AgentApprovalDenied,
  AgentApprovalPending,
  AgentInputError,
  AgentOutputError,
  AgentPolicyError,
  ContextOverflowError,
  IdGenerator,
  ModelProtocolError,
} from "@effect-agent/core";
import {
  AgentRuntime,
  type AgentChildPending,
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
} from "@effect-agent/engine";
import { describe, expect, it } from "@effect/vitest";
import { Effect, type Scope, Schema, Stream } from "effect";
import { type AiError, type Tool, type Toolkit } from "effect/unstable/ai";

import {
  CalculationFailure,
  ChatInput,
  ChatOutput,
  FixtureChatRuntimeLayer,
  FixtureChatToolkit,
  FixtureKnowledge,
  makeFixtureChatAgent,
  searchFixtureKnowledge,
} from "./general-chat";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const fixtureMessage = "What are the best cities to visit in Europe in August?";
const fixtureAgent = makeFixtureChatAgent(fixtureMessage);
const fixtureProgram = AgentRuntime.run(fixtureAgent, { message: fixtureMessage });

type ExpectedRequirements =
  | FixtureKnowledge
  | Tool.HandlersFor<Toolkit.Tools<typeof FixtureChatToolkit>>
  | IdGenerator
  | Scope.Scope;
type ExpectedFailure =
  | CalculationFailure
  | AiError.AiError
  | AgentInputError
  | AgentOutputError
  | AgentRunDispositionError
  | AgentPolicyError
  | ContextOverflowError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentApprovalPending
  | AgentChildPending;
type RequirementsProof = Assert<
  Equal<Effect.Services<typeof fixtureProgram>, ExpectedRequirements>
>;
type FailureProof = Assert<Equal<Effect.Error<typeof fixtureProgram>, ExpectedFailure>>;
type PublicRequirementsProof = Assert<
  Equal<AgentRuntimeRequirements<typeof fixtureAgent> | Scope.Scope, ExpectedRequirements>
>;
type PublicFailureProof = Assert<Equal<AgentRuntimeFailure<typeof fixtureAgent>, ExpectedFailure>>;

describe("general chat fixture profile", () => {
  it("owns a small message/answer contract without hidden trip fields", () => {
    expect(Schema.decodeSync(ChatInput)({ message: fixtureMessage })).toEqual({
      message: fixtureMessage,
    });
    expect(
      Schema.decodeSync(ChatOutput)({
        answer: "A general answer.",
      }),
    ).toEqual({ answer: "A general answer." });
    expect(searchFixtureKnowledge({ query: fixtureMessage })).toMatchObject({
      fixture: true,
      query: fixtureMessage,
      matches: [{ title: "Europe in August · fixture note" }],
    });
  });

  it.effect("runs the submitted message through a real fixture Tool handler", () =>
    Effect.gen(function* () {
      const events = yield* AgentRuntime.stream(fixtureAgent, { message: fixtureMessage }).pipe(
        Stream.runCollect,
        Effect.provide(FixtureChatRuntimeLayer),
        Effect.scoped,
      );
      const declared = events.find((event) => event._tag === "ToolCallDeclared");
      const succeeded = events.find((event) => event._tag === "ToolCallSucceeded");
      const completed = events.find((event) => event._tag === "RunCompleted");

      expect(declared).toMatchObject({
        toolName: "search_fixture_knowledge",
        parameters: { query: fixtureMessage },
        providerExecuted: false,
      });
      expect(succeeded).toMatchObject({
        toolName: "search_fixture_knowledge",
        result: { fixture: true, query: fixtureMessage },
        providerExecuted: false,
      });
      expect(completed).toMatchObject({
        output: {
          answer: expect.stringContaining("deterministic fixture data"),
        },
      });
    }),
  );

  it("preserves fixture Tool failures and requirements in Run E/R", () => {
    const requirementsProof: RequirementsProof = true;
    const failureProof: FailureProof = true;
    const publicRequirementsProof: PublicRequirementsProof = true;
    const publicFailureProof: PublicFailureProof = true;

    expect({
      requirementsProof,
      failureProof,
      publicRequirementsProof,
      publicFailureProof,
    }).toEqual({
      requirementsProof: true,
      failureProof: true,
      publicRequirementsProof: true,
      publicFailureProof: true,
    });
  });
});
