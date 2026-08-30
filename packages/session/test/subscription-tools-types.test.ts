import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Schema } from "effect";
import type { Layer, Crypto } from "effect";
import type { Tool } from "effect/unstable/ai";

import {
  type GitHubWorkflowRunCompletion,
  type GitHubWorkflowRuns,
  type SubscribeToEvent,
  type Subscriptions,
  GitHubRepository,
  makeGitHubWorkflowRunSource,
  type SubscriptionError,
  type SubscriptionSourceError,
  type SubscriptionToolsOptions,
  subscriptionToolsLayer,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const makeToolsLayer = (options: SubscriptionToolsOptions) => subscriptionToolsLayer(options);

type ToolsLayer = ReturnType<typeof makeToolsLayer>;
type ToolsFailureProof = Assert<Equal<Layer.Error<ToolsLayer>, SubscriptionError>>;
type ToolsRequirementsProof = Assert<
  Equal<Layer.Services<ToolsLayer>, Subscriptions | Crypto.Crypto>
>;
type NoDurableStepFailurePayloadProof = Assert<
  Equal<
    Extract<Tool.Failure<typeof SubscribeToEvent>, { readonly _tag: "DurableStepError" }>,
    never
  >
>;

class PrepareInput extends Context.Service<PrepareInput, { readonly prefix: string }>()(
  "@effect-agent/session/test/PrepareInput",
) {}

const sourceEffect = makeGitHubWorkflowRunSource({
  repository: GitHubRepository.make({ id: 1, owner: "effect", name: "agent" }),
  context: Schema.Struct({ message: Schema.String }),
  input: Schema.Struct({ message: Schema.String }),
  prepare: (_completion: GitHubWorkflowRunCompletion, context) =>
    Effect.gen(function* () {
      const service = yield* PrepareInput;
      return { message: `${service.prefix}${context.message}` };
    }),
});

type SourceFailureProof = Assert<Equal<Effect.Error<typeof sourceEffect>, SubscriptionSourceError>>;
type SourceRequirementsProof = Assert<
  Equal<Effect.Services<typeof sourceEffect>, GitHubWorkflowRuns | PrepareInput>
>;

describe("Subscription Tool and GitHub source public types", () => {
  it("keeps typed failures, requirements, and sanitized Tool failures visible", () => {
    const proofs: readonly [
      ToolsFailureProof,
      ToolsRequirementsProof,
      NoDurableStepFailurePayloadProof,
      SourceFailureProof,
      SourceRequirementsProof,
    ] = [true, true, true, true, true];
    expect(proofs).toEqual([true, true, true, true, true]);
  });
});
