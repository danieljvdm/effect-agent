import { describe, expect, it } from "vite-plus/test";

import { Schema } from "effect";
import { Tool, type Toolkit } from "effect/unstable/ai";

import { Agent } from "@effect-agent/core";
import { OpenAiChatDefinition, OpenAiChatToolkit, OpenAiWebSearch } from "./openai-profile";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

type ExpectedRequirements = Tool.HandlersFor<Toolkit.Tools<typeof OpenAiChatToolkit>>;
type RequirementsProof = Assert<
  Equal<Agent.DefinitionRequirements<typeof OpenAiChatDefinition>, ExpectedRequirements>
>;

describe("OpenAI hosted-search demo profile", () => {
  it("offers only real arithmetic and provider-hosted search", () => {
    const requirementsProof: RequirementsProof = true;

    expect(requirementsProof).toBe(true);
    expect(Object.keys(OpenAiChatToolkit.tools)).toEqual(["calculate", "OpenAiWebSearch"]);
    expect(Tool.isProviderDefined(OpenAiWebSearch)).toBe(true);
    expect(OpenAiWebSearch.providerName).toBe("web_search");
    expect(OpenAiWebSearch.args).toMatchObject({ search_context_size: "medium" });
    expect(Schema.decodeSync(OpenAiWebSearch.parametersSchema)({})).toEqual({});
    expect(OpenAiChatDefinition.policy).toMatchObject({
      maxTurns: 4,
      maxToolCalls: 6,
      toolConcurrency: 1,
    });
  });
});
