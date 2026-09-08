import { InboxPage } from "@effect-agent/core/Messaging";
import { WorkerStarted } from "@effect-agent/core/Worker";
import { Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, type Prompt, type Response } from "effect/unstable/ai";

import { CoordinatorInput, Task, Question, Finding, type Models } from "../../src/agents.ts";

const ScriptedInput = Schema.Union([CoordinatorInput, Task, Question]);
const usage = { inputTokens: {}, outputTokens: {} };

const final = (answer: Schema.Json): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify(answer) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const call = (name: string, params: Schema.Json): Response.StreamPartEncoded => ({
  type: "tool-call",
  id: `${name}-call`,
  name,
  params,
  providerExecuted: false,
});

const calls = (
  ...values: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...values,
  { type: "finish", reason: "tool-calls", usage },
];

/** Deterministic native models used only by the host contract tests. */
const scripted = (
  name: string,
  script: (prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) => Stream.fromIterable(script(prompt)),
      }),
    ),
  );

const currentInput = (prompt: Prompt.Prompt): unknown => {
  // A queued input may join an active Run. Instructions still describe its first
  // input. Process pending commands before later report/recommendation notifications.
  let latest: typeof ScriptedInput.Type | undefined;

  for (const [index, message] of prompt.content.entries()) {
    if (message.role !== "user") continue;
    for (const part of message.content) {
      if (part.type !== "text") continue;
      try {
        const input: unknown = JSON.parse(part.text);

        if (!Schema.is(ScriptedInput)(input)) continue;
        latest = input;
        if (!("_tag" in input) || (input._tag !== "Launch" && input._tag !== "Continue")) continue;

        const expected =
          input._tag === "Launch" ? ["build_a_start", "build_b_start"] : ["build_a_follow_up"];

        const settledNames = prompt.content
          .slice(index + 1)
          .flatMap((later) =>
            later.role === "tool"
              ? later.content.flatMap((value) => (value.type === "tool-result" ? [value.name] : []))
              : [],
          );

        if (!expected.every((name) => settledNames.includes(name))) return input;
      } catch {
        // Derived run-status messages are not application inputs.
      }
    }
  }
  if (latest !== undefined) return latest;
  const system = prompt.content.find((message) => message.role === "system");

  if (system === undefined) throw new Error("Missing scripted input instructions");

  return JSON.parse(system.content);
};

const results = (prompt: Prompt.Prompt) =>
  prompt.content.flatMap((message) =>
    message.role === "tool" ? message.content.filter((part) => part.type === "tool-result") : [],
  );

const currentResults = (prompt: Prompt.Prompt) => {
  const encoded = JSON.stringify(currentInput(prompt));

  const lastInput = prompt.content.findLastIndex(
    (message) =>
      message.role === "user" &&
      message.content.some((part) => part.type === "text" && part.text === encoded),
  );

  return prompt.content
    .slice(lastInput + 1)
    .flatMap((message) =>
      message.role === "tool" ? message.content.filter((part) => part.type === "tool-result") : [],
    );
};

const builder = (name: "A" | "B") =>
  scripted(`builder-${name}`, (prompt) => {
    const input = Schema.decodeUnknownSync(Task)(currentInput(prompt));
    const result = currentResults(prompt).find((part) => part.name === "scout");

    if (result === undefined) return calls(call("scout", input));
    const finding = Schema.decodeUnknownSync(Finding)(result.result);

    return final({ plan: `${name}: ${finding.finding}` });
  });

export const models: Models = {
  scout: scripted("scout", (prompt) => {
    const input = Schema.decodeUnknownSync(Task)(currentInput(prompt));

    return final({ finding: `Verified a small step for ${input.task}` });
  }),
  builderA: builder("A"),
  builderB: builder("B"),
  coordinator: scripted("coordinator", (prompt) => {
    const input = Schema.decodeUnknownSync(CoordinatorInput)(currentInput(prompt));
    const recent = currentResults(prompt);

    if (input._tag === "Launch") {
      if (recent.length === 0)
        return calls(
          call("build_a_start", { task: `${input.mission} (A)` }),
          call("build_b_start", { task: `${input.mission} (B)` }),
        );

      return final({
        answer: "Both builders accepted. I can take another input while their scouts work.",
      });
    }
    if (input._tag === "Continue") {
      if (recent.length === 0) {
        const started = results(prompt).find((part) => part.name === "build_a_start");

        if (started === undefined)
          return final({ answer: "Launch the builders before continuing." });
        const { worker } = Schema.decodeUnknownSync(WorkerStarted)(started.result);

        return calls(call("build_a_follow_up", { worker, parameters: { task: input.note } }));
      }

      return final({ answer: "Builder A accepted the follow-up on its existing Thread." });
    }

    return final({
      answer:
        input._tag === "Report"
          ? `Received ${input.builder}: ${input.summary}`
          : `Advisor recommends: ${input.text}`,
    });
  }),
  advisor: scripted("advisor", (prompt) => {
    const recent = currentResults(prompt);
    const received = recent.find((part) => part.name === "coordinator_inbox");

    if (received === undefined) return calls(call("coordinator_inbox", { limit: 20 }));
    if (!recent.some((part) => part.name === "coordinator_reply")) {
      const page = Schema.decodeUnknownSync(InboxPage)(received.result);
      const message = page.items.at(-1)?.admission.message;

      if (message === undefined) return final({ answer: "No authenticated request to reply to." });

      return calls(
        call("coordinator_reply", {
          inReplyTo: message,
          input: { _tag: "Recommendation", text: "Ship the smallest verified step first." },
        }),
      );
    }

    return final({ answer: "Recommendation sent through the recorded return route." });
  }),
};
