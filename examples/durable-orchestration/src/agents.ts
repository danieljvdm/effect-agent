import * as Messaging from "@effect-agent/capabilities/Messaging";
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { InboxPage, MessagingError } from "@effect-agent/core/Messaging";
import { Principal } from "@effect-agent/core/Receipt";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import { WorkerError, WorkerStarted } from "@effect-agent/core/Worker";
import { PeerAuthorizer, PeerRoutes } from "@effect-agent/thread/MessagingHost";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Prompt, type Response } from "effect/unstable/ai";

export const principal = Schema.decodeSync(Principal)("orchestration-demo");
export const rootThread = Schema.decodeSync(ThreadId)("demo-coordinator");
export const advisorThread = Schema.decodeSync(ThreadId)("demo-advisor");
const Text = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));

export const CoordinatorInput = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Launch"), mission: Text }),
  Schema.Struct({ _tag: Schema.Literal("Continue"), note: Text }),
  Schema.Struct({
    _tag: Schema.Literal("Report"),
    builder: Schema.Literals(["A", "B"]),
    runId: RunId,
    summary: Text,
  }),
  Schema.Struct({ _tag: Schema.Literal("Recommendation"), text: Text }),
]);

const Task = Schema.Struct({ task: Text });
const Question = Schema.Struct({ question: Text });
const Finding = Schema.Struct({ finding: Text });
const Plan = Schema.Struct({ plan: Text });
const Answer = Schema.Struct({ answer: Schema.String });
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

/** Deterministic credential-free model. Replace these Model bindings with provider Layers. */
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

export const scout = Agent.withModel(
  Agent.make("demo-scout", {
    input: Task,
    output: Finding,
    instructions: JSON.stringify,
    toolkit: Toolkit.empty,
    policy: {
      maxTurns: 1,
      maxToolCalls: 1,
      maxDuration: "20 seconds",
      toolConcurrency: 1,
      toolResultBounds: { maxBytes: 1024 },
    },
  }),
  scripted("scout", (prompt) => {
    const input = Schema.decodeUnknownSync(Task)(currentInput(prompt));

    return final({ finding: `Verified a small step for ${input.task}` });
  }),
);

export const scoutTask = Subagent.make("scout", {
  target: scout.definition,
  success: Finding,
  projectResult: (value) => Effect.succeed(value),
  grant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 2, childLifetimes: ["attached"] }),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "20 seconds",
    maxResultBytes: 1024,
  }),
});

const builder = (name: "A" | "B") =>
  Agent.withModel(
    Agent.make(`demo-builder-${name}`, {
      input: Task,
      output: Plan,
      instructions: JSON.stringify,
      toolkit: Toolkit.make(scoutTask.tool),
      policy: {
        maxTurns: 2,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
        toolResultBounds: { maxBytes: 1024 },
      },
    }),
    scripted(`builder-${name}`, (prompt) => {
      const input = Schema.decodeUnknownSync(Task)(currentInput(prompt));
      const result = currentResults(prompt).find((part) => part.name === "scout");

      if (result === undefined) return calls(call("scout", input));
      const finding = Schema.decodeUnknownSync(Finding)(result.result);

      return final({ plan: `${name}: ${finding.finding}` });
    }),
  );

export const builderA = builder("A");
export const builderB = builder("B");

const build = <const Name extends string>(name: Name, target: typeof builderA) =>
  Subagent.make(name, {
    target: target.definition,
    success: Plan,
    projectResult: (value) => Effect.succeed(value),
    // This governs builder descendants. Root may launch the builder in the background.
    grant: SubagentGrant.make({
      allowedToolNames: ["scout"],
      maxDepth: 2,
      childLifetimes: ["attached"],
    }),
    // Own builder ceiling is 2 turns / 1 call; the remaining allocation funds its scout.
    policy: Subagent.SubagentPolicy.make({
      maxChildren: 8,
      maxConcurrency: 2,
      maxTurns: 4,
      maxToolCalls: 2,
      maxDuration: "1 minute",
      maxResultBytes: 2048,
      descendantInvocations: 1,
    }),
  });

export const buildA = build("build_a", builderA);
export const buildB = build("build_b", builderB);
const toolsA = Subagent.background(buildA, { start: true, followUp: true });
const toolsB = Subagent.background(buildB, { start: true });

export const coordinator = Agent.withModel(
  Agent.make("demo-coordinator", {
    input: CoordinatorInput,
    output: Answer,
    instructions: JSON.stringify,
    toolkit: Toolkit.make(...Object.values(toolsA.tools), ...Object.values(toolsB.tools)),
    policy: { maxTurns: 64, maxToolCalls: 32, maxDuration: "10 minutes", toolConcurrency: 2 },
  }),
  scripted("coordinator", (prompt) => {
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
);

export const coordinatorPeer = Messaging.peer("coordinator", { target: coordinator.definition });
const inbox = Messaging.inboxTool(coordinatorPeer);
const reply = Messaging.replyTool(coordinatorPeer);

export const advisor = Agent.withModel(
  Agent.make("demo-advisor", {
    input: Question,
    output: Answer,
    instructions: JSON.stringify,
    toolkit: Toolkit.make(inbox.tool, reply.tool),
    policy: { maxTurns: 3, maxToolCalls: 2, maxDuration: "1 minute", toolConcurrency: 1 },
  }),
  scripted("advisor", (prompt) => {
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
);

export const advisorPeer = Messaging.peer("advisor", { target: advisor.definition });

const reporting = (name: "A" | "B", declaration: typeof buildA | typeof buildB) =>
  Subagent.reporting(declaration, {
    input: CoordinatorInput,
    prepare: (report) =>
      Effect.succeed({
        _tag: "Report" as const,
        builder: name,
        runId: report.runId,
        summary: report.outcome === "completed" ? report.result.plan : `${name} ${report.outcome}`,
      }),
  });

const registration = <
  A extends typeof coordinator | typeof builderA | typeof scout | typeof advisor,
>(
  agent: A,
) => ({
  agent,
  definitions: DefinitionDigestInput.make({
    agent: agent.definition.id,
    model: "scripted-v1",
    tools: Object.keys(agent.definition.toolkit.tools),
  }),
});

export const registrations = [
  { ...registration(coordinator), reporting: [reporting("A", buildA), reporting("B", buildB)] },
  registration(builderA),
  registration(builderB),
  registration(scout),
  registration(advisor),
] as const;

export const handlers = Layer.mergeAll(
  toolsA.layer,
  toolsB.layer,
  inbox.layer,
  reply.layer,
  Subagent.SubagentRuntime.layer(scoutTask, scout),
).pipe(Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]));

/** Worker ownership and peer messaging are independent grants. The advisor cannot control workers. */
export const authority = Layer.mergeAll(
  Layer.succeed(WorkerHostAuthorizer)({
    authorize: (request) =>
      request.principal === principal && request.sourceThreadId === rootThread
        ? Effect.succeed(principal)
        : WorkerError.make({ operation: request.operation, reason: "denied" }),
  }),
  Layer.succeed(PeerAuthorizer)({
    authorize: (request) => {
      const fromRoot =
        request.source.threadId === rootThread &&
        request.source.agentId === coordinator.definition.id;

      const fromAdvisor =
        request.source.threadId === advisorThread &&
        request.source.agentId === advisor.definition.id;

      const peerAllowed =
        request.peerName === undefined ||
        request.peerName === (fromRoot ? "advisor" : "coordinator");

      const destinationAllowed =
        request.destination === undefined ||
        (fromRoot
          ? request.destination.threadId === advisorThread &&
            request.destination.agentId === advisor.definition.id
          : request.destination.threadId === rootThread &&
            request.destination.agentId === coordinator.definition.id);

      return request.principal === principal &&
        (fromRoot || fromAdvisor) &&
        peerAllowed &&
        destinationAllowed &&
        request.access !== "control"
        ? Effect.succeed(principal)
        : MessagingError.make({ operation: request.operation, reason: "denied" });
    },
  }),
  Layer.succeed(PeerRoutes)({
    resolve: (request) =>
      request.source.threadId === rootThread &&
      request.peerName === "advisor" &&
      request.targetAgentId === advisor.definition.id
        ? Effect.succeed(advisorThread)
        : MessagingError.make({ operation: "send", reason: "route-unavailable" }),
  }),
);
