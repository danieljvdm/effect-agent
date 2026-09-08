import * as Messaging from "@effect-agent/capabilities/Messaging";
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { MessagingError } from "@effect-agent/core/Messaging";
import { Principal } from "@effect-agent/core/Receipt";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import { WorkerError } from "@effect-agent/core/Worker";
import { PeerAuthorizer, PeerRoutes } from "@effect-agent/thread/MessagingHost";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { Effect, Layer, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";

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

export const Task = Schema.Struct({ task: Text });
export const Question = Schema.Struct({ question: Text });
export const Finding = Schema.Struct({ finding: Text });
export const Plan = Schema.Struct({ plan: Text });
export const Answer = Schema.Struct({ answer: Schema.String });

export const scout = Agent.make("demo-scout", {
  input: Task,
  output: Finding,
  instructions: `You are a scout. Analyze the requested task and propose one concrete, small step to verify it.
You have no filesystem, shell or research tools; do not claim to have executed anything.
Return only JSON matching {"finding":"..."}, with finding at most 256 characters.`,
  toolkit: Toolkit.empty,
  policy: {
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 minute",
    toolConcurrency: 1,
    toolResultBounds: { maxBytes: 1024 },
  },
});

export const scoutTask = Subagent.make("scout", {
  target: scout,
  success: Finding,
  projectResult: (value) => Effect.succeed(value),
  grant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 2, childLifetimes: ["attached"] }),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 minute",
    maxResultBytes: 1024,
  }),
});

const builder = (name: "A" | "B") =>
  Agent.make(`demo-builder-${name}`, {
    input: Task,
    output: Plan,
    instructions: `You are builder ${name}. For each new task, call scout exactly once with that task.
Use its result to propose a concise plan. Return only JSON matching {"plan":"..."}, with plan at most 256 characters.
You propose work; you have no tools to implement or execute it.`,
    toolkit: Toolkit.make(scoutTask.tool),
    policy: {
      maxTurns: 2,
      maxToolCalls: 1,
      maxDuration: "2 minutes",
      toolConcurrency: 1,
      toolResultBounds: { maxBytes: 1024 },
    },
  });

export const builderA = builder("A");
export const builderB = builder("B");

const build = <const Name extends string>(name: Name, target: typeof builderA) =>
  Subagent.make(name, {
    target,
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
      maxDuration: "3 minutes",
      maxResultBytes: 2048,
      descendantInvocations: 1,
    }),
  });

export const buildA = build("build_a", builderA);
export const buildB = build("build_b", builderB);
const toolsA = Subagent.background(buildA, { start: true, followUp: true });
const toolsB = Subagent.background(buildB, { start: true });

export const coordinator = Agent.make("demo-coordinator", {
  input: CoordinatorInput,
  output: Answer,
  instructions: `You coordinate two background builders. Process every pending application command in the conversation, including inputs that arrive after this Run started.
For Launch: call build_a_start and build_b_start exactly once each, passing a concrete task from the mission, then acknowledge acceptance.
For Continue: use the Worker handle returned by build_a_start to call build_a_follow_up with {worker, parameters:{task:note}}. Reuse that worker; never start a replacement.
Do not repeat a command whose tool result is already in the conversation. Report and Recommendation inputs only need acknowledgement; do not launch more work for them.
Return only JSON matching {"answer":"..."}.`,
  toolkit: Toolkit.make(...Object.values(toolsA.tools), ...Object.values(toolsB.tools)),
  policy: { maxTurns: 64, maxToolCalls: 32, maxDuration: "30 minutes", toolConcurrency: 2 },
});

export const coordinatorPeer = Messaging.peer("coordinator", { target: coordinator });
const inbox = Messaging.inboxTool(coordinatorPeer);
const reply = Messaging.replyTool(coordinatorPeer);

export const advisor = Agent.make("demo-advisor", {
  input: Question,
  output: Answer,
  instructions: `You are an independent advisor. For each new request, call coordinator_inbox with limit 20.
Read the newest request's admission.message reference and pass that exact reference as inReplyTo to coordinator_reply.
Reply with input {"_tag":"Recommendation","text":"..."}, giving one practical recommendation in at most 256 characters.
After the reply succeeds, return only JSON matching {"answer":"..."}.`,
  toolkit: Toolkit.make(inbox.tool, reply.tool),
  policy: { maxTurns: 3, maxToolCalls: 2, maxDuration: "2 minutes", toolConcurrency: 1 },
});

export const advisorPeer = Messaging.peer("advisor", { target: advisor });

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

/** Host-supplied native Effect AI model Layers. Tests provide deterministic implementations. */
export interface Models<R = never> {
  readonly scout: Layer.Layer<Agent.ModelServices, never, R>;
  readonly builderA: Layer.Layer<Agent.ModelServices, never, R>;
  readonly builderB: Layer.Layer<Agent.ModelServices, never, R>;
  readonly coordinator: Layer.Layer<Agent.ModelServices, never, R>;
  readonly advisor: Layer.Layer<Agent.ModelServices, never, R>;
}

const registration = <
  A extends typeof coordinator | typeof builderA | typeof scout | typeof advisor,
  R,
>(
  agent: A,
  model: Layer.Layer<Agent.ModelServices, never, R>,
  modelVersion: string,
) => ({
  agent,
  model,
  definitions: DefinitionDigestInput.make({
    agent: { id: agent.id, version: "openai-instructions-v1" },
    model: modelVersion,
    tools: Object.keys(agent.toolkit.tools),
  }),
});

export const registrations = <R>(models: Models<R>, modelVersion: string) =>
  [
    {
      ...registration(coordinator, models.coordinator, modelVersion),
      reporting: [reporting("A", buildA), reporting("B", buildB)],
    },
    registration(builderA, models.builderA, modelVersion),
    registration(builderB, models.builderB, modelVersion),
    registration(scout, models.scout, modelVersion),
    registration(advisor, models.advisor, modelVersion),
  ] as const;

export const handlers = <R>(models: Models<R>) =>
  Layer.mergeAll(
    toolsA.layer,
    toolsB.layer,
    inbox.layer,
    reply.layer,
    Subagent.SubagentRuntime.layer(scoutTask, models.scout),
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
        request.source.threadId === rootThread && request.source.agentId === coordinator.id;

      const fromAdvisor =
        request.source.threadId === advisorThread && request.source.agentId === advisor.id;

      const peerAllowed =
        request.peerName === undefined ||
        request.peerName === (fromRoot ? "advisor" : "coordinator");

      const destinationAllowed =
        request.destination === undefined ||
        (fromRoot
          ? request.destination.threadId === advisorThread &&
            request.destination.agentId === advisor.id
          : request.destination.threadId === rootThread &&
            request.destination.agentId === coordinator.id);

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
      request.targetAgentId === advisor.id
        ? Effect.succeed(advisorThread)
        : MessagingError.make({ operation: "send", reason: "route-unavailable" }),
  }),
);
