import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import type { IdGenerator } from "@effect-agent/core/IdGenerator";
import { IdempotencyKey, type JoinedToHost, Receipt } from "@effect-agent/core/Receipt";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import type { WorkerError } from "@effect-agent/core/Worker";
import type { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { describe, expect, it } from "@effect/vitest";
import { Context, type Crypto, Effect, type Layer, Schema, SchemaGetter } from "effect";
import type { Tool } from "effect/unstable/ai";
import { Toolkit } from "effect/unstable/ai";

import type { SubagentReservations } from "../src/SubagentReservations.ts";

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2
    ? (<T>() => T extends B ? 1 : 2) extends <T>() => T extends A ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
class Prepare extends Context.Service<Prepare, string>()("background-types/Prepare") {}
class Project extends Context.Service<Project, string>()("background-types/Project") {}
class Encoder extends Context.Service<Encoder, string>()("background-types/Encoder") {}
class Decoder extends Context.Service<Decoder, string>()("background-types/Decoder") {}
class ChildInstructions extends Context.Service<ChildInstructions, string>()(
  "background-types/ChildInstructions",
) {}
class DeclaredFailure extends Schema.TaggedError<DeclaredFailure>()("DeclaredFailure", {}) {}

const text = Schema.String.pipe(
  Schema.decodeTo(Schema.String, {
    decode: SchemaGetter.transformOrFail((value) => Effect.as(Decoder, value)),
    encode: SchemaGetter.transformOrFail((value) => Effect.as(Encoder, value)),
  }),
);

const target = Agent.make("typed-background-child", {
  input: text,
  output: text,
  instructions: () => ChildInstructions,
  toolkit: Toolkit.empty,
});

const updateTarget = Agent.make("updating-child", {
  input: Schema.String,
  output: Schema.String,
  updates: Schema.Struct({ finding: text }),
  instructions: "Report findings.",
  toolkit: Toolkit.empty,
});

const updateBackground = Subagent.background(updateTarget, { start: true });
const updateDeclaration = Subagent.make("updating", { target: updateTarget });

const updateLayerProofs = (model: Layer.Layer<Agent.ModelServices>) => {
  const modelLayer = Subagent.SubagentRuntime.layer(updateDeclaration, model);

  const bindingLayer = Subagent.SubagentRuntime.layer(
    updateDeclaration,
    Agent.withModel(updateTarget, model),
  );

  const proofs: [
    Assert<
      Equal<
        Layer.Services<typeof modelLayer>,
        Encoder | Decoder | IdGenerator | SubagentReservations
      >
    >,
    Assert<
      Equal<
        Layer.Services<typeof bindingLayer>,
        Encoder | Decoder | IdGenerator | SubagentReservations
      >
    >,
    Assert<Equal<Layer.Error<typeof modelLayer>, never>>,
    Assert<Equal<Layer.Error<typeof bindingLayer>, never>>,
  ] = [true, true, true, true];

  return proofs;
};

const updateTypes: [
  Assert<Equal<keyof typeof updateBackground.tools, "updating-child_start">>,
  Assert<typeof updateDeclaration.target extends typeof updateTarget ? true : false>,
] = [true, true];

const direct = Subagent.background(target, {
  start: true,
  followUp: true,
  inspect: true,
  reportToParent: true,
});

const directTypes: [
  Assert<
    Equal<
      keyof typeof direct.tools,
      | "typed-background-child_start"
      | "typed-background-child_follow_up"
      | "typed-background-child_inspect"
    >
  >,
  Assert<Equal<Tool.Parameters<(typeof direct.tools)["typed-background-child_start"]>, string>>,
  Assert<
    Equal<
      Tool.Success<(typeof direct.tools)["typed-background-child_inspect"]>,
      Subagent.WorkerObservation<Subagent.SubagentResult<typeof text>>
    >
  >,
  Assert<Equal<Layer.Services<typeof direct.layer>, Encoder | Decoder>>,
  Assert<Equal<Layer.Error<typeof direct.layer>, never>>,
  Assert<
    Equal<
      Tool.Failure<(typeof direct.tools)["typed-background-child_start"]>,
      Subagent.SubagentProjectionFailure | Subagent.SubagentPrestartDenied | WorkerError
    >
  >,
] = [true, true, true, true, true, true];

const declaration = Subagent.make("research", {
  target,
  parameters: text,
  success: text,
  failure: DeclaredFailure,
  prepareInput: (input, context) => {
    if (context.source === "tool") {
      const toolId: string = context.toolCallId;
      const runId: string = context.parent.runId;

      void toolId;
      void runId;
    } else {
      // @ts-expect-error A programmatic source has no fabricated Tool Call.
      void context.toolCallId;
      // @ts-expect-error A programmatic source has no fabricated Run.
      void context.parent.runId;
    }

    return Effect.as(Prepare, input);
  },
  projectResult: (output) => Effect.as(Project, output),
});

const worker = Schema.decodeSync(Subagent.Worker(declaration))({
  schemaVersion: 1,
  delegationId: "research",
  targetAgentId: target.id,
  threadId: "worker",
});

const receipt = Schema.decodeSync(Receipt)({
  threadId: "worker",
  receiptId: "receipt",
  submissionId: "submission",
  queueSequence: 1,
});

const key = Schema.decodeSync(IdempotencyKey)("start-key");
const start = Subagent.start(declaration, "input", { idempotencyKey: key });
const followUp = Subagent.followUp(declaration, worker, "next", { idempotencyKey: key });
const inspect = Subagent.inspect(declaration, worker, receipt);
const workerSummary = Subagent.inspect(declaration, worker);
const awaitResult = Subagent.await(declaration, worker, receipt);
const cancel = Subagent.cancel(declaration, worker, receipt);
const listed = Subagent.list(declaration);
const selected = Subagent.background(declaration, { start: true, inspect: true, cancel: true });
const onlyList = Subagent.background(declaration, { list: true });
const onlyStart = Subagent.background(declaration, { start: true });
const onlyInspect = Subagent.background(declaration, { inspect: true });
const automatic = Subagent.background(declaration, { start: true, reportToParent: true });

const mapped = Subagent.background(declaration, {
  start: true,
  reportToParent: Subagent.reporting(declaration, {
    input: text,
    prepare: (report) =>
      Effect.as(Project, report.outcome === "completed" ? report.result : "failed"),
  }),
});

const automaticServices: Assert<
  Equal<Layer.Services<typeof automatic.layer>, Prepare | Project | Encoder | Decoder>
> = true;

const mappedServices: Assert<
  Equal<Layer.Services<typeof mapped.layer>, Prepare | Project | Encoder | Decoder>
> = true;

const automaticErrors: Assert<Equal<Layer.Error<typeof automatic.layer>, never>> = true;
const automaticKeys: Assert<Equal<keyof typeof automatic.tools, "research_start">> = true;
const onlySummary = Subagent.background(declaration, { summary: true });

type StartErrors =
  | DeclaredFailure
  | Subagent.SubagentProjectionFailure
  | Subagent.SubagentPrestartDenied
  | WorkerError;
type ProjectionErrors = DeclaredFailure | Subagent.SubagentProjectionFailure | WorkerError;

const nestedTarget = Agent.make("typed-nested-worker", {
  input: text,
  output: text,
  instructions: "Use the selected background operations.",
  toolkit: selected.toolkit,
});

const nested = Subagent.make("builder", {
  target: nestedTarget,
  grant: SubagentGrant.make({
    allowedToolNames: Object.keys(selected.tools),
    maxDepth: 2,
    childLifetimes: ["background"],
  }),
});

const nestedParameterProof: Assert<Equal<Tool.Parameters<typeof nested.tool>, string>> = true;

const nestedSuccessProof: Assert<
  Equal<
    Tool.Success<typeof nested.tool>,
    { readonly output: string; readonly budgetExhausted: boolean }
  >
> = true;

const proofs: [
  Assert<Equal<Effect.Services<typeof workerSummary>, SubagentHost>>,
  Assert<Equal<Effect.Error<typeof workerSummary>, WorkerError>>,
  Assert<Equal<Layer.Services<typeof onlySummary.layer>, never>>,
  Assert<Equal<Effect.Services<typeof start>, SubagentHost | Prepare | Encoder>>,
  Assert<Equal<Effect.Services<typeof followUp>, SubagentHost | Prepare | Encoder>>,
  Assert<Equal<Effect.Services<typeof inspect>, SubagentHost | Project | Encoder | Decoder>>,
  Assert<Equal<Effect.Services<typeof awaitResult>, SubagentHost | Project | Encoder | Decoder>>,
  Assert<Equal<Effect.Services<typeof cancel>, SubagentHost>>,
  Assert<Equal<Effect.Services<typeof listed>, SubagentHost>>,
  Assert<Equal<Effect.Error<typeof start>, StartErrors>>,
  Assert<Equal<Effect.Error<typeof followUp>, ProjectionErrors>>,
  Assert<Equal<Effect.Error<typeof inspect>, ProjectionErrors>>,
  Assert<Equal<Effect.Error<typeof cancel>, WorkerError | JoinedToHost>>,
  Assert<Equal<Effect.Success<typeof start>["worker"], Subagent.Worker<"research">>>,
  Assert<Equal<Effect.Success<typeof followUp>, Receipt>>,
  Assert<
    Equal<keyof typeof selected.tools, "research_start" | "research_inspect" | "research_cancel">
  >,
  Assert<Equal<Tool.Parameters<typeof selected.tools.research_start>, string>>,
  Assert<
    Equal<
      Tool.HandlerServices<typeof selected.tools.research_start>,
      SubagentHost | Crypto.Crypto | Decoder
    >
  >,
  Assert<Equal<Layer.Services<typeof onlyList.layer>, never>>,
  Assert<Equal<Layer.Services<typeof onlyStart.layer>, Prepare | Encoder>>,
  Assert<Equal<Layer.Services<typeof onlyInspect.layer>, Project | Encoder | Decoder>>,
] = [
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
  true,
];

const rejectInvalidCalls = () => {
  // @ts-expect-error Starts require an explicit idempotency key.
  void Subagent.start(declaration, "input");
  // @ts-expect-error Idempotency keys must be Schema-validated.
  void Subagent.start(declaration, "input", { idempotencyKey: "raw" });
  // @ts-expect-error Parameter inference is retained.
  void Subagent.start(declaration, 42, { idempotencyKey: key });
  const other = Subagent.make("other", { target });

  const otherWorker = Schema.decodeSync(Subagent.Worker(other))({
    ...worker,
    delegationId: "other",
  });

  // @ts-expect-error Worker identities retain their declaration correlation.
  void Subagent.cancel(declaration, otherWorker, receipt);
  // @ts-expect-error Unselected operations are not in the Tool record.
  void selected.tools.research_follow_up;
  // @ts-expect-error Direct definitions expose only selected operations.
  void direct.tools["typed-background-child_cancel"];

  // @ts-expect-error Direct input parameters retain the target schema.
  const wrongDirectInput: Tool.Parameters<(typeof direct.tools)["typed-background-child_start"]> =
    42;

  void wrongDirectInput;
  // @ts-expect-error Models cannot wait through a background tool.
  Subagent.background(declaration, { await: true });
};

describe("background authoring types", () => {
  it("preserves operation errors, schema services, and selected native Tool names", () => {
    expect(typeof updateLayerProofs).toBe("function");
    expect(proofs.every(Boolean)).toBe(true);
    expect(directTypes.every(Boolean)).toBe(true);
    expect(updateTypes.every(Boolean)).toBe(true);
    expect(automaticServices && mappedServices && automaticErrors && automaticKeys).toBe(true);
    expect(nestedParameterProof && nestedSuccessProof).toBe(true);
    expect(typeof rejectInvalidCalls).toBe("function");
  });
});
