import * as Agent from "@effect-agent/core/Agent";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import { WorkflowDispatchFailpoint } from "@effect-agent/workflow/WorkflowDispatch";
import { Config, Console, Effect, FileSystem, Layer, Schema, Stream } from "effect";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  planner,
  submitOptions,
  usage,
} from "../workflow-fixtures.ts";

export const WorkflowCrashBoundary = Schema.Literals([
  "intent:before-persist",
  "intent:after-persist",
  "launch:after",
  "terminalize:after-canonical-append",
  "cleanup:before",
  "cleanup:after",
  "ordinary:external-effect",
]);

export type WorkflowCrashBoundary = typeof WorkflowCrashBoundary.Type;

export const WorkflowCrashMarker = Schema.TaggedStruct("WorkflowCrashMarker", {
  boundary: WorkflowCrashBoundary,
});

const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});

const tools = Toolkit.make(Book);

export const bookingDefinition = Agent.make("workflow-crash-booking", {
  input: planner.input,
  output: planner.output,
  instructions: "Book before answering as JSON.",
  toolkit: tools,
  policy: planner.policy,
});

const bookParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: "book-1",
    name: "book",
    params: { ref: "reservation" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

export const makeCrashFixture = Effect.fn("WorkflowCrash.makeFixture")(function* (
  directory: string,
  ordinary: boolean,
  block: Effect.Effect<void> = Effect.void,
  fresh = false,
) {
  const fs = yield* FileSystem.FileSystem;
  const definition = ordinary ? bookingDefinition : planner;

  const scripted = yield* makeModel((call) =>
    Stream.unwrap(
      fs
        .writeFileString(`${directory}/model-calls`, "called\n", { flag: "a" })
        .pipe(
          Effect.orDie,
          Effect.as(
            Stream.fromIterable(ordinary && fresh && call === 0 ? bookParts : finalParts()),
          ),
        ),
    ),
  );

  const agent = Agent.withModel(definition, scripted.model);
  const definitions = definitionsFor(definition.id);
  const digests = yield* digestDefinitions(definitions);

  const handlers = tools.toLayer({
    book: () =>
      fs
        .writeFileString(`${directory}/bookings`, "confirmed-reservation\n", { flag: "a" })
        .pipe(
          Effect.orDie,
          Effect.andThen(block),
          Effect.as({ confirmation: "confirmed-reservation" }),
        ),
  });

  return { agent, definitions, digests, handlers };
});

/** The parent kills this process only after observing its exact durable-boundary marker. */
export const workflowCrashWorker = Effect.gen(function* () {
  const directory = yield* Config.string("EFFECT_AGENT_WORKFLOW_DIR");

  const boundary = yield* Config.string("EFFECT_AGENT_WORKFLOW_BOUNDARY").pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(WorkflowCrashBoundary)),
  );

  const marker = yield* Schema.encodeEffect(Schema.fromJsonString(WorkflowCrashMarker))({
    _tag: "WorkflowCrashMarker",
    boundary,
  });

  const block = Console.log(marker).pipe(Effect.andThen(Effect.never));

  const fixture = yield* makeCrashFixture(
    directory,
    boundary === "ordinary:external-effect",
    block,
    true,
  );

  const stack = hostLayer(directory, [{ agent: fixture.agent, definitions: fixture.definitions }], {
    runtimeFailpoint: (point) => (point === boundary ? block : Effect.void),
  }).pipe(Layer.provide(fixture.handlers));

  return yield* Effect.gen(function* () {
    const host = yield* WorkflowAgentHost;

    yield* host.submit(
      fixture.agent,
      { question: "survive SIGKILL" },
      submitOptions(fixture.digests),
    );

    return yield* Effect.never;
  }).pipe(
    Effect.provide(stack),
    Effect.provideService(WorkflowDispatchFailpoint, {
      hit: (point) => (point === boundary ? block : Effect.void),
    }),
  );
});
