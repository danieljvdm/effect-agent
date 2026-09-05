import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import type {
  MemoryConflict,
  MemoryReader,
  MemoryStorageError,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import { MemoryKey, MemoryScope } from "@effect-agent/core/MemoryStore";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import type { ContextHistory, ContextHistoryError } from "@effect-agent/engine/ContextHistory";
import type { ContextWindow } from "@effect-agent/engine/ContextWindow";
import type { DurableStep, DurableStepError } from "@effect-agent/engine/DurableStep";
import type { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { Effect, type Layer, Schema } from "effect";
import type {
  IdGenerator as EffectAiIdGenerator,
  LanguageModel,
  Model,
  Tool,
} from "effect/unstable/ai";
import { expectTypeOf, it } from "vite-plus/test";

import * as ContextTools from "../src/ContextTools.ts";
import * as MemoryNotes from "../src/MemoryNotes.ts";

const policy = AgentPolicy.make({
  maxTurns: 4,
  maxToolCalls: 4,
  maxDuration: "1 minute",
  toolConcurrency: 1,
});

const contextAgent = Agent.make("context-tools-types", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Use historical evidence when needed.",
  toolkit: ContextTools.toolkit,
  policy,
});

const notesAgent = Agent.make("notes-tools-types", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Preserve working notes.",
  toolkit: MemoryNotes.toolkit,
  policy,
});

const NotesNamespace = MemoryNamespace.define({
  name: "types/notes",
  version: 1,
  identity: Schema.String,
});

const notesLayer = MemoryNotes.layer({
  key: MemoryKey.make({ namespace: NotesNamespace.make("host"), id: "notes" }),
  locator: "notes://host/notes",
  attributions: [
    {
      originId: "notes",
      speaker: "agent",
      observers: [],
      locator: "notes://host/notes",
      activityAt: null,
      interpretation: "working notes",
    },
  ],
  scopes: [MemoryScope.make("host")],
});

type NativeRuntimeServices =
  | LanguageModel.LanguageModel
  | Model.ProviderName
  | Model.ModelName
  | IdGenerator
  | ThreadHistory;

it("keeps history and storage dependencies visible while the engine owns its local services", () => {
  expectTypeOf<Tool.HandlerServices<typeof ContextTools.SearchContextWindows>>().toEqualTypeOf<
    ContextWindow | ContextHistory
  >();
  expectTypeOf<
    Tool.Failure<typeof ContextTools.SearchContextWindows>
  >().toEqualTypeOf<ContextHistoryError>();
  expectTypeOf<
    Tool.HandlerError<typeof ContextTools.SearchContextWindows>
  >().toEqualTypeOf<never>();
  expectTypeOf<Tool.HandlerServices<typeof MemoryNotes.WriteNotes>>().toEqualTypeOf<DurableStep>();
  expectTypeOf<Layer.Services<typeof notesLayer>>().toEqualTypeOf<
    MemoryReader | MemoryWriter | EffectAiIdGenerator.IdGenerator
  >();

  const contextRun = AgentRuntime.run(contextAgent, "continue").pipe(
    Effect.provide(ContextTools.layer),
  );

  expectTypeOf<Effect.Services<typeof contextRun>>().toEqualTypeOf<
    NativeRuntimeServices | ContextHistory
  >();
  expectTypeOf<
    Extract<Effect.Error<typeof contextRun>, ContextHistoryError>
  >().toEqualTypeOf<never>();

  const notesRun = AgentRuntime.run(notesAgent, "continue").pipe(Effect.provide(notesLayer));

  expectTypeOf<Effect.Services<typeof notesRun>>().toEqualTypeOf<
    NativeRuntimeServices | MemoryReader | MemoryWriter | EffectAiIdGenerator.IdGenerator
  >();
  expectTypeOf<
    Extract<Effect.Error<typeof notesRun>, MemoryConflict | MemoryStorageError | DurableStepError>
  >().toEqualTypeOf<never>();
});
