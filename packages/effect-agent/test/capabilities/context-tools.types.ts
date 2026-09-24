import { Effect, type Layer, Schema } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import type { ContextHistory, ContextHistoryError } from "effect-agent/context-history";
import type { ContextWindow } from "effect-agent/context-window";
import type { DurableStep, DurableStepError } from "effect-agent/durable-step";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import type {
  MemoryConflict,
  MemoryReader,
  MemoryStorageError,
  MemoryWriter,
} from "effect-agent/memory-store";
import { MemoryKey, MemoryScope } from "effect-agent/memory-store";
import type { ThreadHistory } from "effect-agent/thread-history";
import type {
  IdGenerator as EffectAiIdGenerator,
  LanguageModel,
  Model,
  Tool,
} from "effect/unstable/ai";
import type { expectTypeOf as ExpectTypeOf } from "vite-plus/test";

import * as ContextTools from "../../src/capabilities/ContextTools.ts";
import * as MemoryNotes from "../../src/capabilities/MemoryNotes.ts";

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
  | ThreadHistory;

export const verifyKeepsHistoryAndStorageDependenciesVisibleWhileTheEngineOwnsItsLocalServices =
  () => {
    expectTypeOf<Tool.HandlerServices<typeof ContextTools.SearchContextWindows>>().toEqualTypeOf<
      ContextWindow | ContextHistory
    >();
    expectTypeOf<
      Tool.Failure<typeof ContextTools.SearchContextWindows>
    >().toEqualTypeOf<ContextHistoryError>();
    expectTypeOf<
      Tool.HandlerError<typeof ContextTools.SearchContextWindows>
    >().toEqualTypeOf<never>();
    expectTypeOf<
      Tool.HandlerServices<typeof MemoryNotes.WriteNotes>
    >().toEqualTypeOf<DurableStep>();
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
  };

declare const expectTypeOf: typeof ExpectTypeOf;
