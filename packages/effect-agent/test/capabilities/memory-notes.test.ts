import { describe, expect, it } from "@effect/vitest";
import { Clock, Effect, Layer, Ref, Schema, Stream } from "effect";
import { DurableStep, DurableStepError } from "effect-agent/durable-step";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import { MemoryAttribution } from "effect-agent/memory-reference";
import {
  type MemoryWrite,
  applyMemoryWrite,
  type MemoryDocument,
  MemoryKey,
  MemoryOperationConflict,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
  MemoryWriter,
} from "effect-agent/memory-store";
import { TestClock } from "effect/testing";
import { IdGenerator } from "effect/unstable/ai";

import * as MemoryNotes from "../../src/capabilities/MemoryNotes.ts";

const NotesNamespace = MemoryNamespace.define({
  name: "test/notes",
  version: 1,
  identity: Schema.String,
});

const key = MemoryKey.make({ namespace: NotesNamespace.make("host-owned"), id: "working-notes" });

const options: MemoryNotes.Options = {
  key,
  locator: "notes://host-owned/working-notes",
  attributions: [
    MemoryAttribution.make({
      originId: "agent-working-notes",
      speaker: "agent",
      observers: [],
      locator: "notes://host-owned/working-notes",
      activityAt: null,
      interpretation: "model-authored working notes",
    }),
  ],
  scopes: [MemoryScope.make("host-owned")],
};

/** Local step journal for replaying this one Tool Call through its public DurableStep seam. */
const steps = (saved = new Map<string, unknown>()): DurableStep["Service"] => ({
  do: (name, output, execute) =>
    Effect.gen(function* () {
      if (saved.has(name)) {
        return yield* Schema.decodeUnknownEffect(output)(saved.get(name)).pipe(
          Effect.mapError(() =>
            DurableStepError.make({
              stepName: name,
              reason: "recorded-result-invalid",
              message: "Invalid saved step",
            }),
          ),
        );
      }
      const result = yield* execute;

      const encoded = yield* Schema.encodeEffect(output)(result).pipe(
        Effect.mapError(() =>
          DurableStepError.make({
            stepName: name,
            reason: "output-encoding-failed",
            message: "Could not encode step",
          }),
        ),
      );

      saved.set(name, encoded);

      return result;
    }),
});

/** The scenario uses the core transition and receipt-first reconciliation, then loses one ack. */
const makeMemory = Effect.fn("test.makeNotesMemory")(function* () {
  const current = yield* Ref.make<MemoryDocument | null>(null);
  const commits = yield* Ref.make(0);
  const commands: Array<MemoryWrite> = [];

  const receipts = new Map<
    string,
    { readonly command: string; readonly document: MemoryDocument }
  >();

  let loseAck = true;
  const reader = MemoryReader.fromAdapter({ get: () => Ref.get(current) });

  const writer = MemoryWriter.fromAdapter({
    change: Effect.fn("test.changeNotes")(function* (write) {
      commands.push(write);

      const receiptKey = JSON.stringify([
        write.key.namespace.address,
        write.key.id,
        write.operationId,
      ]);

      const receipt = receipts.get(receiptKey);
      const command = JSON.stringify(write);

      if (receipt !== undefined) {
        if (receipt.command !== command) {
          return yield* MemoryOperationConflict.make({
            key: write.key,
            operationId: write.operationId,
          });
        }

        return receipt.document;
      }

      const document = yield* applyMemoryWrite(
        yield* Ref.get(current),
        write,
        yield* Clock.currentTimeMillis,
      );

      yield* Ref.set(current, document);
      receipts.set(receiptKey, { command, document });
      yield* Ref.update(commits, (count) => count + 1);
      if (loseAck) {
        loseAck = false;

        return yield* MemoryStorageError.make({
          operation: "acknowledge write",
          reason: "unavailable",
        });
      }

      return document;
    }),
  });

  return { current, commands, commits, reader, writer };
});

describe("durable working notes", () => {
  it.effect(
    "replays the exact prepared write after a committed write loses its acknowledgement",
    () =>
      Effect.gen(function* () {
        const memory = yield* makeMemory();
        const savedSteps = new Map<string, unknown>();
        const generatedIds = yield* Ref.make(0);

        const exercise = Effect.gen(function* () {
          const tools = yield* MemoryNotes.toolkit;

          const request = {
            text: "Keep the original operation and timestamp.",
            expectedRevision: null,
          };

          const first = yield* tools
            .handle("write_notes", request, "save")
            .pipe(
              Effect.flatMap(Stream.runCollect),
              Effect.provideService(DurableStep, steps(savedSteps)),
            );

          expect(first).toMatchObject([
            { isFailure: true, result: { _tag: "MemoryStorageError", reason: "unavailable" } },
          ]);
          yield* TestClock.adjust(5_000);

          const recovered = yield* tools
            .handle("write_notes", request, "save")
            .pipe(
              Effect.flatMap(Stream.runCollect),
              Effect.provideService(DurableStep, steps(savedSteps)),
            );

          expect(recovered).toMatchObject([
            { isFailure: false, result: { revision: "1", text: request.text } },
          ]);

          const repeated = yield* tools
            .handle("write_notes", request, "save")
            .pipe(
              Effect.flatMap(Stream.runCollect),
              Effect.provideService(DurableStep, steps(savedSteps)),
            );

          expect(repeated).toEqual(recovered);
        });

        yield* exercise.pipe(
          Effect.provide(
            MemoryNotes.layer(options).pipe(
              Layer.provide(
                Layer.mergeAll(
                  Layer.succeed(IdGenerator.IdGenerator, {
                    generateId: () =>
                      Ref.updateAndGet(generatedIds, (count) => count + 1).pipe(
                        Effect.map((count) => `notes-operation:${count}`),
                      ),
                  }),
                  Layer.succeed(MemoryReader, memory.reader),
                  Layer.succeed(MemoryWriter, memory.writer),
                ),
              ),
            ),
          ),
        );
        expect(memory.commands).toHaveLength(2);
        expect(memory.commands[0]?.operationId).toBe("notes-operation:1");
        expect(memory.commands[1]).toEqual(memory.commands[0]);
        expect(yield* Ref.get(generatedIds)).toBe(1);
        expect(yield* Ref.get(memory.commits)).toBe(1);
      }),
  );
});
