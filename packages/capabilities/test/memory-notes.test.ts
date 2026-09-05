import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { MemoryAttribution } from "@effect-agent/core/MemoryReference";
import {
  applyMemoryWrite,
  type MemoryDocument,
  MemoryKey,
  MemoryOperationConflict,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
  MemoryWrite,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import { DurableStep, DurableStepError } from "@effect-agent/engine/DurableStep";
import { describe, expect, it } from "@effect/vitest";
import { Clock, Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import * as MemoryNotes from "../src/MemoryNotes.ts";

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
const makeMemory = Effect.fn("test.makeNotesMemory")(function* (loseFirstAcknowledgement = false) {
  const current = yield* Ref.make<MemoryDocument | null>(null);
  const commits = yield* Ref.make(0);
  const commands: Array<MemoryWrite> = [];

  const receipts = new Map<
    string,
    { readonly command: string; readonly document: MemoryDocument }
  >();

  let loseAck = loseFirstAcknowledgement;
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
  it.effect("rejects oversized notes without returning a partial document", () =>
    Effect.gen(function* () {
      const memory = yield* makeMemory();
      const text = "\u0000".repeat(6_000);

      yield* memory.writer.change(
        MemoryWrite.make({
          _tag: "Put",
          key,
          locator: options.locator,
          operationId: "host-write",
          expectedRevision: null,
          scopes: options.scopes,
          content: { text, attributions: options.attributions, metadata: {}, recordedAt: 0 },
        }),
      );

      const exercise = Effect.gen(function* () {
        const tools = yield* MemoryNotes.toolkit;

        const read = yield* tools
          .handle("read_notes", {}, "read")
          .pipe(Effect.flatMap(Stream.runCollect));

        expect(read).toMatchObject([
          { isFailure: true, result: { _tag: "MemoryNotesError", reason: "limit" } },
        ]);

        const write = yield* tools
          .handle("write_notes", { text: `${text}A`, expectedRevision: "1" }, "write")
          .pipe(Effect.flip, Effect.provideService(DurableStep, steps()));

        expect(write).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
      });

      yield* exercise.pipe(
        Effect.provide(
          MemoryNotes.layer(options).pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(MemoryReader, memory.reader),
                Layer.succeed(MemoryWriter, memory.writer),
              ),
            ),
          ),
        ),
      );
      expect(memory.commands).toHaveLength(1);
      expect(yield* Ref.get(memory.current)).toMatchObject({ content: { text } });
    }),
  );

  it.effect("uses the host document and preserves a competing revision", () =>
    Effect.gen(function* () {
      const memory = yield* makeMemory();

      const exercise = Effect.gen(function* () {
        const tools = yield* MemoryNotes.toolkit;

        const empty = yield* tools
          .handle("read_notes", {}, "read-empty")
          .pipe(Effect.flatMap(Stream.runCollect));

        expect(empty).toMatchObject([{ result: { revision: null, text: "" } }]);

        const untrustedWrite = {
          text: "Track the failing test.",
          expectedRevision: null,
          key: { id: "other" },
        };

        const saved = yield* tools
          .handle("write_notes", untrustedWrite, "save")
          .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(DurableStep, steps()));

        expect(saved).toMatchObject([
          { isFailure: false, result: { revision: "1", text: "Track the failing test." } },
        ]);

        const conflict = yield* tools
          .handle(
            "write_notes",
            { text: "Discard the previous work.", expectedRevision: null },
            "stale",
          )
          .pipe(Effect.flatMap(Stream.runCollect), Effect.provideService(DurableStep, steps()));

        expect(conflict).toMatchObject([
          { isFailure: true, result: { _tag: "MemoryConflict", actualRevision: "1" } },
        ]);
      });

      yield* exercise.pipe(
        Effect.provide(
          MemoryNotes.layer(options).pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(MemoryReader, memory.reader),
                Layer.succeed(MemoryWriter, memory.writer),
              ),
            ),
          ),
        ),
      );
      expect(memory.commands.map((command) => command.key)).toEqual([key, key]);
      expect(yield* Ref.get(memory.commits)).toBe(1);
      expect(yield* Ref.get(memory.current)).toMatchObject({
        content: { text: "Track the failing test." },
        source: { revision: "1" },
      });
    }),
  );

  it.effect(
    "replays the exact prepared write after a committed write loses its acknowledgement",
    () =>
      Effect.gen(function* () {
        const memory = yield* makeMemory(true);
        const savedSteps = new Map<string, unknown>();

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
                  Layer.succeed(MemoryReader, memory.reader),
                  Layer.succeed(MemoryWriter, memory.writer),
                ),
              ),
            ),
          ),
        );
        expect(memory.commands).toHaveLength(2);
        expect(memory.commands[1]).toEqual(memory.commands[0]);
        expect(yield* Ref.get(memory.commits)).toBe(1);
      }),
  );

  it.effect("interrupts a pending write and closes the writer's resources", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const released = yield* Ref.make(false);
      const memory = yield* makeMemory();

      const writer = MemoryWriter.fromAdapter({
        change: () =>
          Effect.scoped(
            Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
              Ref.set(released, true),
            ).pipe(Effect.andThen(Effect.never)),
          ),
      });

      const exercise = Effect.gen(function* () {
        const tools = yield* MemoryNotes.toolkit;

        const fiber = yield* tools
          .handle("write_notes", { text: "Pending note.", expectedRevision: null }, "save")
          .pipe(
            Effect.flatMap(Stream.runCollect),
            Effect.provideService(DurableStep, steps()),
            Effect.forkChild,
          );

        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);

        expect(yield* Ref.get(released)).toBe(true);
      });

      yield* exercise.pipe(
        Effect.provide(
          MemoryNotes.layer(options).pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(MemoryReader, memory.reader),
                Layer.succeed(MemoryWriter, writer),
              ),
            ),
          ),
        ),
      );
    }),
  );

  it.effect("preserves defects instead of returning them as note content", () =>
    Effect.gen(function* () {
      const memory = yield* makeMemory();

      const exercise = Effect.gen(function* () {
        const tools = yield* MemoryNotes.toolkit;

        const exit = yield* tools
          .handle("read_notes", {}, "read")
          .pipe(Effect.flatMap(Stream.runCollect), Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit))
          expect(exit.cause.reasons).toMatchObject([{ _tag: "Die", defect: "reader defect" }]);
      });

      yield* exercise.pipe(
        Effect.provide(
          MemoryNotes.layer(options).pipe(
            Layer.provide(
              Layer.mergeAll(
                Layer.succeed(
                  MemoryReader,
                  MemoryReader.fromAdapter({ get: () => Effect.die("reader defect") }),
                ),
                Layer.succeed(MemoryWriter, memory.writer),
              ),
            ),
          ),
        ),
      );
    }),
  );
});
