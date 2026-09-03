import { ToolCallId } from "@effect-agent/core";
import {
  DurableAgentRuntime,
  ResolutionCompletedWithResult,
  UnknownResolutionCommand,
} from "@effect-agent/thread";
import { WorkflowDurableHost } from "@effect-agent/workflow";
import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Fiber, FileSystem, Layer, Option, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

import {
  hostLayer,
  pendingIntents,
  readLog,
  submitOptions,
  temporaryDirectory,
  until,
} from "../workflow-fixtures.ts";
import { makeCrashFixture, WorkflowCrashBoundary, WorkflowCrashMarker } from "./workflow-worker.ts";

it.live.each(WorkflowCrashBoundary.literals)(
  "recovers Workflow boundary %s after SIGKILL without inventing or replaying external effects",
  (boundary) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fs = yield* FileSystem.FileSystem;

      const cwd = (yield* fs.exists("test/crash/workflow-worker-entry.ts"))
        ? "."
        : "packages/platform-node";

      const child = yield* ChildProcess.make(
        "vp",
        ["exec", "node", "--experimental-transform-types", "test/crash/workflow-worker-entry.ts"],
        {
          cwd,
          env: { EFFECT_AGENT_WORKFLOW_DIR: directory, EFFECT_AGENT_WORKFLOW_BOUNDARY: boundary },
          extendEnv: true,
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        },
      );

      const stderr = yield* Stream.mkString(Stream.decodeText(child.stderr)).pipe(
        Effect.forkScoped,
      );

      const marker = yield* child.stdout.pipe(
        Stream.decodeText(),
        Stream.splitLines,
        Stream.map((line) =>
          Schema.decodeUnknownOption(Schema.fromJsonString(WorkflowCrashMarker))(line),
        ),
        Stream.filter(Option.isSome),
        Stream.map((value) => value.value),
        Stream.runHead,
        Effect.timeout("15 seconds"),
      );

      if (Option.isNone(marker)) {
        const exit = yield* child.exitCode.pipe(Effect.result);
        const stderrText = yield* Fiber.join(stderr);

        return yield* Effect.die(
          new Error(
            `Workflow worker exited before ${boundary}; exit=${JSON.stringify(exit)}; stderr=${stderrText}`,
          ),
        );
      }
      expect(marker.value.boundary).toBe(boundary);
      yield* child.kill({ killSignal: "SIGKILL" });
      yield* child.exitCode.pipe(Effect.result);
      yield* Fiber.join(stderr);
      const ordinary = boundary === "ordinary:external-effect";
      const fixture = yield* makeCrashFixture(directory, ordinary);

      yield* Effect.gen(function* () {
        const host = yield* WorkflowDurableHost;
        const runtime = yield* DurableAgentRuntime;

        const receipt = yield* runtime.submit(
          fixture.agent,
          { question: "survive SIGKILL" },
          submitOptions(fixture.digests),
        );

        if (ordinary) {
          const unknown = yield* until(readLog(receipt.threadId), (rows) =>
            rows.some((row) => row.record.payload._tag === "ToolCallUnknown"),
          );

          expect(
            unknown.filter((row) => row.record.payload._tag === "ToolCallSettled"),
          ).toHaveLength(0);
          expect(yield* fs.readFileString(`${directory}/bookings`)).toBe("confirmed-reservation\n");
          expect((yield* host.submissionStatus(receipt))._tag).toBe("pending");
          yield* host.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: Schema.decodeSync(ToolCallId)("book-1"),
              author: "supplier-operator",
              reason: "supplier confirms persisted reservation",
              resolution: ResolutionCompletedWithResult.make({
                result: { confirmation: "confirmed-reservation" },
                isFailure: false,
              }),
            }),
          );
        }
        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        const log = yield* readLog(receipt.threadId);

        expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(
          1,
        );
        const ids = log.map((row) => row.record.recordId);

        expect(new Set(ids).size).toBe(ids.length);
        if (ordinary) {
          expect(yield* fs.readFileString(`${directory}/bookings`)).toBe("confirmed-reservation\n");
        } else if (
          boundary === "terminalize:after-canonical-append" ||
          boundary === "cleanup:before" ||
          boundary === "cleanup:after"
        ) {
          expect(yield* fs.readFileString(`${directory}/model-calls`)).toBe("called\n");
        }
      }).pipe(
        Effect.provide(
          hostLayer(directory, [{ agent: fixture.agent, definitions: fixture.definitions }]).pipe(
            Layer.provide(fixture.handlers),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  30000,
);
