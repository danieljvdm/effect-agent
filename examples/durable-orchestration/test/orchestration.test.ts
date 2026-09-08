import { join } from "node:path";

import * as Messaging from "@effect-agent/capabilities/Messaging";
import { MessagingHost } from "@effect-agent/engine/MessagingHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeFileSystem } from "@effect/platform-node";
import { Effect, FileSystem, References, Schema, Stream } from "effect";
import { LanguageModel } from "effect/unstable/ai";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, it } from "vite-plus/test";

import {
  advisorPeer,
  advisorThread,
  coordinator,
  coordinatorPeer,
  principal,
  rootThread,
} from "../src/agents.ts";
import { snapshot, Snapshot } from "../src/application.ts";
import { nodeDemonstration, nodeHost } from "../src/node.ts";

const verify = (state: typeof Snapshot.Type) => {
  expect(state.depths).toEqual([0, 1, 2]);
  expect(state.workers).toHaveLength(2);
  expect(state.workers.every((worker) => worker.state === "idle")).toBe(true);
  expect(state.reports).toHaveLength(3);
  expect(state.recommendations).toEqual([
    { _tag: "Recommendation", text: "Ship the smallest verified step first." },
  ]);
  expect(state.runs).toBeGreaterThan(1);
  expect(state.inputs).toBe(state.settled);
  expect(state.outcomes.every((outcome) => outcome === "completed")).toBe(true);
};

it.each([false, true])(
  "follows up when Continue joins a recommendation Run (later notification: %s)",
  async (laterNotification) => {
    const recommendation = { _tag: "Recommendation", text: "Verify the restart path." };

    const worker = {
      schemaVersion: 1,
      delegationId: "build_a",
      targetAgentId: "demo-builder-A",
      threadId: "existing-builder-a",
    };

    const response = await Effect.runPromise(
      LanguageModel.streamText({
        prompt: [
          { role: "system", content: JSON.stringify(recommendation) },
          { role: "user", content: JSON.stringify({ _tag: "Launch", mission: "Build a feature" }) },
          {
            role: "tool",
            content: [
              {
                type: "tool-result",
                id: "build_a_start-call",
                name: "build_a_start",
                isFailure: false,
                result: {
                  worker,
                  receipt: {
                    threadId: worker.threadId,
                    receiptId: "first",
                    submissionId: "first",
                    queueSequence: 1,
                  },
                },
              },
              {
                type: "tool-result",
                id: "build_b_start-call",
                name: "build_b_start",
                isFailure: false,
                result: {},
              },
            ],
          },
          { role: "user", content: JSON.stringify(recommendation) },
          { role: "user", content: JSON.stringify({ _tag: "Continue", note: "Check recovery" }) },
          ...(laterNotification
            ? [{ role: "user" as const, content: JSON.stringify(recommendation) }]
            : []),
          { role: "user", content: "Current Run status: 1 turn used." },
        ],
        toolkit: coordinator.definition.toolkit,
        disableToolCallResolution: true,
      }).pipe(Stream.runCollect, Effect.provide(coordinator.model)),
    );

    expect(response.find((part) => part.type === "tool-call")).toMatchObject({
      name: "build_a_follow_up",
      params: { worker, parameters: { task: "Check recovery" } },
    });
  },
);

it("runs builders, scouts, later reports and native peer replies with a bounded Node pool", async () => {
  const state = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "orchestration-demo-" });

        return yield* Effect.gen(function* () {
          const state = yield* nodeDemonstration.pipe(
            Effect.onError(() =>
              Effect.gen(function* () {
                const state = yield* snapshot;
                const store = yield* ThreadStore;

                const threads = new Set([
                  rootThread,
                  advisorThread,
                  ...state.workers.map((value) => value.worker.threadId),
                ]);

                const records = [];

                for (const threadId of threads) {
                  const log = yield* store
                    .export(ThreadExportRequest.make({ threadId }))
                    .pipe(
                      Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(undefined)),
                    );

                  if (log === undefined) continue;
                  records.push({
                    threadId,
                    tail: log.records.slice(-8).map(({ sequence, record }) => ({
                      sequence,
                      type: record.payload._tag,
                      ...(record.payload._tag === "RunFailed"
                        ? { failure: record.payload.failure }
                        : {}),
                      ...(record.payload._tag === "ToolCallSettled" && record.payload.isFailure
                        ? { failure: record.payload.result }
                        : {}),
                    })),
                  });
                  for (const { record } of log.records) {
                    if (record.payload._tag === "SubagentRequested")
                      threads.add(record.payload.childThreadId);
                  }
                }
                yield* Effect.logError(JSON.stringify({ state, records }));
              }).pipe(Effect.ignore),
            ),
          );

          const runtime = yield* DurableAgentRuntime;

          const advisor = yield* runtime.messagingHost({
            sourceThreadId: advisorThread,
            principal,
          });

          const inbox = yield* Messaging.inbox(coordinatorPeer).pipe(
            Effect.provideService(MessagingHost, advisor),
          );

          const request = inbox.items[0]?.admission.message;

          if (request === undefined) return yield* Effect.die("Missing advisor request provenance");
          const source = yield* runtime.messagingHost({ sourceThreadId: rootThread, principal });

          const delivered = yield* Messaging.inspect(advisorPeer, request).pipe(
            Effect.provideService(MessagingHost, source),
          );

          // The inbox proves acceptance; the independent delivery observer may acknowledge
          // processing after the advisor has already sent its reply.
          expect(["accepted", "processed"]).toContain(delivered.status);
          expect(delivered.receipt).not.toBeNull();
          expect(
            yield* Messaging.retry(advisorPeer, request).pipe(
              Effect.provideService(MessagingHost, source),
              Effect.result,
            ),
          ).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });
          expect(
            yield* runtime
              .workerHost({ sourceThreadId: advisorThread, principal })
              .pipe(Effect.result),
          ).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });

          return state;
        }).pipe(Effect.provide(nodeHost(`${directory}/runtime.sqlite`)));
      }),
    ).pipe(
      Effect.provide(NodeFileSystem.layer),
      Effect.provideService(References.MinimumLogLevel, "Warn"),
    ),
  );

  verify(state);
}, 65_000);

it("runs the same declarations through the deployable workerd host", async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "../src/cloudflare.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (output === undefined) throw new Error("No worker bundle");

  const runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: output.text,
      modulesRoot: "/",
      compatibilityDate: "2025-05-01",
      compatibilityFlags: ["nodejs_compat"],
      bindings: { DEMO_TOKEN: "test-token" },
      durableObjects: { THREADS: { className: "OrchestrationThread", useSQLite: true } },
    }),
  );

  const headers = { authorization: "Bearer test-token", "content-type": "application/json" };

  const command = async (body: unknown) => {
    const response = await runtime.dispatchFetch("http://demo/command", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });

    expect({ status: response.status, body: await response.text() }).toMatchObject({ status: 202 });
  };

  const until = async (predicate: (state: typeof Snapshot.Type) => boolean) => {
    let latest: typeof Snapshot.Type | undefined;

    for (let attempt = 0; attempt < 600; attempt++) {
      const response = await runtime.dispatchFetch("http://demo/status", { headers });

      expect(response.status).toBe(200);
      const state = Schema.decodeUnknownSync(Snapshot)(await response.json());

      latest = state;
      if (predicate(state)) return state;
      await Effect.runPromise(Effect.sleep("25 millis"));
    }
    throw new Error(`The durable example did not converge: ${JSON.stringify(latest)}`);
  };

  try {
    expect((await runtime.dispatchFetch("http://demo/status")).status).toBe(401);
    await command({
      action: "launch",
      key: "launch-v1",
      mission: "Design a small durable feature",
    });
    await until((state) => state.settled >= 1);
    await command({
      action: "recommend",
      key: "recommend-v1",
      question: "What should the builders prioritize?",
    });

    const before = await until(
      (state) =>
        state.reports.length === 2 && state.workers.every((worker) => worker.state === "idle"),
    );

    await command({ action: "continue", key: "continue-v1", note: "Also verify the restart path" });

    const state = await until(
      (state) =>
        state.reports.length === 3 &&
        state.recommendations.length === 1 &&
        state.inputs === state.settled &&
        state.workers.every((worker) => worker.state === "idle"),
    );

    verify(state);
    expect(state.workers.map((worker) => worker.worker.threadId)).toEqual(
      before.workers.map((worker) => worker.worker.threadId),
    );
    await command({ action: "continue", key: "continue-v1", note: "Also verify the restart path" });
    const repeated = await until((value) => value.inputs === value.settled);

    expect(repeated.reports).toEqual(state.reports);
  } finally {
    await runtime.dispose();
  }
}, 65_000);
