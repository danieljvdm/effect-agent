import { ThreadId } from "@effect-agent/core/Identifiers";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { ThreadExport, ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkflowEntrypoint } from "cloudflare:workers";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import {
  AiError,
  LanguageModel,
  Model,
  type Prompt,
  type Response as AiResponse,
} from "effect/unstable/ai";

import {
  AppCommit,
  AppFile,
  PlannerError,
  PlannerInput,
  TripApp,
  TripSiteStore,
} from "../../src/domain.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import { AppEditorBackground, EditorInput } from "../../src/trip-app/editor.ts";
import { AppSourceStore } from "../../src/trip-app/source.ts";
import { AppTools } from "../../src/trip-app/tools.ts";
import { FixtureBrowserLive } from "./browser.ts";
import fixtureWorker from "./worker.ts";

const SourceFiles = Schema.Array(AppFile);

const sourceError = () =>
  new PlannerError({ code: "storage", message: "Fixture source is unavailable." });

const storage = <A>(run: () => Promise<A>) => Effect.tryPromise({ try: run, catch: sourceError });

const hash = (value: string) =>
  Effect.promise(async () =>
    Array.from(
      new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value))),
      (byte) => byte.toString(16).padStart(2, "0"),
    )
      .join("")
      .slice(0, 40),
  );

/** Only Git transport is replaced; source trees and CAS heads survive the whole runtime restart. */
const FixtureSource = Layer.effect(
  AppSourceStore,
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    const bucket = env.APP_BUILDS;

    if (!bucket) return yield* Effect.die("Fixture needs APP_BUILDS");

    const read = Effect.fn("FixtureSource.read")(function* (input: {
      repoName: string;
      commitId: string;
    }) {
      const object = yield* storage(() =>
        bucket.get(`fixture-source/${input.repoName}/${input.commitId}.json`),
      );

      if (object === null) return yield* sourceError();

      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SourceFiles))(
        yield* storage(() => object.text()),
      ).pipe(Effect.mapError(sourceError));
    });

    const writeTree = Effect.fn("FixtureSource.writeTree")(function* (
      repoName: string,
      parent: string,
      files: ReadonlyArray<AppFile>,
    ) {
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(SourceFiles))(files).pipe(
        Effect.mapError(sourceError),
      );

      const commitId = yield* hash(`${parent}:${encoded}`);

      yield* storage(() =>
        bucket.put(`fixture-source/${repoName}/${commitId}.json`, encoded, {
          onlyIf: { etagDoesNotMatch: "*" },
        }),
      );

      return commitId;
    });

    return AppSourceStore.of({
      read,
      fork: Effect.fn("FixtureSource.fork")(function* ({ repoName, files }) {
        const key = `fixture-source/${repoName}/head`;
        const existing = yield* storage(() => bucket.get(key));

        if (existing !== null)
          return {
            commitId: yield* Schema.decodeUnknownEffect(AppCommit)(
              yield* storage(() => existing.text()),
            ).pipe(Effect.mapError(sourceError)),
          };
        const commitId = yield* writeTree(repoName, "root", files);

        yield* storage(() => bucket.put(key, commitId, { onlyIf: { etagDoesNotMatch: "*" } }));
        const stored = yield* storage(() => bucket.get(key));

        if (stored === null) return yield* sourceError();

        return {
          commitId: yield* Schema.decodeUnknownEffect(AppCommit)(
            yield* storage(() => stored.text()),
          ).pipe(Effect.mapError(sourceError)),
        };
      }),
      commit: Effect.fn("FixtureSource.commit")(function* ({ repoName, parentCommit, files }) {
        const key = `fixture-source/${repoName}/head`;
        const existing = yield* storage(() => bucket.get(key));

        if (existing === null || (yield* storage(() => existing.text())) !== parentCommit)
          return yield* new PlannerError({ code: "conflict", message: "Fixture source changed." });
        const current = yield* read({ repoName, commitId: parentCommit });

        if (JSON.stringify(current) === JSON.stringify(files)) return { commitId: parentCommit };
        const commitId = yield* writeTree(repoName, parentCommit, files);

        const saved = yield* storage(() =>
          bucket.put(key, commitId, { onlyIf: { etagMatches: existing.etag } }),
        );

        if (saved === null)
          return yield* new PlannerError({ code: "conflict", message: "Fixture source changed." });

        return { commitId };
      }),
    });
  }),
);

const usage = { inputTokens: {}, outputTokens: {} };

const call = (name: string, params: Schema.Json): ReadonlyArray<AiResponse.StreamPartEncoded> => [
  { type: "tool-call", id: `${name}-call`, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

const finish = (message: string) => call("deliver_response", { message, content: null });

const toolResults = (prompt: Prompt.Prompt, after = -1) =>
  prompt.content
    .slice(after + 1)
    .flatMap((message) =>
      message.role === "tool" ? message.content.filter((part) => part.type === "tool-result") : [],
    );

const inputs = <A, I>(prompt: Prompt.Prompt, schema: Schema.Codec<A, I>) =>
  prompt.content.flatMap((message, index) =>
    message.role === "user"
      ? message.content.flatMap((part) => {
          if (part.type !== "text") return [];
          const decoded = Schema.decodeUnknownOption(Schema.fromJsonString(schema))(part.text);

          return Option.isSome(decoded) ? [{ index, input: decoded.value }] : [];
        })
      : [],
  );

const gate = Effect.fn("FixtureEditor.gate")(function* (bucket: R2Bucket, key: string) {
  yield* Effect.promise(() => bucket.put(`fixture-gates/${key}/entered`, "yes"));
  while ((yield* Effect.promise(() => bucket.head(`fixture-gates/${key}/open`))) === null)
    yield* Effect.sleep("25 millis");
});

/** The script chooses native tools; it does not invoke application mutations itself. */
const FixtureEditorModel = Model.make(
  "fixture",
  "durable-editor-v1",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (options) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const service = yield* Effect.serviceOption(WorkerEnvironment);

            if (Option.isNone(service) || !service.value.APP_BUILDS)
              return yield* Effect.die("Fixture model needs storage");
            const bucket = service.value.APP_BUILDS;
            const editor = inputs(options.prompt, EditorInput).at(-1);
            const parent = inputs(options.prompt, PlannerInput).at(-1);
            const role = editor ? "editor" : "planner";
            const identity = yield* Effect.serviceOption(ThreadObjectIdentity);

            if (Option.isNone(identity))
              return yield* Effect.die("Fixture model needs its actual thread");
            yield* Effect.promise(() =>
              bucket.put(
                `fixture-tools/${identity.value.threadId}/${role}.json`,
                JSON.stringify(options.tools.map((tool) => tool.name)),
              ),
            );
            if (editor) {
              const { input, index } = editor;

              if (input.message.includes("fail-editor"))
                return yield* AiError.AiError.make({
                  module: "FixtureEditor",
                  method: "streamText",
                  reason: new AiError.InvalidRequestError({
                    description: "Fixture editor failure",
                  }),
                });
              const gateName = /hold:([a-z0-9-]+)/.exec(input.message)?.[1];

              if (gateName) yield* gate(bucket, `child-${gateName}`);
              const results = toolResults(options.prompt, index);
              const rejectedTrip = /reject:([a-zA-Z0-9-]+)/.exec(input.message)?.[1];

              if (rejectedTrip && !results.some((result) => result.name === "edit_trip_app"))
                return Stream.fromIterable(
                  call("edit_trip_app", {
                    tripId: rejectedTrip,
                    expectedCommit: "0".repeat(40),
                    files: [
                      { path: "forbidden.txt", content: "Must never reach another owner's source" },
                    ],
                    deletePaths: [],
                    label: "Attempt a cross-trip edit",
                  }),
                );
              const created = results.find((result) => result.name === "create_trip_app");

              if (!created)
                return Stream.fromIterable(call("create_trip_app", { tripId: input.tripId }));

              const app = yield* Schema.decodeUnknownEffect(TripApp)(created.result).pipe(
                Effect.orDie,
              );

              const read = results.find((result) => result.name === "read_trip_app_files");

              if (!read)
                return Stream.fromIterable(
                  call("read_trip_app_files", { tripId: input.tripId, paths: ["README.md"] }),
                );

              const source = yield* Schema.decodeUnknownEffect(
                AppTools.tools.read_trip_app_files.successSchema,
              )(read.result).pipe(Effect.orDie);

              if (!results.some((result) => result.name === "edit_trip_app"))
                return Stream.fromIterable(
                  call("edit_trip_app", {
                    tripId: app.tripId,
                    expectedCommit: source.commitId,
                    files: [{ path: "editor-change.txt", content: input.message }],
                    deletePaths: [],
                    label: "Fixture editor change",
                  }),
                );

              return Stream.fromIterable(finish(`Editor saved: ${input.message}`));
            }
            if (!parent) return Stream.fromIterable(finish("Ready"));
            const { input, index } = parent;
            const results = toolResults(options.prompt, index);

            if (input.message.startsWith("start-editor ")) {
              if (!results.some((result) => result.name === "app_editor_start"))
                return Stream.fromIterable(
                  call("app_editor_start", {
                    tripId: input.selectedTripId,
                    message: input.message.slice("start-editor ".length),
                  }),
                );
              const gateName = /parent:([a-z0-9-]+)/.exec(input.message)?.[1];

              if (gateName) yield* gate(bucket, `parent-${gateName}`);

              return Stream.fromIterable(
                finish("The app editor is working; the planner is available."),
              );
            }
            if (input.message.startsWith("follow-editor ")) {
              if (!results.some((result) => result.name === "app_editor_follow_up")) {
                const started = toolResults(options.prompt).find(
                  (result) => result.name === "app_editor_start" && !result.isFailure,
                );

                if (!started) return yield* Effect.die("Fixture needs an existing editor");

                const result = yield* Schema.decodeUnknownEffect(
                  AppEditorBackground.tools.app_editor_start.successSchema,
                )(started.result).pipe(Effect.orDie);

                return Stream.fromIterable(
                  call("app_editor_follow_up", {
                    worker: Schema.encodeSync(
                      AppEditorBackground.tools.app_editor_follow_up.parametersSchema.fields.worker,
                    )(result.worker),
                    parameters: {
                      tripId: input.selectedTripId,
                      message: input.message.slice("follow-editor ".length),
                    },
                  }),
                );
              }

              return Stream.fromIterable(finish("The follow-up is with the same editor."));
            }

            return Stream.fromIterable(finish(`Planner handled: ${input.message}`));
          }),
        ),
    }),
  ),
);

const sites = Layer.succeed(TripSiteStore, {
  publish: () =>
    Effect.fail(new PlannerError({ code: "publication", message: "Unused fixture publication." })),
  load: () => Effect.succeed(null),
});

export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(
    FixtureEditorModel,
    "durable-editor-v1",
    "Editor fixture",
    FixtureBrowserLive,
    undefined,
    FixtureSource,
  ),
) {
  fetch(_request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const store = yield* ThreadStore;
        const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(identity.threadId);

        return new Response(
          yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(
            yield* store.export(ThreadExportRequest.make({ threadId })),
          ),
          { headers: { "content-type": "application/json" } },
        );
      }),
    );
  }
}

/** Builds have their own suite; this fixture only acknowledges Workflow admission. */
export class FixtureBuild extends WorkflowEntrypoint {
  async run() {
    return { accepted: true };
  }
}

export default {
  async fetch(request: Request, env: Cloudflare.Env & { readonly PLANNER_TOKEN?: string }) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/__editor/")) {
      if (
        !env.PLANNER_TOKEN ||
        request.headers.get("authorization") !== `Bearer ${env.PLANNER_TOKEN}`
      )
        return new Response("Unauthorized", { status: 401 });
      const bucket = env.APP_BUILDS;

      if (!bucket) return new Response("Missing fixture bucket", { status: 500 });
      if (url.pathname === "/__editor/gate") {
        const key = `fixture-gates/${url.searchParams.get("name") ?? ""}`;

        if (request.method === "POST") await bucket.put(`${key}/open`, "yes");

        return Response.json({ entered: (await bucket.head(`${key}/entered`)) !== null });
      }
      if (url.pathname === "/__editor/journal") {
        const response = await env.THREADS.getByName(url.searchParams.get("thread") ?? "").fetch(
          request,
        );

        return new Response(await response.arrayBuffer(), response);
      }
      if (url.pathname === "/__editor/object") {
        const object = await bucket.get(url.searchParams.get("key") ?? "");

        return new Response(object === null ? "null" : await object.text(), {
          headers: { "content-type": "application/json" },
        });
      }
    }

    return fixtureWorker.fetch(request, env);
  },
};
