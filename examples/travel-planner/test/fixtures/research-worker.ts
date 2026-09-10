import { ThreadId } from "@effect-agent/core/Identifiers";
import { IdempotencyKey, Principal } from "@effect-agent/core/Receipt";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ThreadExport, ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import { LanguageModel, Model, type Prompt, type Response as AiResponse } from "effect/unstable/ai";

import {
  PlannerError,
  PlannerInput,
  SaveTripRequest,
  Trip,
  TripSiteStore,
} from "../../src/domain.ts";
import { ScoutInput } from "../../src/research/contracts.ts";
import { ResearchScoutBackground } from "../../src/research/scout.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import { previousEditorPlanner } from "../../src/server/planner.ts";
import { ownerOfThread, storageOwner } from "../../src/server/tenancy.ts";
import { FixtureBrowserLive } from "./browser.ts";
import fixtureWorker from "./worker.ts";

const call = (
  name: string,
  params: Schema.Json,
  id = name,
): ReadonlyArray<AiResponse.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
];

const finish = (message: string) => call("deliver_response", { message, content: null });

const results = (prompt: Prompt.Prompt, after = -1) =>
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
          const input = Schema.decodeUnknownOption(Schema.fromJsonString(schema))(part.text);

          return Option.isSome(input) ? [{ input: input.value, index }] : [];
        })
      : [],
  );

// Exercise real tool rejection and recovery, including a report with no selectedTripId.
const updateCurrentTrip = (prompt: Prompt.Prompt, after: number, note: string) => {
  const current = results(prompt, after);
  const listed = current.find((result) => result.name === "list_trips");

  if (!listed) return call("list_trips", {}, `list-${after}`);
  const trips = Schema.decodeUnknownSync(Schema.Array(Trip))(listed.result);

  if (trips.length !== 1 || trips[0]?.destination !== "Lisbon")
    throw new Error("Trip discovery must return only this conversation's Lisbon trip");
  if (current.some((result) => result.name === "save_trip" && !result.isFailure)) return null;
  const trip = trips[0];

  return call(
    "save_trip",
    Schema.encodeSync(SaveTripRequest)({
      ...trip,
      tripId: trip.id,
      expectedRevision: trip.revision,
      notes: [...trip.notes, note],
    }),
    `save-current-${after}`,
  );
};

const model = Model.make(
  "fixture",
  "research-v1",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: ({ prompt, tools }) =>
        Stream.unwrap(
          Effect.gen(function* () {
            const environment = yield* Effect.serviceOption(WorkerEnvironment);
            const thread = yield* Effect.serviceOption(ThreadObjectIdentity);

            if (Option.isNone(environment) || Option.isNone(thread))
              return yield* Effect.die("Missing fixture context");
            const identity = thread.value;
            const bucket = environment.value.APP_BUILDS;

            if (!bucket) return yield* Effect.die("Missing fixture bucket");
            const scout = inputs(prompt, ScoutInput).at(-1);

            if (scout) {
              yield* Effect.promise(() =>
                bucket.put(
                  `tools/${identity.threadId}`,
                  JSON.stringify(tools.map((tool) => tool.name)),
                ),
              );
              const key = `gate/${scout.input.title}`;

              yield* Effect.promise(() => bucket.put(`${key}/entered`, "yes"));
              while ((yield* Effect.promise(() => bucket.head(`${key}/open`))) === null)
                yield* Effect.sleep("25 millis");
              if (
                !results(prompt, scout.index).some((result) => result.name === "read_travel_page")
              )
                return Stream.fromIterable(
                  call(
                    "read_travel_page",
                    { url: "https://visitlisboa.com", focus: scout.input.message },
                    `read-${scout.index}`,
                  ),
                );

              return Stream.fromIterable(
                call(
                  "finish_research",
                  {
                    summary: `${scout.input.title}: ${scout.input.message}`,
                    sources: [
                      {
                        title: "Lisbon source",
                        url: "https://visitlisboa.com",
                        notes: "Fixture source evidence",
                        photos: [],
                      },
                    ],
                  },
                  `finish-${scout.index}`,
                ),
              );
            }
            const parent = inputs(prompt, PlannerInput).at(-1);

            const reportIndex = prompt.content.findLastIndex(
              (message) =>
                message.role === "user" &&
                message.content.some(
                  (part) =>
                    part.type === "text" && part.text.startsWith("Internal research completion"),
                ),
            );

            if (reportIndex > (parent?.index ?? -1)) {
              const report = prompt.content[reportIndex];

              if (
                report?.role === "user" &&
                report.content.some(
                  (part) => part.type === "text" && part.text.includes('"title":"Stays"'),
                )
              )
                return Stream.fromIterable(
                  call(
                    "research_scout_start",
                    { title: "Recursive scout", message: "Must be denied" },
                    `recursive-${reportIndex}`,
                  ),
                );

              return Stream.fromIterable(
                updateCurrentTrip(prompt, reportIndex, "Research findings saved after restart") ??
                  finish("Research update received."),
              );
            }
            if (!parent) return Stream.fromIterable(finish("Ready"));
            const current = results(prompt, parent.index);

            if (parent.input.message === "start research") {
              const started = current.filter(
                (result) => result.name === "research_scout_start",
              ).length;

              if (started < 2) {
                const title = started === 0 ? "Stays" : "Activities";

                return Stream.fromIterable(
                  call(
                    "research_scout_start",
                    { title, message: "Lisbon hold initial research" },
                    `start-${title}`,
                  ),
                );
              }

              return Stream.fromIterable(finish("Research is running. What is your budget?"));
            }
            if (
              parent.input.message.startsWith("follow research") &&
              !current.some((result) => result.name === "research_scout_follow_up")
            ) {
              const started = results(prompt).find(
                (result) => result.name === "research_scout_start" && !result.isFailure,
              );

              if (!started) return yield* Effect.die("Missing existing scout");

              const accepted = yield* Schema.decodeUnknownEffect(
                ResearchScoutBackground.tools.research_scout_start.successSchema,
              )(started.result).pipe(Effect.orDie);

              return Stream.fromIterable(
                call(
                  "research_scout_follow_up",
                  {
                    worker: Schema.encodeSync(
                      ResearchScoutBackground.tools.research_scout_follow_up.parametersSchema.fields
                        .worker,
                    )(accepted.worker),
                    parameters: { title: "Stays", message: parent.input.message },
                  },
                  `follow-${parent.index}`,
                ),
              );
            }

            if (parent.input.message.startsWith("follow research")) {
              const referenceId = /referenceTripId=([a-f0-9-]+)/.exec(parent.input.message)?.[1];
              const reference = current.find((result) => result.name === "get_trip");

              if (!referenceId) return yield* Effect.die("Missing other-trip fixture ID");
              if (!reference) return Stream.fromIterable(call("get_trip", { tripId: referenceId }));
              const wrongSave = current.find((result) => result.id === "save-wrong-trip");

              if (!wrongSave) {
                const trip = Schema.decodeUnknownSync(Trip)(reference.result);

                return Stream.fromIterable(
                  call(
                    "save_trip",
                    Schema.encodeSync(SaveTripRequest)({
                      ...trip,
                      tripId: trip.id,
                      expectedRevision: trip.revision,
                      notes: ["Must not overwrite another conversation"],
                    }),
                    "save-wrong-trip",
                  ),
                );
              }
              if (!wrongSave.isFailure)
                return yield* Effect.die("Cross-conversation write was accepted");

              const next = updateCurrentTrip(
                prompt,
                parent.index,
                "Budget 200, quiet neighborhood",
              );

              if (next) return Stream.fromIterable(next);
            }

            return Stream.fromIterable(finish(`Planner handled: ${parent.input.message}`));
          }),
        ),
    }),
  ),
);

const sites = Layer.succeed(TripSiteStore, {
  publish: () =>
    Effect.fail(new PlannerError({ code: "publication", message: "Unused fixture publication" })),
  load: () => Effect.succeed(null),
});

export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(model, "research-v1", "Research fixture", FixtureBrowserLive),
) {
  fetch(request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const store = yield* ThreadStore;
        const url = new URL(request.url);

        const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(
          url.searchParams.get("thread") ?? identity.threadId,
        );

        if (url.pathname === "/__research/seed") {
          const runtime = yield* DurableAgentRuntime;
          const owner = ownerOfThread(threadId);

          yield* runtime.submitRegistered(
            { definition: previousEditorPlanner },
            {
              message: "Previous trip conversation",
              selectedTripId: null,
              publication: null,
            },
            {
              threadId,
              principal: Schema.decodeSync(Principal)(
                owner === storageOwner ? "travel-planner-owner" : owner,
              ),
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("previous-planner-input"),
            },
          );

          return Response.json({ accepted: true });
        }

        return new Response(
          yield* Schema.encodeEffect(Schema.fromJsonString(ThreadExport))(
            yield* store.export(ThreadExportRequest.make({ threadId })),
          ),
        );
      }),
    );
  }
}

export default {
  async fetch(request: Request, env: Cloudflare.Env & { readonly PLANNER_TOKEN?: string }) {
    const url = new URL(request.url);

    if (url.pathname.startsWith("/__research/")) {
      if (
        !env.PLANNER_TOKEN ||
        request.headers.get("authorization") !== `Bearer ${env.PLANNER_TOKEN}`
      )
        return new Response("Unauthorized", { status: 401 });
      if (url.pathname === "/__research/seed")
        return env.THREADS.getByName(ownerOfThread(url.searchParams.get("thread") ?? "")).fetch(
          request,
        );
      if (url.pathname === "/__research/journal") {
        const threadId = url.searchParams.get("thread") ?? "";

        return env.THREADS.getByName(
          threadId.startsWith("worker:") ? threadId : ownerOfThread(threadId),
        ).fetch(request);
      }
      const bucket = env.APP_BUILDS;

      if (!bucket) return new Response("Missing fixture bucket", { status: 500 });
      if (url.pathname === "/__research/gate") {
        const key = `gate/${url.searchParams.get("name") ?? ""}`;

        if (request.method === "POST") await bucket.put(`${key}/open`, "yes");

        return Response.json({ entered: (await bucket.head(`${key}/entered`)) !== null });
      }

      return new Response(
        (await (await bucket.get(`tools/${url.searchParams.get("thread") ?? ""}`))?.text()) ??
          "null",
      );
    }

    return fixtureWorker.fetch(request, env);
  },
};
