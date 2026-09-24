import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import { ThreadId } from "effect-agent/identifiers";
import { ThreadExport, ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { WorkerCompletion, WorkerUpdate } from "effect-agent/worker";
import { DurableObject, WorkerEnvironment } from "effect-cf";
import {
  LanguageModel,
  Model,
  Toolkit,
  type Prompt,
  type Response as AiResponse,
} from "effect/unstable/ai";

import { PlannerError, PlannerInput, TripSiteStore } from "../../src/domain.ts";
import { ReadTravelPage } from "../../src/research.ts";
import { ScoutInput } from "../../src/research/contracts.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import { ownerOfThread } from "../../src/server/tenancy.ts";
import fixtureWorker from "./worker.ts";

const call = (
  name: string,
  params: Schema.Json,
  id = name,
): ReadonlyArray<AiResponse.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  {
    type: "finish",
    reason: "tool-calls",
    usage: { inputTokens: { total: 24_000 }, outputTokens: { total: 100 } },
  },
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
          const input = Schema.decodeOption(Schema.fromJsonString(schema))(part.text);

          return Option.isSome(input) ? [{ input: input.value, index }] : [];
        })
      : [],
  );

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
            const bucket = environment.value.APP_BUILDS;

            if (!bucket) return yield* Effect.die("Missing fixture bucket");
            const scout = inputs(prompt, ScoutInput).at(-1);

            if (scout) {
              const key = `gate/${scout.input.title}`;

              const milestone = {
                summary:
                  (scout.input.title === "Report denial" ? "Report denial: " : "") +
                  "The coastal trail offers a verified 50 km route; entry availability is unconfirmed.",
                sources: ["https://visitlisboa.com"],
              };

              if (
                scout.input.title === "Report denial" &&
                tools.some((tool) => tool.name === "emit_update") &&
                !results(prompt, scout.index).some((result) => result.name === "emit_update")
              )
                return Stream.fromIterable(
                  call("emit_update", { value: milestone }, `milestone-${scout.index}`),
                );

              yield* Effect.promise(() => bucket.put(`${key}/entered`, "yes"));
              while ((yield* Effect.promise(() => bucket.head(`${key}/open`))) === null)
                yield* Effect.sleep("25 millis");

              const reads = results(prompt, scout.index).filter(
                (result) => result.name === "read_travel_page",
              ).length;

              if (reads < 1)
                return Stream.fromIterable(
                  call(
                    "read_travel_page",
                    { url: "https://visitlisboa.com", focus: scout.input.message },
                    `read-${scout.index}-${reads}`,
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

            const update = inputs(prompt, WorkerUpdate).at(-1);
            const completion = inputs(prompt, WorkerCompletion).at(-1);
            const frameworkIndex = Math.max(update?.index ?? -1, completion?.index ?? -1);

            if (frameworkIndex > (parent?.index ?? -1)) {
              const report = prompt.content[frameworkIndex];

              if (
                report?.role === "user" &&
                report.content.some(
                  (part) => part.type === "text" && part.text.includes("Report denial"),
                )
              )
                return Stream.fromIterable(
                  call(
                    "research_scout_start",
                    {
                      title: "Recursive scout",
                      message: "Must be denied",
                    },
                    `recursive-${frameworkIndex}`,
                  ),
                );

              return Stream.fromIterable(
                finish(
                  frameworkIndex === update?.index
                    ? "Verified milestone received while research continues."
                    : "Research update received.",
                ),
              );
            }

            if (!parent) return Stream.fromIterable(finish("Ready"));
            const current = results(prompt, parent.index);

            if (parent.input.message === "start report denial")
              return Stream.fromIterable(
                current.some((result) => result.name === "research_scout_start")
                  ? finish("Research started.")
                  : call("research_scout_start", {
                      title: "Report denial",
                      message: "Find coastal trails; distance unknown",
                    }),
              );

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

const researchBrowser = Toolkit.make(ReadTravelPage).toLayer({
  read_travel_page: ({ url, focus }) =>
    Effect.succeed({
      url,
      title: "Fixture travel source",
      excerpts: [focus],
      photos: [],
      truncated: false,
    }),
});

export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(model, "research-v1", "Research fixture", researchBrowser),
  { ownershipLeaseDuration: 3_000, leaseRenewalInterval: 500 },
) {
  fetch(request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const store = yield* ThreadStore;
        const url = new URL(request.url);

        const threadId = yield* Schema.decodeEffect(ThreadId)(
          url.searchParams.get("thread") ?? identity.threadId,
        );

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
      if (url.pathname === "/__research/journal") {
        const threadId = url.searchParams.get("thread") ?? "";

        return env.ACCOUNT_THREADS.getByName(
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
    }

    return fixtureWorker.fetch(request, env);
  },
};
