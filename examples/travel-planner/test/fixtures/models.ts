import { Effect, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, type Prompt, type Response } from "effect/unstable/ai";

import { PlannerInput, Trip, SaveTripRequest, PlannerSettings } from "../../src/domain.ts";
import { PlannerAttempt, trackTool } from "../../src/server/progress.ts";
import { requestsPublication } from "../../src/server/security.ts";
import { TravelContent } from "../../src/travel-content.ts";

export const fixtureTravelContent = TravelContent.make({
  title: "Fixture lodging shortlist",
  items: [
    {
      kind: "stay",
      name: "Fixture Tahoe cabin",
      location: "Lake Tahoe",
      url: "https://www.airbnb.com/rooms/24912220",
      photos: [
        {
          url: "https://a0.muscache.com/im/pictures/prohost-api/Hosting-24912220/original/212412ce-c6db-413b-8586-0a88654d4ae4.jpeg?im_w=720",
          caption: "A source photo reference used by the test",
        },
      ],
      highlights: ["Fixture amenity evidence"],
      price: null,
      note: "Test data only; availability is unverified.",
    },
  ],
});

const usage = { inputTokens: {}, outputTokens: {} };

const finish = (message: string): ReadonlyArray<Response.StreamPartEncoded> => [
  ...call("deliver_response", { message, content: null }),
];

const call = (name: string, params: Schema.Json): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id: `${name}-call`, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

/** Deliberately small offline script. Exercises native tools; it is never presented as AI. */
export const fixtureScript = (prompt: Prompt.Prompt): ReadonlyArray<Response.StreamPartEncoded> => {
  let input: typeof PlannerInput.Type | undefined;
  let latestIndex = -1;
  let userInputs = 0;

  for (const [index, message] of prompt.content.entries()) {
    if (message.role !== "user") continue;
    for (const part of message.content) {
      if (part.type !== "text") continue;
      const parsed = Schema.decodeOption(Schema.fromJsonString(PlannerInput))(part.text);

      if (parsed._tag === "Some") {
        userInputs++;
        input = parsed.value;
        latestIndex = index;
      }
    }
  }
  if (input === undefined) return finish("Where do you want to go?");
  if (input.message === "complete travel cards fixture")
    return call("deliver_response", {
      message: "Compare these stays. Exact availability is still unverified.",
      content: Schema.encodeSync(TravelContent)(fixtureTravelContent),
    });
  if (input.message === "plain shortlist fixture")
    return [
      { type: "text-start", id: "answer" },
      { type: "text-delta", id: "answer", delta: "Plain shortlist must not complete this run." },
      { type: "text-end", id: "answer" },
      { type: "finish", reason: "stop", usage },
    ];

  const results = prompt.content
    .slice(latestIndex + 1)
    .flatMap((message) =>
      message.role === "tool" ? message.content.filter((part) => part.type === "tool-result") : [],
    );

  const failure = results.find((result) => result.isFailure);

  if (failure !== undefined)
    return finish(
      "That operation did not complete. Inspect the activity, refresh the trip, and try again when ready.",
    );
  if (input.message === "travel cards fixture")
    return results.some((result) => result.name === "show_travel_options")
      ? finish("These sourced options are ready to compare.")
      : call("show_travel_options", Schema.encodeSync(TravelContent)(fixtureTravelContent));
  const published = results.find((result) => result.name === "publish_trip_site");

  if (published !== undefined)
    return finish("Your saved trip revision is published. Open its site from the trip card.");
  const saved = results.find((result) => result.name === "save_trip");

  if (saved !== undefined)
    return finish(
      "I've saved your draft. Tell me your dates, what you enjoy, or what you'd like to change. This is an offline demonstration; suggestions are unverified.",
    );
  const previous = results.find((result) => result.name === "get_trip");

  if (input.selectedTripId !== null && previous === undefined)
    return call("get_trip", { tripId: input.selectedTripId });
  if (previous !== undefined) {
    const trip = Schema.decodeUnknownSync(Trip)(previous.result);

    if (requestsPublication(input.message))
      return call("publish_trip_site", { tripId: trip.id, expectedRevision: trip.revision });

    return call(
      "save_trip",
      Schema.encodeSync(SaveTripRequest)({
        ...trip,
        tripId: trip.id,
        expectedRevision: trip.revision,
        notes: [...trip.notes.slice(-19), input.message.slice(0, 240)],
      }),
    );
  }
  if (!results.some((result) => result.name === "list_trips")) return call("list_trips", {});

  const destination = /kyoto/i.test(input.message)
    ? "Kyoto"
    : /copenhagen/i.test(input.message)
      ? "Copenhagen"
      : /lisbon/i.test(input.message)
        ? "Lisbon"
        : null;

  if (destination === null)
    return finish(
      "Where do you want to go? Offline mode has sample drafts for Lisbon, Kyoto, and Copenhagen. Live mode can plan other destinations.",
    );

  return call("save_trip", {
    tripId: null,
    expectedRevision: null,
    title: `A few days in ${destination}`,
    destination,
    summary: `An unverified sample itinerary for ${destination}. Adjust the dates and pace together.`,
    startDate: null,
    endDate: null,
    travelers: 1,
    days: [
      {
        title: "Arrive and settle in",
        activities: ["Explore the neighborhood on foot", "Find a relaxed local dinner"],
      },
      {
        title: "Make room for discovery",
        activities: [
          "Visit a museum or cultural site",
          "Leave the afternoon open for a neighborhood walk",
        ],
      },
      {
        title: "A slower last day",
        activities: ["Enjoy a local market", "Allow plenty of time for departure"],
      },
    ],
    notes: [
      `Earlier user messages in this conversation: ${userInputs - 1}`,
      "Offline sample: verify opening hours and reservations independently.",
      "No prices, availability, or bookings have been checked.",
    ],
  });
};

let observedSettings: PlannerSettings | null = null;
let progressStage = 0;
let completedProgressModels = 0;
let finalizedProgressModels = 0;

export const advanceFixtureProgress = () => {
  progressStage++;
};

export const fixtureProgressStatus = () => ({
  settings: observedSettings,
  completed: completedProgressModels,
  finalized: finalizedProgressModels,
});

const awaitProgressStage = (stage: number): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (progressStage < stage) yield* Effect.sleep("10 millis");
  });

const streamingFixture = Stream.fromEffect(
  Effect.gen(function* () {
    const attempt = yield* Effect.serviceOption(PlannerAttempt);

    if (Option.isNone(attempt))
      return yield* Effect.die("Fixture needs the real planner attempt writer");
    observedSettings = Schema.encodeSync(PlannerSettings)(
      yield* attempt.value.settings.pipe(Effect.orDie),
    );
    const writer = attempt.value.progress;

    yield* writer.newResponse;
    yield* writer.text("A quiet ");
    yield* trackTool("fixture-research", "Reading travel details", awaitProgressStage(1));
    yield* writer.text("escape.");
    yield* awaitProgressStage(2);
    completedProgressModels++;

    return [
      { type: "reasoning-start", id: "private" },
      { type: "reasoning-delta", id: "private", delta: "PRIVATE_REASONING_SENTINEL" },
      { type: "reasoning-end", id: "private" },
      ...finish("A quiet escape."),
    ] satisfies ReadonlyArray<Response.StreamPartEncoded>;
  }).pipe(
    Effect.ensuring(
      Effect.sync(() => {
        finalizedProgressModels++;
      }),
    ),
  ),
).pipe(Stream.flatMap(Stream.fromIterable));

export const FixtureModel = Model.make(
  "fixture",
  "travel-planner-script-v1",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: ({ prompt }) => {
        if (JSON.stringify(prompt).includes("streaming fixture")) return streamingFixture;
        const parts = fixtureScript(prompt);

        const cardsCompleted =
          JSON.stringify(prompt).includes("travel cards fixture") &&
          parts[0]?.type === "tool-call" &&
          parts[0].name === "deliver_response" &&
          !JSON.stringify(prompt).includes("complete travel cards fixture");

        // Hold the final answer so the test observes the already committed card result.
        return cardsCompleted
          ? Stream.fromEffect(awaitProgressStage(3)).pipe(
              Stream.flatMap(() => Stream.fromIterable(parts)),
            )
          : Stream.fromIterable(parts);
      },
    }),
  ),
);
