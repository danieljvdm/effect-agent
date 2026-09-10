import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { AppId, PlannerError, TripApp, TripAppData, TripId } from "../domain.ts";
import { ownerOfThread } from "../server/tenancy.ts";
import { TripRepository } from "../server/trips.ts";
import { AppRepository, AppRepositoryLive } from "./repository.ts";

export const AppCommand = Schema.Union([
  Schema.TaggedStruct("Get", { tripId: TripId }),
  Schema.TaggedStruct("GetById", { appId: AppId }),
  Schema.TaggedStruct("Save", { app: TripApp, expectedRevision: Schema.NullOr(Schema.Natural) }),
  Schema.TaggedStruct("Data", { tripId: TripId }),
]);

const reply = <A, I>(schema: Schema.Codec<A, I>) =>
  Schema.Union([
    Schema.TaggedStruct("Success", { value: schema }),
    Schema.TaggedStruct("Failure", { error: PlannerError }),
  ]);

const unavailable = () =>
  new PlannerError({ code: "storage", message: "Trip app data is unavailable. Please retry." });

/** An owner-only native RPC, never exposed as an app-controlled service binding. */
export const serveAppRepository = Effect.fn("serveAppRepository")(function* (encoded: string) {
  const request = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(AppCommand))(
    encoded,
  ).pipe(Effect.mapError(unavailable));

  const repository = yield* AppRepository;

  const encode = <A, I, R>(
    schema: Schema.Codec<A, I>,
    operation: Effect.Effect<A, PlannerError, R>,
  ) =>
    operation.pipe(
      Effect.match({
        onSuccess: (value) => ({ _tag: "Success" as const, value }),
        onFailure: (error) => ({ _tag: "Failure" as const, error }),
      }),
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(reply(schema)))),
      Effect.mapError(unavailable),
    );

  switch (request._tag) {
    case "Get":
      return yield* encode(Schema.NullOr(TripApp), repository.get(request.tripId));
    case "GetById":
      return yield* encode(Schema.NullOr(TripApp), repository.getById(request.appId));
    case "Save":
      return yield* encode(TripApp, repository.save(request.app, request.expectedRevision));
    case "Data":
      return yield* encode(
        TripAppData,
        Effect.gen(function* () {
          const trips = yield* TripRepository;
          const trip = yield* trips.get(request.tripId);

          return {
            title: trip.title,
            destination: trip.destination,
            summary: trip.summary,
            startDate: trip.startDate,
            endDate: trip.endDate,
            travelers: trip.travelers,
            days: trip.days.map((day) => ({ ...day, date: null })),
            stays: (trip.places ?? [])
              .filter((place) => place.kind === "stay")
              .map((place) => ({
                id: place.id,
                name: place.label,
                location: trip.destination,
                url: place.url,
              })),
            places: trip.places ?? [],
          };
        }),
      );
  }
});

export const callAppRepository = <A, I>(
  env: Cloudflare.Env,
  owner: string,
  schema: Schema.Codec<A, I>,
  request: typeof AppCommand.Type,
): Effect.Effect<A, PlannerError> =>
  Effect.gen(function* () {
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(AppCommand))(request);

    const response = yield* Effect.tryPromise({
      try: () => env.THREADS.getByName(owner).tripApp(encoded),
      catch: unavailable,
    });

    const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(reply(schema)))(
      response,
    );

    if (result._tag === "Failure") return yield* result.error;

    return result.value;
  }).pipe(Effect.catchTag("SchemaError", unavailable));

export const OwnerAppRepositoryLive = Layer.unwrap(
  Effect.gen(function* () {
    const identity = yield* ThreadObjectIdentity;
    const owner = ownerOfThread(identity.threadId);

    if (owner === identity.threadId) return AppRepositoryLive;
    const env = yield* WorkerEnvironment;

    return Layer.succeed(AppRepository, appRepositoryForOwner(env, owner));
  }),
);

/** The caller derives owner from authenticated ingress or canonical worker lineage. */
export const appRepositoryForOwner = (
  env: Cloudflare.Env,
  owner: string,
): AppRepository["Service"] => ({
  get: (tripId) => callAppRepository(env, owner, Schema.NullOr(TripApp), { _tag: "Get", tripId }),
  getById: (appId) =>
    callAppRepository(env, owner, Schema.NullOr(TripApp), { _tag: "GetById", appId }),
  save: (app, expectedRevision) =>
    callAppRepository(env, owner, TripApp, { _tag: "Save", app, expectedRevision }),
});
