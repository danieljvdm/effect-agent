import { Effect, Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { trackTool } from "../server/progress.ts";
import { TripRepository } from "../server/trips.ts";
import { AppRepository } from "./repository.ts";
import {
  addTripAppMap,
  createTripApp,
  editTripApp,
  readTripAppFiles,
  requireAppTrip,
  restoreTripApp,
} from "./service.ts";
import { AppTools } from "./tools.ts";

export const AppToolsLive = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) =>
    AppTools.toLayer({
      create_trip_app: ({ tripId }, context) =>
        trackTool(
          context.toolCallId ?? "create_trip_app",
          "Building your trip app",
          createTripApp(tripId).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
      get_trip_app: ({ tripId }) =>
        Effect.gen(function* () {
          yield* requireAppTrip(tripId);

          return yield* Effect.flatMap(AppRepository, (apps) => apps.get(tripId));
        }),
      read_trip_app_files: ({ tripId, paths }) => readTripAppFiles(tripId, paths),
      edit_trip_app: (input, context) =>
        trackTool(
          context.toolCallId ?? "edit_trip_app",
          "Updating your trip app",
          editTripApp(input).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
      add_trip_app_map: ({ tripId }, context) =>
        trackTool(
          context.toolCallId ?? "add_trip_app_map",
          "Adding your journey map",
          addTripAppMap(tripId).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
      restore_trip_app: ({ tripId, commitId }, context) =>
        trackTool(
          context.toolCallId ?? "restore_trip_app",
          "Restoring your app",
          restoreTripApp(tripId, commitId).pipe(Effect.provideService(WorkerEnvironment, env)),
        ),
      set_trip_places: ({ tripId, places }, context) =>
        trackTool(
          context.toolCallId ?? "set_trip_places",
          "Saving map locations",
          Effect.gen(function* () {
            const trip = yield* requireAppTrip(tripId);
            const trips = yield* TripRepository;

            return yield* trips.save(
              { ...trip, tripId, expectedRevision: trip.revision, places },
              yield* trips.conversationId(tripId),
            );
          }),
        ),
    }),
  ),
);
