import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import {
  ConversationId,
  ConversationSummary,
  PlannerError,
  PublishedSite,
  SaveTripRequest,
  Trip,
  TripId,
} from "../domain.ts";
import { ownerOfThread } from "./tenancy.ts";
import { TripRepository, TripRepositoryLive } from "./trips.ts";

const Request = Schema.Union([
  Schema.TaggedStruct("List", {}),
  Schema.TaggedStruct("ListConversations", {}),
  Schema.TaggedStruct("RememberConversation", { conversation: ConversationSummary }),
  Schema.TaggedStruct("Get", { id: TripId }),
  Schema.TaggedStruct("Conversation", { id: TripId }),
  Schema.TaggedStruct("Save", { request: SaveTripRequest, conversationId: ConversationId }),
  Schema.TaggedStruct("Publish", { site: PublishedSite }),
]);

const response = <A, I>(schema: Schema.Codec<A, I>) =>
  Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: schema }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), error: PlannerError }),
  ]);

const unavailable = () =>
  new PlannerError({ code: "storage", message: "Trip storage is unavailable. Please retry." });

/** Private native RPC preserves the owner catalogue while each conversation gets its own DO. */
export const serveTripRepository = Effect.fn("serveTripRepository")(function* (encoded: string) {
  const request = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Request))(encoded).pipe(
    Effect.mapError(unavailable),
  );

  const repository = yield* TripRepository;

  const encode = <A, I>(schema: Schema.Codec<A, I>, operation: Effect.Effect<A, PlannerError>) =>
    operation.pipe(
      Effect.match({
        onSuccess: (value) => ({ _tag: "Success" as const, value }),
        onFailure: (error) => ({ _tag: "Failure" as const, error }),
      }),
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(response(schema)))),
      Effect.mapError(unavailable),
    );

  switch (request._tag) {
    case "List":
      return yield* encode(Schema.Array(Trip), repository.list);
    case "ListConversations":
      return yield* encode(Schema.Array(ConversationSummary), repository.listConversations);
    case "RememberConversation":
      return yield* encode(
        Schema.Null,
        repository.rememberConversation(request.conversation).pipe(Effect.as(null)),
      );
    case "Get":
      return yield* encode(Trip, repository.get(request.id));
    case "Conversation":
      return yield* encode(ConversationId, repository.conversationId(request.id));
    case "Save":
      return yield* encode(Trip, repository.save(request.request, request.conversationId));
    case "Publish":
      return yield* encode(
        Schema.Null,
        repository.recordPublication(request.site).pipe(Effect.as(null)),
      );
  }
});

export const OwnerTripRepositoryLive: Layer.Layer<
  TripRepository,
  PlannerError,
  ThreadObjectIdentity | WorkerEnvironment | Layer.Services<typeof TripRepositoryLive>
> = Layer.unwrap(
  Effect.gen(function* () {
    const identity = yield* ThreadObjectIdentity;

    // Worker objects acquire parent repositories only inside their authorized attempt.
    if (identity.threadId.startsWith("worker:")) {
      const denied = Effect.fail(
        new PlannerError({ code: "invalid", message: "An authorized parent account is required." }),
      );

      return Layer.succeed(TripRepository, {
        list: denied,
        listConversations: denied,
        rememberConversation: () => denied,
        get: () => denied,
        conversationId: () => denied,
        save: () => denied,
        recordPublication: () => denied,
      });
    }
    const storageOwner = ownerOfThread(identity.threadId);

    if (identity.threadId === storageOwner) return TripRepositoryLive;
    const env = yield* WorkerEnvironment;

    return Layer.succeed(TripRepository, tripRepositoryForOwner(env, storageOwner));
  }),
);

/** Only host-verified account identity may select the private repository destination. */
export const tripRepositoryForOwner = (
  env: Cloudflare.Env,
  storageOwner: string,
): TripRepository["Service"] => {
  const call = <A, I>(schema: Schema.Codec<A, I>, request: typeof Request.Type) =>
    Effect.gen(function* () {
      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Request))(request);

      const reply = yield* Effect.tryPromise({
        try: () => env.ACCOUNT_THREADS.getByName(storageOwner).tripRepository(encoded),
        catch: unavailable,
      });

      const result = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(response(schema)))(
        reply,
      );

      if (result._tag === "Failure") return yield* result.error;

      return result.value;
    }).pipe(Effect.catchTag("SchemaError", unavailable));

  return {
    list: call(Schema.Array(Trip), { _tag: "List" }),
    listConversations: call(Schema.Array(ConversationSummary), { _tag: "ListConversations" }),
    rememberConversation: (conversation) =>
      call(Schema.Null, { _tag: "RememberConversation", conversation }).pipe(Effect.asVoid),
    get: (id) => call(Trip, { _tag: "Get", id }),
    conversationId: (id) => call(ConversationId, { _tag: "Conversation", id }),
    save: (request, conversationId) => call(Trip, { _tag: "Save", request, conversationId }),
    recordPublication: (site) => call(Schema.Null, { _tag: "Publish", site }).pipe(Effect.asVoid),
  };
};
