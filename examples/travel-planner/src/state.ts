import type { Redacted } from "effect";
import { Data, Effect, Exit, Layer, Option, Schedule, Schema, Semaphore, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { AsyncResult, Atom, AtomRpc, Reactivity } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";

import { AccountError, type AccountSession } from "./auth/account";
import { runtime } from "./auth/client";
import { mergeSpeech, speechContext } from "./conversation.ts";
import type { OpenAiConnection } from "./credential-domain";
import {
  PlannerError,
  PlannerRpcs,
  ProgressRpcs,
  PlannerSettings,
  defaultPlannerSettings,
  type PlannerProgress,
  type PublishTripRequest,
  type SavedTrip,
  type ConversationSummary,
  type PlannerSnapshot,
  type SendMessageRequest,
  type SpokenMessage,
} from "./domain";

// Seeded once from auth.session in Auth's current account registry. Disposing that
// registry cancels every request/stream and drops all cached state, including drafts.
export const sessionAtom = Atom.make<AsyncResult.AsyncResult<AccountSession, AccountError>>(
  AsyncResult.initial(),
);

const visibleSessionAtom = Atom.make((get): AccountSession | null => {
  const result = get(sessionAtom);

  if (AsyncResult.isSuccess(result)) return result.value;
  if (
    AsyncResult.isFailure(result) &&
    Option.getOrNull(AsyncResult.error(result))?.code !== "unavailable"
  )
    return null;

  return Option.getOrNull(get.self<AccountSession | null>());
});

export const selectionAtom = Atom.make<{
  readonly conversationId: string | null;
  readonly tripId: string | null;
}>({ conversationId: null, tripId: null });

export const draftAtom = Atom.make("");

/** New typed input fences earlier spoken results without cancelling accepted work. */
export const latestTypedInputAtom = Atom.make({ revision: 0, text: "" });

/** The same submitted request is observed by voice; voice never resubmits a typed input. */
export const conversationRequestAtom = Atom.make<SendMessageRequest | null>(null);

/** In-tab speech remains available after ending a call, including exchanges without work. */
export const spokenConversationAtom = Atom.make<{
  readonly subjectId: string;
  readonly conversationId: string;
  readonly messages: ReadonlyArray<SpokenMessage>;
  readonly baseline: ReadonlyArray<string>;
  readonly responses: ReadonlyArray<string>;
  readonly active: boolean;
} | null>(null).pipe(Atom.keepAlive);

type PendingMessage = {
  readonly subjectId: string;
  readonly text: string;
  readonly tripId: string | null;
  readonly conversationId: string;
  readonly id: string;
  readonly settings?: PlannerSettings;
  readonly voice?: SendMessageRequest["voice"];
  readonly placement: "conversation" | "queue";
  readonly status: "sending" | "accepted" | "failed";
};

export class PlannerClient extends AtomRpc.Service<PlannerClient>()("travel-planner/Client", {
  runtime,
  group: PlannerRpcs,
  protocol: (get) =>
    RpcClient.layerProtocolHttp({
      url: "/api/rpc",
      transformClient: (client) =>
        HttpClient.mapRequest(
          client,
          HttpClientRequest.setHeader(
            "x-elsewhere-account",
            AsyncResult.getOrElse(get.once(sessionAtom), () => null)?.subjectId ?? "",
          ),
        ),
    }).pipe(Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer])),
}) {}

const connectionQuery = Atom.family((subjectId: string) =>
  PlannerClient.runtime
    .atom((get) =>
      Effect.gen(function* () {
        const session = get(sessionAtom);

        if (
          !AsyncResult.isSuccess(session) ||
          session.waiting ||
          session.value.subjectId !== subjectId
        )
          return yield* Effect.interrupt;
        const client = yield* PlannerClient;
        const connection = yield* client("GetOpenAiConnection", undefined);
        const current = get.once(sessionAtom);

        if (!AsyncResult.isSuccess(current) || current.value.subjectId !== subjectId)
          return yield* Effect.interrupt;

        return connection;
      }),
    )
    .pipe(PlannerClient.runtime.factory.withReactivity([`openai-connection:${subjectId}`])),
);

export const openAiConnectionAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  return AsyncResult.isSuccess(session)
    ? get(connectionQuery(session.value.subjectId))
    : AsyncResult.initial<OpenAiConnection>();
});

/** UI-only presentation flag; the password draft stays in its mounted form. */
export const modelSettingsOpenAtom = Atom.make(false);

export const changeOpenAiConnectionAtom = PlannerClient.runtime.fn<
  | { readonly action: "connect"; readonly apiKey: Redacted.Redacted<string> }
  | { readonly action: "disconnect" }
>()(
  Effect.fnUntraced(function* (request, get) {
    const session = get(sessionAtom);

    if (!AsyncResult.isSuccess(session) || session.waiting)
      return yield* new PlannerError({
        code: "unavailable",
        message: "Wait for sign-in to finish before changing your OpenAI key.",
      });
    const client = yield* PlannerClient;
    const current = get(sessionAtom);

    if (
      !AsyncResult.isSuccess(current) ||
      current.waiting ||
      current.value.subjectId !== session.value.subjectId
    )
      return yield* Effect.interrupt;

    return yield* Reactivity.mutation(
      request.action === "connect"
        ? client("ConnectOpenAi", { apiKey: request.apiKey })
        : client("DisconnectOpenAi", undefined),
      [`openai-connection:${session.value.subjectId}`],
    );
  }),
);

export const refreshOpenAiConnectionAtom = Atom.fnSync<void>()((_, get) => {
  get.set(changeOpenAiConnectionAtom, Atom.Reset);
  const session = get(sessionAtom);

  if (AsyncResult.isSuccess(session)) get.refresh(connectionQuery(session.value.subjectId));
});

type AccountPreferences = {
  readonly settings: PlannerSettings | null;
  readonly generation: number;
  readonly saving: boolean;
  readonly saveFailed: boolean;
};

const accountPreferences = Atom.family((_subjectId: string) =>
  Atom.make<AccountPreferences>({
    settings: null,
    generation: 0,
    saving: false,
    saveFailed: false,
  }).pipe(Atom.keepAlive),
);

const preferencesWriteLock = Atom.make(Semaphore.make(1)).pipe(Atom.keepAlive);

const preferencesQuery = Atom.family((subjectId: string) =>
  PlannerClient.runtime
    .atom((get) =>
      Effect.gen(function* () {
        const session = get(sessionAtom);

        if (
          !AsyncResult.isSuccess(session) ||
          session.waiting ||
          session.value.subjectId !== subjectId
        )
          return yield* Effect.interrupt;
        const state = accountPreferences(subjectId);
        const generation = get.once(state).generation;
        const client = yield* PlannerClient;
        const settings = yield* client("GetPlannerSettings", undefined);
        const latest = get.once(state);
        const verified = get.once(sessionAtom);

        if (
          latest.generation === generation &&
          AsyncResult.isSuccess(verified) &&
          !verified.waiting &&
          verified.value.subjectId === subjectId
        )
          get.set(state, { ...latest, settings });

        return settings;
      }),
    )
    .pipe(
      PlannerClient.runtime.factory.withReactivity(["planner-settings"]),
      Atom.setIdleTTL("5 minutes"),
    ),
);

// Explicit selections take precedence over an older in-flight hydration response.
const settingsResultAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  if (!AsyncResult.isSuccess(session) || session.waiting)
    return AsyncResult.initial<PlannerSettings>();
  const query = get(preferencesQuery(session.value.subjectId));
  const state = get(accountPreferences(session.value.subjectId));

  return state.settings === null ? query : AsyncResult.success(state.settings);
});

/** Defaults are presentation-only until this account's persisted preference is loaded. */
export const settingsAtom = Atom.make((get) =>
  AsyncResult.getOrElse(get(settingsResultAtom), () => defaultPlannerSettings),
);

export const settingsStatusAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  if (!AsyncResult.isSuccess(session) || session.waiting)
    return { loading: true, saving: false, error: null };
  const query = get(preferencesQuery(session.value.subjectId));
  const state = get(accountPreferences(session.value.subjectId));

  return {
    loading: state.settings === null,
    saving: state.saving,
    error: state.saveFailed
      ? "Couldn't save your model settings. Choose the setting again to retry."
      : AsyncResult.isFailure(query)
        ? "Couldn't load your model settings. Reload to try again."
        : null,
  };
});

export const changeSettingsAtom = PlannerClient.runtime.fn<
  | { readonly kind: "model"; readonly value: string }
  | { readonly kind: "reasoning"; readonly value: string }
  | { readonly kind: "speed" }
>()(
  Effect.fnUntraced(function* (change, get) {
    const session = get(sessionAtom);

    if (!AsyncResult.isSuccess(session) || session.waiting)
      return yield* new AccountError({
        code: "unauthorized",
        message: "Sign in before choosing model settings.",
      });
    const subjectId = session.value.subjectId;
    const lock = yield* get.result(preferencesWriteLock);

    return yield* lock.withPermits(1)(
      Effect.gen(function* () {
        const verified = get(sessionAtom);

        if (
          !AsyncResult.isSuccess(verified) ||
          verified.waiting ||
          verified.value.subjectId !== subjectId
        )
          return yield* Effect.interrupt;
        const current = yield* get.result(settingsResultAtom);
        const hydratedSession = get(sessionAtom);

        if (
          !AsyncResult.isSuccess(hydratedSession) ||
          hydratedSession.waiting ||
          hydratedSession.value.subjectId !== subjectId
        )
          return yield* Effect.interrupt;

        const candidate =
          change.kind === "model"
            ? {
                ...current,
                model: change.value,
                reasoningEffort:
                  change.value === "gpt-6-astra" && current.reasoningEffort === "none"
                    ? "low"
                    : current.reasoningEffort,
              }
            : change.kind === "reasoning"
              ? { ...current, reasoningEffort: change.value }
              : { ...current, fast: !current.fast };

        const settings = yield* Schema.decodeUnknownEffect(PlannerSettings)(candidate);
        const state = accountPreferences(subjectId);
        const generation = get(state).generation + 1;

        get.set(state, { settings, generation, saving: true, saveFailed: false });
        const client = yield* PlannerClient;

        return yield* Reactivity.mutation(client("SavePlannerSettings", settings), [
          "planner-settings",
        ]).pipe(
          Effect.onExit((exit) =>
            Effect.sync(() => {
              const latest = get(state);

              if (latest.generation === generation)
                get.set(state, { ...latest, saving: false, saveFailed: Exit.isFailure(exit) });
            }),
          ),
        );
      }),
    );
  }),
  { concurrent: true },
);

export class ProgressClient extends AtomRpc.Service<ProgressClient>()(
  "travel-planner/ProgressClient",
  {
    runtime,
    group: ProgressRpcs,
    protocol: (get) =>
      RpcClient.layerProtocolHttp({
        url: "/api/progress",
        transformClient: (client) =>
          HttpClient.mapRequest(
            client,
            HttpClientRequest.setHeader(
              "x-elsewhere-account",
              AsyncResult.getOrElse(get.once(sessionAtom), () => null)?.subjectId ?? "",
            ),
          ),
      }).pipe(Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer])),
  },
) {}

class ConversationKey<Id extends string | null> extends Data.Class<{
  readonly subjectId: string;
  readonly conversationId: Id;
}> {}

const progressStream = Atom.family(({ conversationId }: ConversationKey<string>) =>
  ProgressClient.runtime.atom(
    Stream.unwrap(
      ProgressClient.use((client) => Effect.succeed(client("WatchProgress", { conversationId }))),
    ).pipe(
      Stream.retry(Schedule.spaced("2 seconds")),
      Stream.repeat(Schedule.spaced("200 millis")),
    ),
  ),
);

export const progressAtom = Atom.make((get) => {
  const conversationId = get(selectionAtom).conversationId;
  const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));
  const session = get(sessionAtom);

  return AsyncResult.isSuccess(session) &&
    !session.waiting &&
    conversationId !== null &&
    (snapshot?.pending ?? 0) > 0
    ? get(
        progressStream(new ConversationKey({ subjectId: session.value.subjectId, conversationId })),
      )
    : AsyncResult.initial<PlannerProgress>();
});

// Retain only the query, not its polling wrapper: switching trips stops that
// conversation's timer but preserves its history for return visits in this tab.
// Each verified identity has separate nodes, without putting subjectIds on the wire.
// Use one structural key: nested family functions can be collected independently
// of their mounted atoms because Atom.family holds its values through WeakRef.
const snapshotQuery = Atom.family(({ subjectId, conversationId }: ConversationKey<string | null>) =>
  PlannerClient.runtime
    .atom((get) =>
      Effect.gen(function* () {
        const session = yield* get.result(sessionAtom, { suspendOnWaiting: true });

        // A retained inactive query must not refetch using a different user's cookie.
        if (session.subjectId !== subjectId) return yield* Effect.interrupt;
        const client = yield* PlannerClient;

        return yield* client("GetPlanner", { conversationId });
      }),
    )
    .pipe(PlannerClient.runtime.factory.withReactivity(["planner"]), Atom.setIdleTTL("30 minutes")),
);

// Start the next polling interval only after the current request finishes.
// Refreshing an in-flight query interrupts it, which starves slower conversations.
const polledSnapshot = Atom.family((key: ConversationKey<string | null>) => {
  const query = snapshotQuery(key);
  const refresh = query.pipe(Atom.withRefresh("2 seconds"));

  return Atom.make((get) => {
    const result = get(query);

    return result.waiting ? result : get(refresh);
  });
});

type ConversationView = {
  readonly subjectId: string | null;
  readonly conversationId: string | null;
  readonly result: Atom.Type<ReturnType<typeof snapshotQuery>>;
};

const conversationViewAtom = Atom.make((get): ConversationView => {
  const session = get(visibleSessionAtom);
  const conversationId = get(selectionAtom).conversationId;

  if (session === null) return { subjectId: null, conversationId, result: AsyncResult.initial() };

  const result = get(
    polledSnapshot(new ConversationKey({ subjectId: session.subjectId, conversationId })),
  );

  const previous = Option.getOrNull(get.self<ConversationView>());

  const sameConversation =
    previous?.subjectId === session.subjectId && previous.conversationId === conversationId;

  // Only a new conversation needs an initial loading screen. Runtime rebuilds
  // and overlapping refreshes preserve the last view for this exact identity.
  return {
    subjectId: session.subjectId,
    conversationId,
    result:
      sameConversation && Option.isSome(AsyncResult.value(previous.result))
        ? AsyncResult.isInitial(result)
          ? AsyncResult.waiting(previous.result)
          : AsyncResult.replacePrevious(result, Option.some(previous.result))
        : result,
  };
});

export const plannerAtom = Atom.map(conversationViewAtom, (view) => view.result);

/** A missing snapshot is loading or failed, never an empty conversation. */
export const conversationStatusAtom = Atom.make((get): "loading" | "ready" | "error" => {
  const result = get(plannerAtom);

  return Option.isSome(AsyncResult.value(result))
    ? "ready"
    : AsyncResult.isFailure(result)
      ? "error"
      : "loading";
});

type TripList = {
  readonly subjectId: string | null;
  readonly trips: ReadonlyArray<SavedTrip>;
  readonly conversations: ReadonlyArray<ConversationSummary>;
  readonly timestamp: number;
};

const tripListAtom = Atom.make((get): TripList => {
  const session = get(visibleSessionAtom);

  if (session === null) return { subjectId: null, trips: [], conversations: [], timestamp: 0 };
  const result = get(plannerAtom);

  const success = AsyncResult.isSuccess(result)
    ? result
    : AsyncResult.isFailure(result)
      ? Option.getOrNull(result.previousSuccess)
      : null;

  const previous = Option.getOrNull(get.self<TripList>());

  const current =
    previous?.subjectId === session.subjectId
      ? previous
      : { subjectId: session.subjectId, trips: [], conversations: [], timestamp: 0 };

  return success && success.timestamp >= current.timestamp
    ? {
        subjectId: session.subjectId,
        trips: success.value.trips,
        conversations: success.value.conversations ?? [],
        timestamp: success.timestamp,
      }
    : current;
});

/** Sidebar metadata survives a conversation fetch; conversation messages never cross selections. */
export const savedTripsAtom = Atom.map(tripListAtom, (value) => value.trips);

export const sidebarTripsAtom = Atom.map(tripListAtom, ({ trips, conversations }) => [
  ...conversations
    .filter(
      (conversation) => !trips.some((trip) => trip.conversationId === conversation.conversationId),
    )
    .map((conversation) => ({ ...conversation, id: null, destination: "Planning" })),
  ...trips,
]);

const outboxAtom = Atom.writable<ReadonlyArray<PendingMessage>, ReadonlyArray<PendingMessage>>(
  (get) => {
    get.subscribe(conversationViewAtom, (view) => {
      const snapshot = Option.getOrNull(AsyncResult.value(view.result));

      if (snapshot === null) return;

      const recorded = new Set(
        snapshot.messages.flatMap((message) =>
          message.role === "user" && message.requestId !== undefined ? [message.requestId] : [],
        ),
      );

      const entries = Option.getOrElse(get.self<ReadonlyArray<PendingMessage>>(), () => []);

      const remaining = entries.filter(
        (entry) =>
          entry.subjectId !== view.subjectId ||
          entry.conversationId !== view.conversationId ||
          !recorded.has(entry.id),
      );

      if (remaining.length !== entries.length) get.setSelf(remaining);
    });

    return Option.getOrElse(get.self<ReadonlyArray<PendingMessage>>(), () => []);
  },
  (get, entries) => get.setSelf(entries),
).pipe(Atom.keepAlive);

const unrecordedMessagesAtom = Atom.make((get) => {
  const session = get(visibleSessionAtom);

  if (session === null) return [];
  const { conversationId } = get(selectionAtom);
  const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));

  const recorded = new Set(
    snapshot?.messages.flatMap((message) =>
      message.role === "user" && message.requestId !== undefined ? [message.requestId] : [],
    ) ?? [],
  );

  return get(outboxAtom).filter(
    (entry) =>
      entry.subjectId === session.subjectId &&
      entry.conversationId === conversationId &&
      !recorded.has(entry.id),
  );
});

type ConversationMessage = PlannerSnapshot["messages"][number] & {
  readonly delivery?: PendingMessage["status"];
};

/** Idle sends stay in the transcript until their request IDs appear in the saved history. */
export const messagesAtom = Atom.make((get): ReadonlyArray<ConversationMessage> => {
  if (get(visibleSessionAtom) === null) return [];
  const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));

  const messages: ReadonlyArray<ConversationMessage> = [
    ...(snapshot?.messages.filter((message) => message.id !== "welcome") ?? []),
    ...get(unrecordedMessagesAtom)
      .filter((entry) => entry.placement === "conversation")
      .map((entry): ConversationMessage => ({
        id: entry.id,
        requestId: entry.id,
        role: "user",
        text: entry.text,
        tripId: entry.tripId,
        delivery: entry.status,
      })),
  ];

  const spoken = get(spokenConversationAtom);

  if (
    spoken?.subjectId !== get(visibleSessionAtom)?.subjectId ||
    spoken?.conversationId !== get(selectionAtom).conversationId
  )
    return messages;

  return mergeSpeech(
    messages.map((message) =>
      message.response &&
      !message.content &&
      (spoken.responses.includes(message.id) ||
        (spoken.active && !spoken.baseline.includes(message.id)))
        ? { ...message, supporting: true }
        : message,
    ),
    spoken.messages,
  );
});

/** The server queue survives reload; local entries preserve where each send first appeared. */
export const pendingMessagesAtom = Atom.make((get) => {
  if (get(visibleSessionAtom) === null) return [];
  const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));
  const entries = get(unrecordedMessagesAtom);
  const queued = snapshot?.queuedMessages ?? [];

  return [
    ...queued
      .filter(
        (entry) =>
          !entries.some(
            (local) => local.id === entry.requestId && local.placement === "conversation",
          ),
      )
      .map((entry) => ({
        id: entry.requestId,
        text: entry.text,
        status: "queued" as const,
      })),
    ...entries
      .filter(
        (entry) =>
          entry.placement === "queue" && !queued.some((saved) => saved.requestId === entry.id),
      )
      .map((entry) => ({
        id: entry.id,
        text: entry.text,
        status: entry.status === "accepted" ? ("queued" as const) : entry.status,
      })),
  ];
});

export const activeTripAtom = Atom.make((get) => {
  const selection = get(selectionAtom);

  return get(savedTripsAtom).find((trip) =>
    selection.tripId === null
      ? trip.conversationId === selection.conversationId
      : trip.id === selection.tripId,
  );
});

export const selectTripAtom = Atom.fnSync<{
  readonly conversationId: string;
  readonly id: string | null;
}>()((trip, get) => {
  if (get(selectionAtom).conversationId === trip.conversationId) return;
  get.set(selectionAtom, { conversationId: trip.conversationId, tripId: trip.id });
  get.set(draftAtom, "");
});

export const sendMessageAtom = PlannerClient.runtime.fn<string | void>()(
  Effect.fnUntraced(function* (retryId, get) {
    const session = get(visibleSessionAtom);

    if (session === null) return;
    const selection = get(selectionAtom);

    const retry =
      retryId === undefined
        ? undefined
        : get(outboxAtom).find(
            (entry) =>
              entry.id === retryId &&
              entry.subjectId === session.subjectId &&
              entry.conversationId === selection.conversationId &&
              entry.status === "failed",
          );

    if (retryId !== undefined && retry === undefined) return;
    const message = retry?.text ?? get(draftAtom).trim();

    if (!message) return;
    const conversationId = retry?.conversationId ?? selection.conversationId ?? crypto.randomUUID();
    const selectedTripId = retry ? retry.tripId : (get(activeTripAtom)?.id ?? null);
    const requestId = retry?.id ?? crypto.randomUUID();
    const spoken = get(spokenConversationAtom);

    const voice = retry
      ? retry.voice
      : spoken?.subjectId === session.subjectId &&
          spoken.conversationId === conversationId &&
          spoken.messages.length > 0
        ? { input: false, messages: speechContext(spoken.messages) }
        : undefined;

    if (retry === undefined)
      get.set(latestTypedInputAtom, {
        revision: get(latestTypedInputAtom).revision + 1,
        text: message,
      });
    const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));

    const waiting =
      (snapshot?.pending ?? 0) > 0 ||
      get(unrecordedMessagesAtom).some((entry) => entry.status !== "failed");

    if (selection.conversationId === null)
      get.set(selectionAtom, { conversationId, tripId: selectedTripId });

    const entry: PendingMessage = {
      ...retry,
      subjectId: session.subjectId,
      text: message,
      tripId: selectedTripId,
      conversationId,
      id: requestId,
      placement:
        retry?.placement ??
        (waiting &&
        !(
          spoken?.active &&
          spoken.subjectId === session.subjectId &&
          spoken.conversationId === conversationId
        )
          ? "queue"
          : "conversation"),
      status: "sending",
      ...(voice === undefined ? {} : { voice }),
    };

    get.set(
      outboxAtom,
      retry
        ? get(outboxAtom).map((current) => (current.id === requestId ? entry : current))
        : [...get(outboxAtom), entry],
    );
    if (retry === undefined) get.set(draftAtom, "");

    yield* Effect.gen(function* () {
      const settings = retry?.settings ?? (yield* get.result(settingsResultAtom));
      const liveSession = get(sessionAtom);

      if (!AsyncResult.isSuccess(liveSession) || liveSession.value.subjectId !== session.subjectId)
        return yield* new AccountError({
          code: "unauthorized",
          message: "Sign in to the same account before retrying this message.",
        });
      get.set(
        outboxAtom,
        get(outboxAtom).map((current) =>
          current.id === requestId ? { ...current, settings } : current,
        ),
      );
      const client = yield* PlannerClient;

      const request = {
        message,
        requestId,
        selectedTripId,
        conversationId,
        settings,
        ...(voice === undefined ? {} : { voice }),
      };

      get.set(conversationRequestAtom, request);
      yield* Reactivity.mutation(client("SendMessage", request), ["planner"]);
    }).pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          get.set(
            outboxAtom,
            get(outboxAtom).map((current) =>
              current.id === requestId
                ? { ...current, status: Exit.isSuccess(exit) ? "accepted" : "failed" }
                : current,
            ),
          );
        }),
      ),
    );
  }),
);

export const publishTripAtom = PlannerClient.runtime.fn<PublishTripRequest>()(
  Effect.fnUntraced(function* (payload) {
    const client = yield* PlannerClient;

    return yield* Reactivity.mutation(client("PublishTrip", payload), ["planner"]);
  }),
);

export const changeTripAppAtom = PlannerClient.runtime.fn<
  | { readonly action: "create" | "retry"; readonly tripId: string }
  | { readonly action: "restore"; readonly tripId: string; readonly commitId: string }
>()(
  Effect.fnUntraced(function* (request) {
    const client = yield* PlannerClient;

    return yield* Reactivity.mutation(
      request.action === "restore"
        ? client("RestoreTripApp", { tripId: request.tripId, commitId: request.commitId })
        : request.action === "retry"
          ? client("RetryTripAppBuild", { tripId: request.tripId })
          : client("CreateTripApp", { tripId: request.tripId }),
      ["planner"],
    );
  }),
);
