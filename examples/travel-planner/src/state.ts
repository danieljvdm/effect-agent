import { Data, Effect, Exit, Layer, Option, Schedule, Schema, Semaphore, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { AsyncResult, Atom, AtomRpc, Reactivity } from "effect/unstable/reactivity";
import { RpcClient, RpcSerialization } from "effect/unstable/rpc";

import {
  AccessError,
  AccessRpcs,
  Email,
  type AccessMembers,
  type AccessSession,
} from "./access-domain";
import {
  PlannerRpcs,
  ProgressRpcs,
  PlannerSettings,
  defaultPlannerSettings,
  type PlannerProgress,
  type PublishTripRequest,
  type SavedTrip,
  type ConversationSummary,
} from "./domain";

export class AccessClient extends AtomRpc.Service<AccessClient>()("travel-planner/AccessClient", {
  group: AccessRpcs,
  protocol: RpcClient.layerProtocolHttp({ url: "/api/access" }).pipe(
    Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]),
  ),
}) {}

export const sessionAtom = AccessClient.query("GetSession", undefined);

// A client reconnect must not blank data already shown to this verified account.
// Mutations still require the live session; an explicit rejection clears the view.
const visibleSessionAtom = Atom.make((get): AccessSession | null => {
  const result = get(sessionAtom);

  if (AsyncResult.isSuccess(result)) return result.value;
  if (AsyncResult.isFailure(result)) {
    const error = Option.getOrNull(AsyncResult.error(result));

    if (error?._tag === "AccessError" && error.code !== "unavailable") return null;
  }

  return Option.getOrNull(get.self<AccessSession | null>());
});

const membersQuery = AccessClient.query("GetMembers", undefined, {
  reactivityKeys: ["access-members"],
});

export const membersAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  return AsyncResult.isSuccess(session) && session.value.isAdmin
    ? get(membersQuery)
    : AsyncResult.initial<AccessMembers>();
});

export const memberEmailAtom = Atom.make("");

export const manageMemberAtom = AccessClient.runtime.fn<
  { readonly action: "invite" } | { readonly action: "remove"; readonly email: string }
>()(
  Effect.fnUntraced(function* (request, get) {
    const draft = get(memberEmailAtom);

    const email = yield* Schema.decodeUnknownEffect(Email)(
      (request.action === "invite" ? draft : request.email).trim().toLowerCase(),
    );

    const client = yield* AccessClient;

    yield* Reactivity.mutation(
      request.action === "invite"
        ? client("InviteMember", { email })
        : client("RemoveMember", { email }),
      ["access-members"],
    );
    if (request.action === "invite" && get(memberEmailAtom) === draft) get.set(memberEmailAtom, "");

    return request.action === "invite"
      ? `${email} can now sign in. Share this site's address with them.`
      : `Access removed for ${email}.`;
  }),
);

export const refreshMembersAtom = Atom.fnSync<void>()((_, get) => {
  if (get(manageMemberAtom).waiting) return;
  get.set(manageMemberAtom, Atom.Reset);
  get.refresh(membersQuery);
});

export const selectionAtom = Atom.make<{
  readonly conversationId: string | null;
  readonly tripId: string | null;
}>({ conversationId: null, tripId: null });

export const draftAtom = Atom.make("");

const pendingRequestAtom = Atom.make<{
  readonly text: string;
  readonly tripId: string | null;
  readonly conversationId: string;
  readonly id: string;
  readonly settings: PlannerSettings;
} | null>(null);

export class PlannerClient extends AtomRpc.Service<PlannerClient>()("travel-planner/Client", {
  group: PlannerRpcs,
  protocol: RpcClient.layerProtocolHttp({ url: "/api/rpc" }).pipe(
    Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]),
  ),
}) {}

type AccountPreferences = {
  readonly settings: PlannerSettings | null;
  readonly generation: number;
  readonly saving: boolean;
  readonly saveFailed: boolean;
};

const accountPreferences = Atom.family((_email: string) =>
  Atom.make<AccountPreferences>({
    settings: null,
    generation: 0,
    saving: false,
    saveFailed: false,
  }).pipe(Atom.keepAlive),
);

const preferencesWriteLock = Atom.make(Semaphore.make(1)).pipe(Atom.keepAlive);

const preferencesQuery = Atom.family((email: string) =>
  PlannerClient.runtime
    .atom((get) =>
      Effect.gen(function* () {
        const session = get(sessionAtom);

        if (!AsyncResult.isSuccess(session) || session.waiting || session.value.email !== email)
          return yield* Effect.interrupt;
        const state = accountPreferences(email);
        const generation = get.once(state).generation;
        const client = yield* PlannerClient;
        const settings = yield* client("GetPlannerSettings", undefined);
        const latest = get.once(state);
        const verified = get.once(sessionAtom);

        if (
          latest.generation === generation &&
          AsyncResult.isSuccess(verified) &&
          !verified.waiting &&
          verified.value.email === email
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
  const query = get(preferencesQuery(session.value.email));
  const state = get(accountPreferences(session.value.email));

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
  const query = get(preferencesQuery(session.value.email));
  const state = get(accountPreferences(session.value.email));

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
      return yield* new AccessError({
        code: "unauthorized",
        message: "Sign in before choosing model settings.",
      });
    const email = session.value.email;
    const lock = yield* get.result(preferencesWriteLock);

    return yield* lock.withPermits(1)(
      Effect.gen(function* () {
        const verified = get(sessionAtom);

        if (!AsyncResult.isSuccess(verified) || verified.waiting || verified.value.email !== email)
          return yield* Effect.interrupt;
        const current = yield* get.result(settingsResultAtom);
        const hydratedSession = get(sessionAtom);

        if (
          !AsyncResult.isSuccess(hydratedSession) ||
          hydratedSession.waiting ||
          hydratedSession.value.email !== email
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
        const state = accountPreferences(email);
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
    group: ProgressRpcs,
    protocol: RpcClient.layerProtocolHttp({ url: "/api/progress" }).pipe(
      Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]),
    ),
  },
) {}

class ConversationKey<Id extends string | null> extends Data.Class<{
  readonly email: string;
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
    ? get(progressStream(new ConversationKey({ email: session.value.email, conversationId })))
    : AsyncResult.initial<PlannerProgress>();
});

// Retain only the query, not its polling wrapper: switching trips stops that
// conversation's timer but preserves its history for return visits in this tab.
// Each verified identity has separate nodes, without putting emails on the wire.
// Use one structural key: nested family functions can be collected independently
// of their mounted atoms because Atom.family holds its values through WeakRef.
const snapshotQuery = Atom.family(({ email, conversationId }: ConversationKey<string | null>) =>
  PlannerClient.runtime
    .atom((get) =>
      Effect.gen(function* () {
        const session = yield* get.result(sessionAtom, { suspendOnWaiting: true });

        // A retained inactive query must not refetch using a different user's cookie.
        if (session.email !== email) return yield* Effect.interrupt;
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
  readonly email: string | null;
  readonly conversationId: string | null;
  readonly result: Atom.Type<ReturnType<typeof snapshotQuery>>;
};

const conversationViewAtom = Atom.make((get): ConversationView => {
  const session = get(visibleSessionAtom);
  const conversationId = get(selectionAtom).conversationId;

  if (session === null) return { email: null, conversationId, result: AsyncResult.initial() };
  const result = get(polledSnapshot(new ConversationKey({ email: session.email, conversationId })));
  const previous = Option.getOrNull(get.self<ConversationView>());

  const sameConversation =
    previous?.email === session.email && previous.conversationId === conversationId;

  // Only a new conversation needs an initial loading screen. Runtime rebuilds
  // and overlapping refreshes preserve the last view for this exact identity.
  return {
    email: session.email,
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
  readonly email: string | null;
  readonly trips: ReadonlyArray<SavedTrip>;
  readonly conversations: ReadonlyArray<ConversationSummary>;
  readonly timestamp: number;
};

const tripListAtom = Atom.make((get): TripList => {
  const session = get(visibleSessionAtom);

  if (session === null) return { email: null, trips: [], conversations: [], timestamp: 0 };
  const result = get(plannerAtom);

  const success = AsyncResult.isSuccess(result)
    ? result
    : AsyncResult.isFailure(result)
      ? Option.getOrNull(result.previousSuccess)
      : null;

  const previous = Option.getOrNull(get.self<TripList>());

  const current =
    previous?.email === session.email
      ? previous
      : { email: session.email, trips: [], conversations: [], timestamp: 0 };

  return success && success.timestamp >= current.timestamp
    ? {
        email: session.email,
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

export const activeTripAtom = Atom.make((get) => {
  const selection = get(selectionAtom);

  return get(savedTripsAtom).find((trip) =>
    selection.tripId === null
      ? trip.conversationId === selection.conversationId
      : trip.id === selection.tripId,
  );
});

export const newTripAtom = Atom.fnSync<void>()((_, get) => {
  get.set(selectionAtom, { conversationId: crypto.randomUUID(), tripId: null });
  get.set(draftAtom, "");
  get.set(pendingRequestAtom, null);
});

export const selectTripAtom = Atom.fnSync<{
  readonly conversationId: string;
  readonly id: string | null;
}>()((trip, get) => {
  get.set(selectionAtom, { conversationId: trip.conversationId, tripId: trip.id });
  get.set(draftAtom, "");
  get.set(pendingRequestAtom, null);
});

export const sendMessageAtom = PlannerClient.runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const draft = get(draftAtom);
    const message = draft.trim();
    const selection = get(selectionAtom);
    const conversationId = selection.conversationId ?? crypto.randomUUID();
    const selectedTripId = get(activeTripAtom)?.id ?? null;

    if (!message) return;
    if (selection.conversationId === null)
      get.set(selectionAtom, { conversationId, tripId: selectedTripId });
    const previous = get(pendingRequestAtom);

    const retrying =
      previous?.text === message &&
      previous.tripId === selectedTripId &&
      previous.conversationId === conversationId;

    const requestId = retrying ? previous.id : crypto.randomUUID();
    const settings = retrying ? previous.settings : yield* get.result(settingsResultAtom);

    get.set(pendingRequestAtom, {
      text: message,
      tripId: selectedTripId,
      conversationId,
      id: requestId,
      settings,
    });
    const client = yield* PlannerClient;

    yield* Reactivity.mutation(
      client("SendMessage", {
        message,
        requestId,
        selectedTripId,
        conversationId,
        settings,
      }),
      ["planner"],
    );
    if (get(selectionAtom).conversationId === conversationId && get(draftAtom) === draft)
      get.set(draftAtom, "");
    if (get(pendingRequestAtom)?.id === requestId) get.set(pendingRequestAtom, null);
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
