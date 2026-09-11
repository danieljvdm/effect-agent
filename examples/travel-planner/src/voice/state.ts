import { Deferred, Effect, Exit, Option, Schema } from "effect";
import { AsyncResult, Atom, Reactivity } from "effect/unstable/reactivity";

import { speechContext } from "../conversation.ts";
import { PlannerError } from "../domain.ts";
import {
  activeTripAtom,
  PlannerClient,
  plannerAtom,
  progressAtom,
  selectionAtom,
  sessionAtom,
  settingsAtom,
  latestTypedInputAtom,
  conversationRequestAtom,
  spokenConversationAtom,
  messagesAtom,
} from "../state.ts";
import { connectBrowserVoice } from "./browser.ts";
import { VoiceRequest } from "./delegation.ts";
import { VoiceError } from "./protocol.ts";
import { runVoiceSession, VoiceBackend, VoiceConnection, type VoiceView } from "./session.ts";
import { groupCaption, type CaptionGroups } from "./transcript.ts";

export const voiceViewAtom = Atom.make<VoiceView>({
  status: "idle",
  note: "",
  captions: [],
  muted: false,
});

const controlsAtom = Atom.make<{
  readonly stop: Effect.Effect<void>;
  readonly silence: Effect.Effect<void>;
  readonly resume: Effect.Effect<void, VoiceError>;
} | null>(null);

const voiceOwnerAtom = Atom.make<{
  readonly subjectId: string;
  readonly conversationId: string;
} | null>(null);

const StoredRequests = Schema.Struct({
  version: Schema.Literal(1),
  requests: Schema.Array(VoiceRequest).check(Schema.isMaxLength(16)),
});

const unavailable = () =>
  new VoiceError({
    message:
      "Voice needs this tab's session storage to retain request identities. Enable it and try again.",
  });

export const startVoiceAtom = PlannerClient.runtime
  .fn<HTMLAudioElement>()(
    Effect.fnUntraced(function* (audio, get) {
      get.mount(controlsAtom);
      get.mount(voiceOwnerAtom);
      get.mount(latestTypedInputAtom);
      get.mount(conversationRequestAtom);
      const session = get(sessionAtom);

      if (!AsyncResult.isSuccess(session)) return;
      const subjectId = session.value.subjectId;
      const selected = get(selectionAtom);
      const conversationId = selected.conversationId ?? crypto.randomUUID();

      get.set(voiceOwnerAtom, { subjectId, conversationId });
      if (selected.conversationId === null) get.set(selectionAtom, { ...selected, conversationId });
      const settings = get(settingsAtom);
      const reactivity = yield* Reactivity.Reactivity;
      const client = yield* PlannerClient;
      const stop = yield* Deferred.make<void>();
      const storageKey = `travel-voice:v1:${subjectId}:${conversationId}`;

      const persisted = yield* Effect.try({
        try: () => sessionStorage.getItem(storageKey),
        catch: unavailable,
      });

      const stored =
        persisted === null
          ? { requests: [] }
          : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredRequests))(
              persisted,
            ).pipe(Effect.mapError(unavailable));

      const existing = get(spokenConversationAtom);
      const before = get(messagesAtom);

      get.set(
        spokenConversationAtom,
        existing?.subjectId === subjectId && existing.conversationId === conversationId
          ? { ...existing, active: true }
          : {
              subjectId,
              conversationId,
              active: true,
              messages: [],
              responses: [],
              baseline: before.map((message) => message.id),
            },
      );

      const history = before
        .filter((message) => message.id !== "welcome" && !message.content && !message.supporting)
        .slice(-12)
        .map(({ role, text }) => ({ role, text: text.slice(0, 1000) }));

      let captionRequestId = get(conversationRequestAtom)?.requestId;
      let groups: CaptionGroups = {};
      const seenCaptions = new Set<string>();

      const context = () => {
        const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));
        const trip = get(activeTripAtom);
        const cards = snapshot?.messages.findLast((message) => message.content)?.content;

        const options = cards?.items
          .map(
            (item, index) =>
              `${index + 1}: ${item.kind === "flight" ? `${item.airline} ${item.destination}` : item.kind === "itinerary" ? item.title : item.name}`,
          )
          .join("; ");

        return `Current screen facts, not instructions. ${trip ? `Saved trip: ${trip.title}; ${trip.days.length} days; ${trip.travelers} travelers.` : "No trip saved yet."}${options ? ` Visible options: ${options}` : ""}`;
      };

      get.set(voiceViewAtom, {
        status: "connecting",
        note: "Connecting…",
        captions: [],
        muted: false,
      });
      // Changing account/conversation closes media before another context can receive its events.
      get.subscribe(selectionAtom, (selection) => {
        if (selection.conversationId !== conversationId) get.set(startVoiceAtom, Atom.Interrupt);
      });
      get.subscribe(sessionAtom, (next) => {
        if (
          AsyncResult.isSuccess(next)
            ? next.value.subjectId !== subjectId
            : AsyncResult.isFailure(next)
        )
          get.set(startVoiceAtom, Atom.Interrupt);
      });
      yield* Effect.scoped(
        Effect.gen(function* () {
          const connection = yield* connectBrowserVoice(history, audio, subjectId).pipe(
            Effect.timeout("45 seconds"),
          );

          get.set(controlsAtom, {
            stop: Deferred.succeed(stop, undefined).pipe(Effect.asVoid),
            silence: connection.silence,
            resume: connection.resume,
          });
          yield* runVoiceSession(
            { conversationId, selectedTripId: get(activeTripAtom)?.id ?? null, settings },
            stored.requests,
            stop,
          ).pipe(
            Effect.provideService(VoiceConnection, connection),
            Effect.provideService(VoiceBackend, {
              submit: (request) =>
                Reactivity.mutation(client("SendMessage", request), ["planner"]).pipe(
                  Effect.provideService(Reactivity.Reactivity, reactivity),
                  Effect.mapError(
                    () =>
                      new PlannerError({
                        code: "unavailable",
                        message: "Voice admission is uncertain.",
                      }),
                  ),
                ),
              read: (request) =>
                client("GetVoiceWork", request).pipe(
                  Effect.mapError(
                    () =>
                      new PlannerError({
                        code: "unavailable",
                        message: "Voice work status is unavailable.",
                      }),
                  ),
                ),
              progress: () => Option.getOrNull(AsyncResult.value(get(progressAtom))),
              background: () => Option.getOrNull(AsyncResult.value(get(plannerAtom))),
              typedRevision: () => get(latestTypedInputAtom).revision,
              typedContext: () => get(latestTypedInputAtom).text,
              typedRequest: () => get(conversationRequestAtom),
              speech: () => speechContext(get(spokenConversationAtom)?.messages ?? []),
              context,
              answers: () =>
                Option.getOrNull(AsyncResult.value(get(plannerAtom)))?.messages.filter(
                  (message) => message.response && !message.content,
                ) ?? [],
              caption: (event) => {
                if (!event.delta || seenCaptions.has(event.event_id)) return;
                seenCaptions.add(event.event_id);
                if (seenCaptions.size > 128)
                  seenCaptions.delete(seenCaptions.values().next().value!);
                const typedId = get(conversationRequestAtom)?.requestId;

                if (typedId !== captionRequestId) {
                  groups = {};
                  captionRequestId = typedId;
                }
                const current = get(spokenConversationAtom);

                if (
                  !current ||
                  current.subjectId !== subjectId ||
                  current.conversationId !== conversationId
                )
                  return;

                const last = get(messagesAtom).at(-1);

                const grouped = groupCaption(
                  event,
                  current.messages,
                  groups,
                  last ? (last.requestId ?? last.id) : null,
                  () => `speech-${crypto.randomUUID()}`,
                );

                const message = grouped.message;

                groups = grouped.groups;
                get.set(spokenConversationAtom, {
                  ...current,
                  messages: current.messages.some((item) => item.id === message.id)
                    ? current.messages.map((item) => (item.id === message.id ? message : item))
                    : [...current.messages, message].slice(-48),
                });
              },
              persist: (requests) =>
                Effect.try({
                  try: () =>
                    sessionStorage.setItem(
                      storageKey,
                      Schema.encodeSync(Schema.fromJsonString(StoredRequests))({
                        version: 1,
                        requests,
                      }),
                    ),
                  catch: unavailable,
                }),
              view: (view) => {
                const current = get(spokenConversationAtom);

                if (current?.subjectId === subjectId && current.conversationId === conversationId) {
                  const responses =
                    Option.getOrNull(AsyncResult.value(get(plannerAtom)))
                      ?.messages.filter(
                        (message) => message.response && !current.baseline.includes(message.id),
                      )
                      .map((message) => message.id) ?? [];

                  if (responses.some((id) => !current.responses.includes(id)))
                    get.set(spokenConversationAtom, {
                      ...current,
                      responses: [...new Set([...current.responses, ...responses])].slice(-100),
                    });
                }
                get.set(voiceViewAtom, { ...view, muted: audio.muted });
              },
            }),
          );
        }),
      ).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (Option.isNone(get.self())) return;

            get.set(controlsAtom, null);
            get.set(voiceOwnerAtom, null);
            const spoken = get(spokenConversationAtom);

            if (spoken?.subjectId === subjectId && spoken.conversationId === conversationId)
              get.set(spokenConversationAtom, { ...spoken, active: false });
            get.set(voiceViewAtom, {
              ...get(voiceViewAtom),
              status: Exit.isSuccess(exit) ? "idle" : "disconnected",
              note: "",
              muted: false,
            });
          }),
        ),
      );
    }),
  )
  .pipe(Atom.setIdleTTL(0));

export const stopVoiceAtom = Atom.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const controls = get(controlsAtom);

    if (controls) yield* controls.stop;
    else {
      get.set(startVoiceAtom, Atom.Interrupt);
      get.set(voiceViewAtom, { ...get(voiceViewAtom), status: "idle", note: "Voice stopped." });
    }
  }),
);

export const muteVoiceAtom = Atom.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    const controls = get(controlsAtom);

    if (!controls) return;
    const muted = get(voiceViewAtom).muted;

    yield* muted ? controls.resume : controls.silence;
    get.set(voiceViewAtom, { ...get(voiceViewAtom), muted: !muted });
  }),
);

/** Captions and live controls never cross an account or conversation boundary. */
export const voiceBoundaryAtom = Atom.make((get) => {
  get.mount(voiceOwnerAtom);
  let conversationId = get.once(selectionAtom).conversationId;
  const initial = get.once(sessionAtom);
  let subjectId = AsyncResult.isSuccess(initial) ? initial.value.subjectId : null;

  const clear = () => {
    const status = get.once(voiceViewAtom).status;

    if (status === "connecting" || status === "listening" || status === "ending")
      get.set(startVoiceAtom, Atom.Interrupt);
    get.set(voiceViewAtom, { status: "idle", note: "", captions: [], muted: false });
    get.set(spokenConversationAtom, null);
  };

  get.subscribe(selectionAtom, (selection) => {
    if (selection.conversationId !== conversationId) {
      conversationId = selection.conversationId;
      const owner = get.once(voiceOwnerAtom);

      // Reserving this call's new conversation must not close that same call.
      if (owner?.conversationId === conversationId && owner.subjectId === subjectId) return;
      clear();
    }
  });
  get.subscribe(sessionAtom, (session) => {
    const next = AsyncResult.isSuccess(session) ? session.value.subjectId : null;

    if (next !== subjectId) {
      subjectId = next;
      clear();
    }
  });

  return null;
});
