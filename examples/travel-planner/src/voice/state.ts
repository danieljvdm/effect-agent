import { Deferred, Effect, Exit, Option, Schema } from "effect";
import { AsyncResult, Atom, Reactivity } from "effect/unstable/reactivity";

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
} from "../state.ts";
import { connectBrowserVoice } from "./browser.ts";
import { VoiceRequest } from "./delegation.ts";
import { VoiceError } from "./protocol.ts";
import { runVoiceSession, type VoiceView } from "./session.ts";

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
  readonly email: string;
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

export const startVoiceAtom = PlannerClient.runtime.fn<HTMLAudioElement>()(
  Effect.fnUntraced(function* (audio, get) {
    get.mount(controlsAtom);
    get.mount(voiceOwnerAtom);
    get.mount(latestTypedInputAtom);
    const session = get(sessionAtom);

    if (!AsyncResult.isSuccess(session)) return;
    const email = session.value.email;
    const selected = get(selectionAtom);
    const conversationId = selected.conversationId ?? crypto.randomUUID();

    get.set(voiceOwnerAtom, { email, conversationId });
    if (selected.conversationId === null) get.set(selectionAtom, { ...selected, conversationId });
    const settings = get(settingsAtom);
    const reactivity = yield* Reactivity.Reactivity;
    const client = yield* PlannerClient;
    const stop = yield* Deferred.make<void>();
    const storageKey = `travel-voice:v1:${email}:${conversationId}`;

    const persisted = yield* Effect.try({
      try: () => sessionStorage.getItem(storageKey),
      catch: unavailable,
    });

    const stored =
      persisted === null
        ? { requests: [] }
        : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredRequests))(persisted).pipe(
            Effect.mapError(unavailable),
          );

    const snapshot = Option.getOrNull(AsyncResult.value(get(plannerAtom)));

    const history =
      snapshot?.messages
        .filter((message) => message.id !== "welcome" && !message.content)
        .slice(-12)
        .map(({ role, text }) => ({ role, text: text.slice(0, 1000) })) ?? [];

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
      if (AsyncResult.isSuccess(next) ? next.value.email !== email : AsyncResult.isFailure(next))
        get.set(startVoiceAtom, Atom.Interrupt);
    });
    yield* Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* connectBrowserVoice(history, audio).pipe(
          Effect.timeout("45 seconds"),
        );

        get.set(controlsAtom, {
          stop: Deferred.succeed(stop, undefined).pipe(Effect.asVoid),
          silence: connection.silence,
          resume: connection.resume,
        });
        yield* runVoiceSession(
          connection,
          {
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
            typedRevision: () => get(latestTypedInputAtom).revision,
            typedContext: () => get(latestTypedInputAtom).text,
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
            view: (view) => get.set(voiceViewAtom, { ...view, muted: audio.muted }),
          },
          { conversationId, selectedTripId: get(activeTripAtom)?.id ?? null, settings },
          stored.requests,
          stop,
        );
      }),
    ).pipe(
      Effect.onExit((exit) =>
        Effect.sync(() => {
          get.set(controlsAtom, null);
          get.set(voiceOwnerAtom, null);
          get.set(voiceViewAtom, {
            ...get(voiceViewAtom),
            status: Exit.isSuccess(exit) ? "idle" : "disconnected",
            note: "Voice ended. Accepted planner work continues. You can reconnect or type a follow-up.",
            muted: false,
          });
        }),
      ),
    );
  }),
);

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
  let email = AsyncResult.isSuccess(initial) ? initial.value.email : null;

  const clear = () => {
    const status = get.once(voiceViewAtom).status;

    if (status === "connecting" || status === "listening" || status === "ending")
      get.set(startVoiceAtom, Atom.Interrupt);
    get.set(voiceViewAtom, { status: "idle", note: "", captions: [], muted: false });
  };

  get.subscribe(selectionAtom, (selection) => {
    if (selection.conversationId !== conversationId) {
      conversationId = selection.conversationId;
      const owner = get.once(voiceOwnerAtom);

      // Reserving this call's new conversation must not close that same call.
      if (owner?.conversationId === conversationId && owner.email === email) return;
      clear();
    }
  });
  get.subscribe(sessionAtom, (session) => {
    const next = AsyncResult.isSuccess(session) ? session.value.email : null;

    if (next !== email) {
      email = next;
      clear();
    }
  });

  return null;
});
