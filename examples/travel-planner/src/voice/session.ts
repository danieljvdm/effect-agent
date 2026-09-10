import { Clock, Deferred, Effect, Schema, Stream } from "effect";

import {
  SendMessageRequest,
  type PlannerError,
  type PlannerProgress,
  type VoiceWork,
  type VoiceWorkRequest,
  type SpokenMessage,
  type PlannerSnapshot,
} from "../domain.ts";
import type { VoiceConnection } from "./browser.ts";
import {
  appendCaption,
  delegationMessage,
  isCaption,
  voiceUpdate,
  voiceActivity,
  type VoiceRequest,
} from "./delegation.ts";
import { shortContext, VoiceError, type Caption } from "./protocol.ts";

export interface VoiceView {
  readonly status: "idle" | "connecting" | "listening" | "ending" | "disconnected";
  readonly note: string;
  readonly captions: ReadonlyArray<Caption>;
  readonly muted: boolean;
}

export interface VoiceBackend {
  readonly submit: (request: SendMessageRequest) => Effect.Effect<unknown, PlannerError>;
  readonly read: (request: typeof VoiceWorkRequest.Type) => Effect.Effect<VoiceWork, PlannerError>;
  readonly progress: () => PlannerProgress | null;
  readonly background: () => Pick<PlannerSnapshot, "scouts" | "editor"> | null;
  readonly typedRevision: () => number;
  readonly typedContext: () => string;
  readonly typedRequest: () => SendMessageRequest | null;
  readonly speech: () => ReadonlyArray<SpokenMessage>;
  readonly caption: (caption: Caption) => void;
  readonly context: () => string;
  readonly answers: () => ReadonlyArray<{ readonly id: string; readonly text: string }>;
  readonly persist: (requests: ReadonlyArray<VoiceRequest>) => Effect.Effect<void, VoiceError>;
  readonly view: (view: VoiceView) => void;
}

/** No audio lifecycle operation owns the admitted request's lifetime. */
export const runVoiceSession = Effect.fn("runVoiceSession")(function* (
  connection: VoiceConnection,
  backend: VoiceBackend,
  envelope: Omit<SendMessageRequest, "message" | "requestId">,
  retained: ReadonlyArray<VoiceRequest>,
  stop: Deferred.Deferred<void>,
) {
  const ready = yield* Deferred.make<void>();
  const closed = yield* Deferred.make<void>();
  let sessionId = "";
  let captions: ReadonlyArray<Caption> = [];
  let requests = [...retained];
  let latest = requests.at(-1);
  let stale = false;
  let typedRevision = backend.typedRevision();
  let lastUpdate = "";
  let lastProgressAt = 0;
  let lastDelegatedOffset = -1;
  let lastUserAt = -Infinity;
  let lastSpeechAt = yield* Clock.currentTimeMillis;
  let lastCommentaryAt = lastSpeechAt;
  let lastActivity = "";
  let typedRequestId = backend.typedRequest()?.requestId;
  let lastContext = "";
  let seenAnswers = new Set(backend.answers().map((answer) => answer.id));
  let status: VoiceView["status"] = "connecting";
  let note = "Connecting…";
  let pending: { id: string; offset: number; at: number } | undefined;
  let append: { id: string; at: number } | undefined;
  let typedContext: string | undefined;
  let pauseForTyped = false;
  const knownDelegations = new Set<string>();
  const render = () => backend.view({ status, note, captions, muted: false });

  const replace = Effect.fn("voice.replace")(function* (next: VoiceRequest) {
    requests = [
      ...requests.filter((item) => item.request.requestId !== next.request.requestId),
      next,
    ].slice(-16);
    if (latest?.request.requestId === next.request.requestId) latest = next;
    yield* backend.persist(requests);
  });

  const sendContext = Effect.fn("voice.context")(function* (
    type: "session.thinking.append" | "session.commentary.append" | "session.instructions.append",
    text: string,
    delegation: string | null,
  ) {
    const id = crypto.randomUUID();

    append = { id, at: yield* Clock.currentTimeMillis };
    yield* connection.send({
      type,
      event_id: id,
      delegation_id: delegation,
      content: shortContext(text),
    });
  });

  const receive = connection.events.pipe(
    Stream.runForEach((event) =>
      Effect.gen(function* () {
        if (event.type === "session.started") {
          sessionId = event.session.id;
          status = "listening";
          note = "Listening";
          yield* Deferred.succeed(ready, undefined);
        } else if (event.type === "session.closed") {
          yield* Deferred.succeed(closed, undefined);
        } else if (isCaption(event)) {
          captions = appendCaption(captions, event);
          backend.caption(event);
          lastSpeechAt = yield* Clock.currentTimeMillis;
          if (event.type === "session.input_transcript.delta") lastUserAt = lastSpeechAt;
        } else if (event.type === "session.delegation.created") {
          if (event.offset_ms <= lastDelegatedOffset) return;
          if (!knownDelegations.has(event.delegation.id)) {
            if (knownDelegations.size >= 64)
              return yield* new VoiceError({
                message: "This call reached its task limit. End voice and reconnect.",
              });
            knownDelegations.add(event.delegation.id);
            // Coalesce metadata arriving before its transcripts, without treating a gap as a turn.
            pending = {
              id: event.delegation.id,
              offset: event.offset_ms,
              at: yield* Clock.currentTimeMillis,
            };
            stale = true;
          }
        } else if (event.type === "error") {
          // Never display arbitrary provider bodies or label rejected output as spoken.
          return yield* new VoiceError({
            message: "The voice provider rejected an update. Reconnect to check existing work.",
          });
        } else if (event.client_event_id === append?.id) {
          append = undefined;
          note = "Listening";
        }
        render();
      }),
    ),
  );

  const tick = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;

    if (typedRevision !== backend.typedRevision()) {
      typedRevision = backend.typedRevision();
      stale = true;
      pending = undefined;
      lastDelegatedOffset = captions.at(-1)?.end_ms ?? lastDelegatedOffset;
      typedContext = `New typed user input (reference data): ${JSON.stringify(backend.typedContext())}. This request is already being handled. Continue from its updated result.`;
      pauseForTyped = true;
      note = "Updating your trip…";
      seenAnswers = new Set(backend.answers().map((answer) => answer.id));
    }
    const typedRequest = backend.typedRequest();

    if (
      typedRequest &&
      typedRequest.conversationId === envelope.conversationId &&
      typedRequest.requestId !== typedRequestId
    ) {
      typedRequestId = typedRequest.requestId;
      latest = {
        request: typedRequest,
        sessionId,
        delegationId: "",
        offset: lastDelegatedOffset,
        status: "accepted",
        receipt: null,
      };
      stale = false;
      lastUpdate = "";
      lastActivity = "";
      lastCommentaryAt = now;
      yield* replace(latest);
    }
    if (append && now - append.at > 15_000)
      return yield* new VoiceError({
        message: "Voice did not acknowledge an update. Reconnect to check existing work.",
      });
    if (pauseForTyped && !append) {
      yield* sendContext(
        "session.instructions.append",
        "Pause the current explanation. The user has typed an update in this same conversation. Read the next context update and continue naturally; its work is already being handled, so do not delegate it again.",
        null,
      );
      pauseForTyped = false;
    } else if (typedContext && !append) {
      yield* sendContext("session.thinking.append", typedContext, null);
      typedContext = undefined;
    }
    if (pending && now - pending.at >= 400) {
      const delegation = pending;
      const message = delegationMessage(captions, delegation.offset, lastDelegatedOffset);

      if (message !== null) {
        pending = undefined;

        const request = yield* Schema.decodeUnknownEffect(SendMessageRequest)({
          ...envelope,
          message,
          voice: { input: true, messages: backend.speech() },
          requestId: crypto.randomUUID(),
        }).pipe(
          Effect.mapError(
            () =>
              new VoiceError({
                message: "The spoken request is too long. Please send a shorter request.",
              }),
          ),
        );

        latest = {
          request,
          sessionId,
          delegationId: delegation.id,
          offset: delegation.offset,
          status: "prepared",
          receipt: null,
        };
        stale = false;
        lastDelegatedOffset = delegation.offset;
        lastUpdate = "";
        lastActivity = "";
        lastCommentaryAt = now;
        seenAnswers = new Set(backend.answers().map((answer) => answer.id));
        // Freeze before the first network call. A lost acknowledgement retains identical input/settings.
        yield* replace(latest);
      } else if (now - delegation.at > 3_000 && !append) {
        pending = undefined;
        yield* sendContext(
          "session.commentary.append",
          "I did not receive enough of the spoken request. Please repeat it or type it.",
          delegation.id,
        );
      }
    }
    const context = backend.context();

    if (context && context !== lastContext && !append && !typedContext && !pending) {
      yield* sendContext("session.thinking.append", context, null);
      lastContext = context;
    }
    const current = latest;

    if (!current || stale || pending) {
      render();

      return;
    }
    if (current.status === "prepared") {
      // One initial send per frozen envelope. Reconnect first reconciles before retrying it.
      const admitted = yield* Effect.result(
        backend.submit(current.request).pipe(Effect.timeout("20 seconds")),
      );

      const accepted = {
        ...current,
        status: admitted._tag === "Success" ? ("accepted" as const) : ("uncertain" as const),
      };

      yield* replace(accepted);
      note = accepted.status === "accepted" ? "Working on your trip…" : "Checking your request…";
    }

    const observed = yield* Effect.result(
      backend.read(current.request).pipe(Effect.timeout("10 seconds")),
    );

    if (observed._tag === "Failure") {
      note = "Reconnecting to your trip…";
      render();

      return;
    }
    const work = observed.success;

    if (
      latest?.request.requestId !== current.request.requestId ||
      stale ||
      pending ||
      typedRevision !== backend.typedRevision()
    )
      return;
    if (work.state === "missing") {
      render();

      return;
    }
    yield* replace({
      ...latest,
      receipt: work,
      status: work.state === "pending" ? "accepted" : "settled",
    });
    const answers = backend.answers();
    const answer = answers.findLast((item) => !seenAnswers.has(item.id));

    const update =
      !work.superseded && work.state === "completed" && answer && answer.text !== work.text
        ? { kind: "result" as const, key: `answer:${answer.id}`, text: shortContext(answer.text) }
        : voiceUpdate(work, backend.progress());

    const activity = voiceActivity(work, backend.progress(), backend.background());

    const activityDue =
      activity !== null &&
      now - lastSpeechAt >= 10_000 &&
      now - lastCommentaryAt >= (activity === lastActivity ? 30_000 : 12_000);

    if (
      update &&
      now - lastUserAt >= 1500 &&
      !append &&
      update.key !== lastUpdate &&
      (update.kind === "result" || !activityDue) &&
      (update.kind === "result" || now - lastProgressAt >= 3000)
    ) {
      const delegation =
        current.sessionId === sessionId && current.delegationId ? current.delegationId : null;

      yield* sendContext(
        update.kind === "result" ? "session.commentary.append" : "session.thinking.append",
        update.text,
        delegation,
      );
      lastUpdate = update.key;
      if (update.kind === "result") {
        lastCommentaryAt = now;
        for (const answer of answers) seenAnswers.add(answer.id);
        // A later research answer continues the exchange; don't then repeat the earlier receipt answer.
        if (update.key.startsWith("answer:")) lastUpdate = `settled:${work.receiptId}`;
      }
      lastProgressAt = now;
      note = update.kind === "result" ? "Listening" : "Working on your trip…";
    }
    if (activity && activityDue && !append && !typedContext && !pauseForTyped) {
      yield* sendContext(
        "session.commentary.append",
        activity,
        current.sessionId === sessionId && current.delegationId ? current.delegationId : null,
      );
      lastActivity = activity;
      lastCommentaryAt = now;
    }
    render();
  });

  const work = Effect.gen(function* () {
    yield* Deferred.await(ready).pipe(Effect.timeout("15 seconds"));
    // On replacement sessions, look up the original key before any retry. Known work is never resubmitted.
    if (latest && (latest.status === "uncertain" || latest.status === "prepared")) {
      const prior = yield* backend.read(latest.request).pipe(Effect.timeout("10 seconds"));

      yield* replace({
        ...latest,
        receipt: prior,
        status:
          prior.state === "missing"
            ? "prepared"
            : prior.state === "pending"
              ? "accepted"
              : "settled",
      });
    }
    yield* Stream.fromEffectRepeat(tick.pipe(Effect.andThen(Effect.sleep("500 millis")))).pipe(
      Stream.runDrain,
    );
  });

  const ending = Effect.gen(function* () {
    yield* Deferred.await(stop);
    status = "ending";
    note = "Ending voice…";
    render();
    yield* connection.silence;
    yield* connection.send({ type: "session.close" });
    yield* Deferred.await(closed).pipe(Effect.timeout("5 seconds"), Effect.ignore);
  });

  yield* Effect.all([receive, work], { concurrency: 2 }).pipe(
    Effect.raceFirst(ending),
    Effect.raceFirst(Deferred.await(closed)),
    Effect.timeout("15 minutes"),
    Effect.mapError(
      () =>
        new VoiceError({
          message:
            "Voice disconnected. Your trip is still being worked on. You can reconnect or keep typing.",
        }),
    ),
  );
});
