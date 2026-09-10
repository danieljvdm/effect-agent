import { Clock, Deferred, Effect, Schema, Stream } from "effect";

import {
  SendMessageRequest,
  type PlannerError,
  type PlannerProgress,
  type VoiceWork,
  type VoiceWorkRequest,
} from "../domain.ts";
import type { VoiceConnection } from "./browser.ts";
import {
  appendCaption,
  delegationMessage,
  isCaption,
  voiceUpdate,
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
  readonly typedRevision: () => number;
  readonly typedContext: () => string;
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
  let status: VoiceView["status"] = "connecting";
  let note = "Connecting…";
  let pending: { id: string; offset: number; at: number } | undefined;
  let append: { id: string; at: number } | undefined;
  let typedContext: string | undefined;
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
    type: "session.thinking.append" | "session.commentary.append",
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
          note = "Listening · AI voice";
          yield* Deferred.succeed(ready, undefined);
        } else if (event.type === "session.closed") {
          yield* Deferred.succeed(closed, undefined);
        } else if (isCaption(event)) {
          captions = appendCaption(captions, event);
          if (
            event.type === "session.input_transcript.delta" &&
            latest &&
            (latest.sessionId !== sessionId || event.start_ms > latest.offset)
          )
            stale = true;
        } else if (event.type === "session.delegation.created") {
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
          note = "Update accepted by voice · playback unconfirmed";
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
      yield* connection.silence;
      typedContext = `New typed input supersedes earlier results. Already sent to the planner; do not delegate it again. ${backend.typedContext()}`;
      note = "Typed update sent · earlier speech muted";
    }
    if (append && now - append.at > 15_000)
      return yield* new VoiceError({
        message: "Voice did not acknowledge an update. Reconnect to check existing work.",
      });
    if (typedContext && !append) {
      yield* sendContext("session.thinking.append", typedContext, null);
      typedContext = undefined;
    }
    if (pending && now - pending.at >= 400) {
      const delegation = pending;
      const message = delegationMessage(captions, delegation.offset);

      if (message !== null) {
        pending = undefined;

        const request = yield* Schema.decodeUnknownEffect(SendMessageRequest)({
          ...envelope,
          message,
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
        lastUpdate = "";
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
      note =
        accepted.status === "accepted"
          ? "Planner accepted the request"
          : "Admission uncertain · reconnect to reconcile";
    }

    const observed = yield* Effect.result(
      backend.read(current.request).pipe(Effect.timeout("10 seconds")),
    );

    if (observed._tag === "Failure") {
      note = "Planner status temporarily unavailable";
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
    const update = voiceUpdate(work, backend.progress());

    if (
      update &&
      !append &&
      update.key !== lastUpdate &&
      (update.kind === "result" || now - lastProgressAt >= 3000)
    ) {
      const delegation = current.sessionId === sessionId ? current.delegationId : null;

      yield* sendContext(
        update.kind === "result" ? "session.commentary.append" : "session.thinking.append",
        update.text,
        delegation,
      );
      lastUpdate = update.key;
      lastProgressAt = now;
      note =
        update.kind === "result"
          ? "Planner settled · sent to voice"
          : "Planner working · preview sent";
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
    note = "Ending voice · planner work continues";
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
            "Voice ended or disconnected. Accepted planner work continues; reconnect to check it.",
        }),
    ),
  );
});
