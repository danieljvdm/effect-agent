import { Clock, Context, Deferred, Effect, Schema, Stream } from "effect";

import {
  SendMessageRequest,
  type PlannerError,
  type PlannerProgress,
  type VoiceWork,
  type VoiceWorkRequest,
  type SpokenMessage,
} from "../domain.ts";
import { websiteContext, websiteUpdate, type VoiceBackground } from "./background.ts";
import {
  appendCaption,
  delegationMessage,
  isCaption,
  voiceUpdate,
  type VoiceRequest,
} from "./delegation.ts";
import {
  contextParts,
  shortContext,
  VoiceError,
  type Caption,
  type LiveEvent,
} from "./protocol.ts";

export interface VoiceView {
  readonly status: "idle" | "connecting" | "listening" | "ending" | "disconnected";
  readonly note: string;
  readonly captions: ReadonlyArray<Caption>;
  readonly muted: boolean;
}

export class VoiceConnection extends Context.Service<
  VoiceConnection,
  {
    readonly events: Stream.Stream<LiveEvent, VoiceError>;
    readonly send: (event: Readonly<Record<string, unknown>>) => Effect.Effect<void, VoiceError>;
    readonly silence: Effect.Effect<void>;
    readonly resume: Effect.Effect<void, VoiceError>;
  }
>()("travel-planner/voice/VoiceConnection") {}

export class VoiceBackend extends Context.Service<
  VoiceBackend,
  {
    readonly submit: (request: SendMessageRequest) => Effect.Effect<unknown, PlannerError>;
    readonly read: (
      request: typeof VoiceWorkRequest.Type,
    ) => Effect.Effect<VoiceWork, PlannerError>;
    readonly progress: () => PlannerProgress | null;
    readonly background: () => VoiceBackground | null;
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
>()("travel-planner/voice/VoiceBackend") {}

/** Per-call ports are provided at the browser boundary. Audio never owns admitted work's lifetime. */
export const runVoiceSession = Effect.fn("runVoiceSession")(function* (
  envelope: Omit<SendMessageRequest, "message" | "requestId">,
  retained: ReadonlyArray<VoiceRequest>,
  stop: Deferred.Deferred<void>,
) {
  const connection = yield* VoiceConnection;
  const backend = yield* VoiceBackend;
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

  const findings = () =>
    backend
      .background()
      ?.scouts?.flatMap((scout) =>
        scout.state === "idle" && scout.finding?.text.trim() ? [scout.finding] : [],
      ) ?? [];

  let seenFindings = new Set(findings().map((finding) => finding.id));

  const rememberFinding = (id: string) => {
    seenFindings.add(id);
    const oldest = seenFindings.values().next();

    if (seenFindings.size > 128 && !oldest.done) seenFindings.delete(oldest.value);
  };

  let researchNoteSerial = 0;

  let researchNote:
    | { id: string; serial: number; parts: ReadonlyArray<string>; next: number }
    | undefined;

  let typedRequestId = backend.typedRequest()?.requestId;
  let lastContext = "";
  let lastWebsiteUpdate = websiteUpdate(backend.background())?.id;
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
    // Observation must not reorder the conversation or evict an uncertain admission.
    const retained = requests.some((item) => item.request.requestId === next.request.requestId)
      ? requests.map((item) => (item.request.requestId === next.request.requestId ? next : item))
      : [...requests, next];

    if (retained.length > 16) {
      const discard = retained.findIndex(
        (item) =>
          item.request.requestId !== next.request.requestId &&
          (item.status === "accepted" || item.status === "settled"),
      );

      if (discard < 0)
        return yield* new VoiceError({
          message: "Too many voice requests need reconciliation. Reconnect before adding more.",
        });
      retained.splice(discard, 1);
    }
    yield* backend.persist(retained);
    requests = retained;
    if (latest?.request.requestId === next.request.requestId) latest = next;
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
      researchNote = undefined;
      seenFindings = new Set(findings().map((finding) => finding.id));
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
        researchNote = undefined;
        seenFindings = new Set(findings().map((finding) => finding.id));
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
    const context = `${websiteContext(backend.background())} ${backend.context()}`.trim();

    if (context && context !== lastContext && !append && !typedContext && !pending) {
      yield* sendContext("session.thinking.append", context, null);
      lastContext = context;
    }
    const website = websiteUpdate(backend.background());

    if (
      website &&
      website.id !== lastWebsiteUpdate &&
      !append &&
      !pending &&
      !typedContext &&
      now - lastSpeechAt >= 1500
    ) {
      yield* sendContext("session.commentary.append", website.text, null);
      lastWebsiteUpdate = website.id;
    }
    const current = latest;
    let work: VoiceWork | undefined;

    // Admission recovery belongs to every retained envelope, independently of spoken updates.
    for (const request of requests) {
      if (
        request.status !== "prepared" &&
        request.status !== "uncertain" &&
        request.request.requestId !== current?.request.requestId
      )
        continue;
      if (request.status === "prepared") {
        // One send per envelope in this call. Reconnect looks up every unresolved identity first.
        const admitted = yield* Effect.result(
          backend.submit(request.request).pipe(Effect.timeout("20 seconds")),
        );

        yield* replace({
          ...request,
          status: admitted._tag === "Success" ? "accepted" : "uncertain",
        });
        if (request.request.requestId === current?.request.requestId)
          note = admitted._tag === "Success" ? "Working on your trip…" : "Checking your request…";
      }

      const observed = yield* Effect.result(
        backend.read(request.request).pipe(Effect.timeout("10 seconds")),
      );

      if (observed._tag === "Failure") {
        if (request.request.requestId === current?.request.requestId)
          note = "Reconnecting to your trip…";
        continue;
      }
      if (observed.success.state !== "missing")
        yield* replace({
          ...request,
          receipt: observed.success,
          status: observed.success.state === "pending" ? "accepted" : "settled",
        });
      if (request.request.requestId === current?.request.requestId) work = observed.success;
    }

    if (
      !current ||
      !work ||
      latest?.request.requestId !== current.request.requestId ||
      stale ||
      pending ||
      typedRevision !== backend.typedRevision()
    ) {
      render();

      return;
    }
    if (work.state === "missing") {
      render();

      return;
    }
    const answers = backend.answers();

    if (lastUpdate === `settled:${work.receiptId}`)
      for (const item of answers) if (item.text === work.text) seenAnswers.add(item.id);
    const answer = answers.find((item) => !seenAnswers.has(item.id));

    const update =
      !work.superseded && work.state === "completed" && answer && answer.text !== work.text
        ? { kind: "result" as const, key: `answer:${answer.id}`, text: shortContext(answer.text) }
        : voiceUpdate(work, backend.progress());

    if (
      update &&
      now - lastUserAt >= 1500 &&
      !append &&
      update.key !== lastUpdate &&
      (update.kind === "result" || !researchNote) &&
      (update.kind === "result" || now - lastProgressAt >= 3000)
    ) {
      const delegation =
        current.sessionId === sessionId && current.delegationId ? current.delegationId : null;

      yield* sendContext(
        update.kind === "result" ? "session.commentary.append" : "session.thinking.append",
        [websiteContext(backend.background()), update.text].filter(Boolean).join("\n"),
        delegation,
      );
      lastUpdate = update.key;
      if (update.kind === "result") {
        if (answer && (update.key === `answer:${answer.id}` || answer.text === work.text))
          seenAnswers.add(answer.id);
        // A later research answer continues the exchange; don't then repeat the earlier receipt answer.
        if (update.key.startsWith("answer:")) lastUpdate = `settled:${work.receiptId}`;
      }
      lastProgressAt = now;
      note = update.kind === "result" ? "Listening" : "Working on your trip…";
    }
    if (!work.superseded && (work.state === "pending" || work.state === "completed")) {
      const available = findings();

      if (researchNote && !available.some((finding) => finding.id === researchNote?.id))
        researchNote = undefined;
      const finding = available.find((item) => !seenFindings.has(item.id));

      if (!researchNote && finding)
        researchNote = {
          id: finding.id,
          serial: ++researchNoteSerial,
          parts: contextParts(finding.text),
          next: 0,
        };
      if (researchNote && !append && !typedContext && !pauseForTyped) {
        const note = researchNote;

        const delegation =
          current.sessionId === sessionId && current.delegationId ? current.delegationId : null;

        if (note.next < note.parts.length) {
          yield* sendContext(
            "session.thinking.append",
            `Research note ${note.serial}, part ${note.next + 1}/${note.parts.length}: ${note.parts[note.next]}`,
            delegation,
          );
          note.next++;
        } else if (now - lastSpeechAt >= 1500) {
          yield* sendContext(
            "session.commentary.append",
            `New finding from complete research note ${note.serial}; preserve all caveats: ${note.parts[0]}`,
            delegation,
          );
          rememberFinding(note.id);
          researchNote = undefined;
        }
      }
    }
    render();
  });

  const work = Effect.gen(function* () {
    yield* Deferred.await(ready).pipe(Effect.timeout("15 seconds"));
    // On replacement sessions, look up the original key before any retry. Known work is never resubmitted.
    for (const request of requests) {
      if (request.status !== "uncertain" && request.status !== "prepared") continue;
      const prior = yield* backend.read(request.request).pipe(Effect.timeout("10 seconds"));

      yield* replace({
        ...request,
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
