import { ThreadId } from "@effect-agent/core/Identifiers";
import { IdempotencyKey, Principal } from "@effect-agent/core/Receipt";
import { ThreadObjectIdentity } from "@effect-agent/platform-cloudflare/CloudflareBindings";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
} from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { Context, Effect, Option, Schema, Stream } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { mergeSpeech } from "../conversation.ts";
import {
  type VoiceWork,
  PlannerError,
  PlannerInput,
  PlannerSettings,
  type VoiceWorkRequest,
  defaultPlannerSettings,
  type PlannerSnapshot,
  type SendMessageRequest,
} from "../domain.ts";
import { ScoutReportInput } from "../research/contracts.ts";
import { researchSnapshot } from "../research/state.ts";
import { PlannerResponse } from "../response.ts";
import { TravelContent } from "../travel-content.ts";
import { publishTripAppAddress } from "../trip-app/addresses.ts";
import { editorSnapshot } from "../trip-app/editor-state.ts";
import { AppRepository } from "../trip-app/repository.ts";
import { plannerActivity } from "./activity.ts";
import { completedAnswer, type Messages } from "./conversation.ts";
import { readDiagnostics } from "./diagnostics.ts";
import {
  planner,
  previousProgressPlanner,
  previousTextPlanner,
  previousVoicePlanner,
} from "./planner.ts";
import { requestsPublication } from "./security.ts";
import { ownerOfThread } from "./tenancy.ts";
import { TripRepository } from "./trips.ts";

export class PlannerModel extends Context.Service<PlannerModel, { readonly model: string }>()(
  "travel-planner/PlannerModel",
) {}

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "The planner is unavailable. Your saved trips are retained.",
  });

const readThread = Effect.fn("readPlannerThread")(function* (id: string) {
  const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(id).pipe(
    Effect.mapError(unavailable),
  );

  const store = yield* ThreadStore;

  return yield* store.export(ThreadExportRequest.make({ threadId })).pipe(
    Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(undefined)),
    Effect.mapError(unavailable),
  );
});

export const sendMessage = Effect.fn("sendMessage")(function* (request: SendMessageRequest) {
  const runtime = yield* DurableAgentRuntime;
  const repository = yield* TripRepository;
  const identity = yield* ThreadObjectIdentity;

  const settings = yield* Schema.decodeUnknownEffect(PlannerSettings)(
    request.settings ?? defaultPlannerSettings,
  ).pipe(
    Effect.mapError(
      () => new PlannerError({ code: "invalid", message: "Invalid model settings." }),
    ),
  );

  const principal = yield* Schema.decodeUnknownEffect(Principal)(
    ownerOfThread(identity.threadId),
  ).pipe(Effect.mapError(unavailable));

  const threadId = yield* Schema.decodeUnknownEffect(ThreadId)(request.conversationId).pipe(
    Effect.mapError(unavailable),
  );

  const idempotencyKey = yield* Schema.decodeUnknownEffect(IdempotencyKey)(request.requestId).pipe(
    Effect.mapError(
      () => new PlannerError({ code: "invalid", message: "Invalid request identifier." }),
    ),
  );

  const ledger = yield* SubmissionLedger;

  const admitted = yield* ledger
    .lookup(SubmissionLookupByKey.make({ threadId, principal, idempotencyKey }))
    .pipe(Effect.mapError(unavailable));

  if (Option.isSome(admitted)) {
    const input = yield* Schema.decodeUnknownEffect(PlannerInput)(admitted.value.inputPayload).pipe(
      Effect.mapError(unavailable),
    );

    const priorSettings = input.settings ?? defaultPlannerSettings;

    if (
      input.message !== request.message ||
      JSON.stringify(input.voice) !== JSON.stringify(request.voice) ||
      input.selectedTripId !== request.selectedTripId ||
      priorSettings.model !== settings.model ||
      priorSettings.reasoningEffort !== settings.reasoningEffort ||
      priorSettings.fast !== settings.fast
    )
      return yield* new PlannerError({
        code: "conflict",
        message: "This message was already submitted with different details.",
      });
    // Preserve the admitted publication revision and legacy seed when acknowledgement was lost.
    // Resubmission completes readiness; finding an admitted ledger row alone is not acceptance.
    const options = { threadId, principal, idempotencyKey };

    yield* (
      admitted.value.agentId === previousTextPlanner.id
        ? runtime.submitRegistered({ definition: previousTextPlanner }, input, options)
        : admitted.value.agentId === previousVoicePlanner.id
          ? runtime.submitRegistered({ definition: previousVoicePlanner }, input, options)
          : admitted.value.agentId === previousProgressPlanner.id
            ? runtime.submitRegistered({ definition: previousProgressPlanner }, input, options)
            : runtime.submitRegistered({ definition: planner }, input, options)
    ).pipe(Effect.mapError(unavailable));

    return { accepted: true as const };
  }

  const selectedTrip =
    request.selectedTripId === null ? null : yield* repository.get(request.selectedTripId);

  if (
    selectedTrip !== null &&
    (yield* repository.conversationId(selectedTrip.id)) !== request.conversationId
  )
    return yield* new PlannerError({
      code: "invalid",
      message: "Open this trip's conversation before sending a message.",
    });

  yield* repository.rememberConversation({
    conversationId: request.conversationId,
    title: request.message.trim().replace(/\s+/g, " ").slice(0, 160) || "New trip",
  });

  yield* runtime
    .submitRegistered(
      { definition: planner },
      {
        message: request.message,
        ...(request.voice === undefined ? {} : { voice: request.voice }),
        settings,
        selectedTripId: request.selectedTripId,
        publication:
          selectedTrip !== null && requestsPublication(request.message)
            ? { tripId: selectedTrip.id, expectedRevision: selectedTrip.revision }
            : null,
      },
      { threadId, principal, idempotencyKey },
    )
    .pipe(Effect.mapError(unavailable));

  return { accepted: true as const };
});

/** Each view reads only its own canonical Thread; the trip catalogue remains owner-wide. */
export const plannerSnapshot = Effect.fn("plannerSnapshot")(function* (
  conversationId: string | null,
) {
  const model = yield* PlannerModel;
  const repository = yield* TripRepository;
  const saved = yield* repository.list;

  const trips = yield* Effect.forEach(
    saved,
    Effect.fn("plannerTrip")(function* (trip) {
      return { ...trip, conversationId: yield* repository.conversationId(trip.id) };
    }),
  );

  const source = conversationId === null ? undefined : yield* readThread(conversationId);
  const currentTrip = trips.find((trip) => trip.conversationId === conversationId);

  const ledger = yield* SubmissionLedger;

  const pending = yield* ledger.scanNonterminal.pipe(
    Stream.filter((submission) => submission.threadId === conversationId),
    Stream.runCollect,
    Effect.mapError(unavailable),
  );

  let messages: Messages = [
    { id: "welcome", role: "assistant", text: "Where do you want to go?", tripId: null },
  ];

  let selected: string | null = null;
  let inputTokens: number | null = null;
  let outputTokens: number | null = null;
  let usageComplete = true;
  let usedModel = model.model;
  const reportSubmissions = new Set<string>();
  const recordedInputs = new Set<string>();

  for (const { record, sequence } of source?.records ?? []) {
    const id = String(sequence);
    const payload = record.payload;

    // oxlint-disable-next-line typescript/switch-exhaustiveness-check
    switch (payload._tag) {
      case "UserInputRecorded": {
        if (
          payload.submissionId !== undefined &&
          Schema.decodeUnknownOption(ScoutReportInput)(payload.input)._tag === "Some"
        )
          reportSubmissions.add(payload.submissionId);
        const input = Schema.decodeUnknownOption(PlannerInput)(payload.input);

        if (input._tag === "Some") {
          selected = input.value.selectedTripId;
          if (payload.submissionId !== undefined) recordedInputs.add(payload.submissionId);
          if (input.value.voice) messages = [...mergeSpeech(messages, input.value.voice.messages)];

          const spokenInput = input.value.voice?.input
            ? input.value.voice.messages.findLast((message) => message.role === "user")
            : undefined;

          if (spokenInput) {
            messages = messages.map((message) =>
              message.id === spokenInput.id
                ? {
                    ...message,
                    ...(payload.submissionId === undefined
                      ? {}
                      : { submissionId: payload.submissionId }),
                  }
                : message,
            );
          } else
            messages.push({
              id,
              role: "user",
              text: input.value.message,
              tripId: selected,
              ...(payload.submissionId === undefined ? {} : { submissionId: payload.submissionId }),
            });
        }
        break;
      }
      case "RunCompleted": {
        const answer = Schema.decodeUnknownOption(completedAnswer)(payload.output);

        if (answer._tag === "Some")
          messages.push({
            id,
            role: "assistant",
            text: typeof answer.value === "string" ? answer.value : answer.value.message,
            tripId: selected,
            response: true,
          });
        break;
      }
      case "ToolCallSettled":
        if (payload.toolName === "deliver_response" && !payload.isFailure) {
          const response = Schema.decodeUnknownOption(PlannerResponse)(payload.result);

          if (response._tag === "Some" && response.value.content !== null)
            messages.push({
              id: `${id}-cards`,
              role: "assistant",
              text: response.value.content.title,
              tripId: selected,
              content: response.value.content,
            });
        }
        if (payload.toolName === "show_travel_options" && !payload.isFailure) {
          const content = Schema.decodeUnknownOption(TravelContent)(payload.result);

          if (content._tag === "Some")
            messages.push({
              id: `${id}-cards`,
              role: "assistant",
              text: content.value.title,
              tripId: selected,
              content: content.value,
            });
        }
        break;
      case "ModelResponseRecorded": {
        const reportedModel = payload.modelUsage?.at(-1)?.model;

        if (reportedModel !== undefined) usedModel = reportedModel;
        if (
          payload.modelUsage === undefined ||
          payload.modelUsage.some((usage) => usage.usageStatus !== "complete")
        )
          usageComplete = false;
        if (payload.inputTokens !== undefined)
          inputTokens = (inputTokens ?? 0) + payload.inputTokens;
        if (payload.outputTokens !== undefined)
          outputTokens = (outputTokens ?? 0) + payload.outputTokens;
        break;
      }
      case "SubmissionSettled":
        if (payload.outcome !== "completed" && !reportSubmissions.has(payload.submissionId)) {
          messages.push({
            id,
            role: "assistant",
            text:
              payload.outcome === "aborted"
                ? "This request was stopped. Your saved trips are still available; send another message to continue."
                : payload.policyLimit === "duration"
                  ? "I ran out of time on this request. Your saved trips are still available; send another message to continue."
                  : "I couldn't finish this request. Your saved trips are still available; send another message to continue.",
            tripId: selected,
          });
        }
        break;
      default:
        break;
    }
  }

  const app =
    currentTrip === undefined
      ? null
      : yield* Effect.flatMap(AppRepository, (apps) => apps.get(currentTrip.id));

  if (app !== null && conversationId !== null) {
    const env = yield* WorkerEnvironment;

    if (env.APP_BUILDS && env.APP_DOMAIN) {
      // Repair the derived address index for pre-publication member apps on their next view.
      yield* publishTripAppAddress(ownerOfThread(conversationId), app, env.APP_DOMAIN).pipe(
        Effect.ignore,
      );
    }
  }

  const visibleMessages = yield* Effect.forEach(
    messages.slice(-100),
    Effect.fn("plannerMessageIdentity")(function* (message) {
      if (message.role !== "user" || message.submissionId === undefined) return message;

      const submissionId = yield* Schema.decodeUnknownEffect(
        SubmissionLookupById.fields.submissionId,
      )(message.submissionId).pipe(Effect.mapError(unavailable));

      const submission = yield* ledger
        .lookup(SubmissionLookupById.make({ submissionId }))
        .pipe(Effect.mapError(unavailable));

      return Option.isSome(submission)
        ? { ...message, requestId: submission.value.idempotencyKey }
        : message;
    }),
  );

  const queuedMessages = pending.flatMap((submission) => {
    if (recordedInputs.has(submission.submissionId)) return [];
    const input = Schema.decodeUnknownOption(PlannerInput)(submission.inputPayload);

    return Option.isSome(input) && !input.value.voice?.input
      ? [{ requestId: submission.idempotencyKey, text: input.value.message }]
      : [];
  });

  return {
    conversationId,
    scouts:
      conversationId === null ? [] : yield* researchSnapshot(conversationId, source?.records ?? []),
    editor:
      currentTrip === undefined || conversationId === null
        ? null
        : yield* editorSnapshot(conversationId, currentTrip.id, source?.records ?? []),
    app,
    messages: visibleMessages,
    trips,
    conversations: yield* repository.listConversations,
    activity: plannerActivity(source?.records ?? [], yield* readDiagnostics),
    pending: pending.length,
    pendingSubmissionIds: pending.map((submission) => submission.submissionId),
    queuedMessages,
    usage: {
      model: usedModel,
      inputTokens: usageComplete ? inputTokens : null,
      outputTokens: usageComplete ? outputTokens : null,
      estimatedCostMicrousd: null,
    },
  } satisfies PlannerSnapshot;
});

/** Reconcile by the original admitted key. Never admit or replay from a read. */
export const voiceWork = Effect.fn("voiceWork")(function* (request: typeof VoiceWorkRequest.Type) {
  const identity = yield* ThreadObjectIdentity;

  const lookup = yield* Schema.decodeUnknownEffect(SubmissionLookupByKey)({
    _tag: "SubmissionLookupByKey",
    threadId: request.conversationId,
    principal: ownerOfThread(identity.threadId),
    idempotencyKey: request.requestId,
  }).pipe(Effect.mapError(unavailable));

  const ledger = yield* SubmissionLedger;
  const found = yield* ledger.lookup(lookup).pipe(Effect.mapError(unavailable));

  if (Option.isNone(found))
    return {
      requestId: request.requestId,
      receiptId: null,
      superseded: false,
      submissionId: null,
      runId: null,
      state: "missing",
      text: null,
    } satisfies VoiceWork;
  const submission = found.value;
  const history = yield* readThread(request.conversationId);
  const records = history?.records ?? [];

  const pending = yield* ledger.scanNonterminal.pipe(
    Stream.runCollect,
    Effect.mapError(unavailable),
  );

  const inputIndex = records.findIndex(
    ({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      record.payload.submissionId === submission.submissionId,
  );

  const superseded =
    pending.some(
      (next) =>
        next.threadId === submission.threadId &&
        next.queueSequence > submission.queueSequence &&
        Schema.is(PlannerInput)(next.inputPayload),
    ) ||
    (inputIndex >= 0 &&
      records
        .slice(inputIndex + 1)
        .some(
          ({ record }) =>
            record.payload._tag === "UserInputRecorded" &&
            Schema.is(PlannerInput)(record.payload.input),
        ));

  const settled = records.find(
    ({ record }) =>
      record.payload._tag === "SubmissionSettled" &&
      record.payload.submissionId === submission.submissionId,
  )?.record.payload;

  const input = records.find(
    ({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      record.payload.submissionId === submission.submissionId,
  )?.record.payload;

  const runId =
    (settled?._tag === "SubmissionSettled" ? settled.runId : undefined) ??
    (input?._tag === "UserInputRecorded" ? input.runId : undefined);

  // Joined inputs settle with the host; only the host stores the validated result.
  const host =
    runId === undefined
      ? undefined
      : records.find(
          ({ record }) =>
            record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
        )?.record.payload;

  const result = host?._tag === "SubmissionSettled" ? host : settled;
  const state = settled?._tag === "SubmissionSettled" ? settled.outcome : "pending";

  const answer =
    state === "completed" && result?._tag === "SubmissionSettled"
      ? Schema.decodeUnknownOption(completedAnswer)(result.result)
      : Option.none();

  return {
    requestId: request.requestId,
    receiptId: submission.receiptId,
    superseded,
    submissionId: submission.submissionId,
    runId: runId ?? null,
    state,
    text: Option.isSome(answer)
      ? typeof answer.value === "string"
        ? answer.value
        : answer.value.message
      : null,
  } satisfies VoiceWork;
});
