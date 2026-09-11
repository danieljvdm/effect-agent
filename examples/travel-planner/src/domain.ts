import type { Effect } from "effect";
import { Context, Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";

import { ConnectOpenAi, OpenAiConnection } from "./credential-domain.ts";
import { TravelContent, TravelUrl } from "./travel-content.ts";

export const ShortText = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(240));
export const Text = Schema.String.check(Schema.isMaxLength(4000));
export const TripId = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9-]{1,80}$/));
export const ConversationId = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9-]{1,240}$/));
export const Revision = Schema.Int.check(Schema.isGreaterThan(0));
const DateLabel = Schema.NullOr(Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/)));

export const TripPlace = Schema.Struct({
  id: TripId,
  label: ShortText,
  latitude: Schema.Number.check(Schema.isBetween({ minimum: -90, maximum: 90 })),
  longitude: Schema.Number.check(Schema.isBetween({ minimum: -180, maximum: 180 })),
  kind: Schema.Literals(["stay", "activity", "transport"]),
  url: Schema.NullOr(TravelUrl),
});

export type TripPlace = typeof TripPlace.Type;

export const AppId = Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/));
export const AppCommit = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

export const AppSiteName = Schema.String.check(
  Schema.isPattern(/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/),
);

export const AppSiteRegistration = Schema.Struct({
  version: Schema.Literal(1),
  owner: Schema.String.check(Schema.isPattern(/^(?:travel-planner-owner-v1|member-[a-f0-9]{64})$/)),
  appId: AppId,
  tripId: TripId,
  hostname: Schema.String.check(Schema.isPattern(/^[a-z0-9.-]{1,253}$/)),
});

export const AppFilePath = Schema.String.check(
  Schema.isMaxLength(240),
  // OpenAI's structured-output regex engine does not support lookaround.
  Schema.isPattern(/^[a-zA-Z0-9_.@-]+(\/[a-zA-Z0-9_.@-]+)*$/),
  Schema.makeFilter((path) => path.split("/").every((part) => part !== "." && part !== ".."), {
    title: "a relative source path without traversal segments",
  }),
);

export const AppFile = Schema.Struct({
  path: AppFilePath,
  content: Schema.String.check(Schema.isMaxLength(128 * 1024)),
});

export type AppFile = typeof AppFile.Type;

export const TripAppBuildEvent = Schema.Struct({
  at: Schema.String,
  phase: Schema.Literals([
    "queued",
    "starting",
    "installing",
    "checking",
    "compiling",
    "uploading",
    "ready",
    "failed",
  ]),
  message: ShortText,
});

export type TripAppBuildEvent = typeof TripAppBuildEvent.Type;

export const TripAppVersion = Schema.Struct({
  commitId: AppCommit,
  label: ShortText,
  createdAt: Schema.String,
});

export const TripApp = Schema.Struct({
  id: AppId,
  tripId: TripId,
  revision: Revision,
  url: Schema.String,
  repoName: ShortText,
  sourceCommit: AppCommit,
  activeCommit: Schema.NullOr(AppCommit),
  pendingCommit: Schema.NullOr(AppCommit),
  status: Schema.Literals(["building", "ready", "failed"]),
  error: Schema.NullOr(Text),
  updatedAt: Schema.String,
  versions: Schema.Array(TripAppVersion),
  buildProgress: Schema.optionalKey(Schema.Array(TripAppBuildEvent).check(Schema.isMaxLength(40))),
});

export type TripApp = typeof TripApp.Type;

export const TripAppData = Schema.Struct({
  title: ShortText,
  destination: ShortText,
  summary: Text,
  startDate: DateLabel,
  endDate: DateLabel,
  travelers: Schema.Int,
  days: Schema.Array(
    Schema.Struct({
      date: DateLabel,
      title: ShortText,
      activities: Schema.Array(ShortText),
    }),
  ),
  stays: Schema.Array(
    Schema.Struct({
      id: TripId,
      name: ShortText,
      location: ShortText,
      url: Schema.NullOr(Schema.String),
    }),
  ),
  places: Schema.Array(TripPlace),
});

export type TripAppData = typeof TripAppData.Type;

export const AppBuildRequest = Schema.Struct({
  owner: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  appId: AppId,
  tripId: TripId,
  repoName: ShortText,
  commitId: AppCommit,
  label: ShortText,
});

export type AppBuildRequest = typeof AppBuildRequest.Type;

export const TripDraft = Schema.Struct({
  title: ShortText,
  destination: ShortText,
  summary: Text,
  startDate: DateLabel,
  endDate: DateLabel,
  travelers: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30 })),
  days: Schema.Array(
    Schema.Struct({
      title: ShortText,
      activities: Schema.Array(ShortText).check(Schema.isMaxLength(12)),
    }),
  ).check(Schema.isMaxLength(30)),
  notes: Schema.Array(Text).check(Schema.isMaxLength(20)),
  places: Schema.optionalKey(Schema.Array(TripPlace).check(Schema.isMaxLength(40))),
});

export type TripDraft = typeof TripDraft.Type;

export const PublishedSite = Schema.Struct({
  tripId: TripId,
  revision: Revision,
  path: Schema.String,
  commitId: Schema.String,
  publishedAt: Schema.String,
});

export type PublishedSite = typeof PublishedSite.Type;

/** A revision is immutable. Publication always points at one explicit revision. */
export const Trip = Schema.Struct({
  ...TripDraft.fields,
  id: TripId,
  revision: Revision,
  published: Schema.NullOr(PublishedSite),
});

export type Trip = typeof Trip.Type;

export const SavedTrip = Schema.Struct({ ...Trip.fields, conversationId: ConversationId });
export type SavedTrip = typeof SavedTrip.Type;

/** Navigation exists from the first message, independently of model-authored trip details. */
export const ConversationSummary = Schema.Struct({
  conversationId: ConversationId,
  title: ShortText,
});

export type ConversationSummary = typeof ConversationSummary.Type;

export const SaveTripRequest = Schema.Struct({
  ...TripDraft.fields,
  tripId: Schema.NullOr(TripId),
  expectedRevision: Schema.NullOr(Revision),
});

export type SaveTripRequest = typeof SaveTripRequest.Type;

export const SaveTripRpcRequest = Schema.Struct({
  ...SaveTripRequest.fields,
  conversationId: ConversationId,
});

export const PublishTripRequest = Schema.Struct({ tripId: TripId, expectedRevision: Revision });
export type PublishTripRequest = typeof PublishTripRequest.Type;

const ReasoningEffort = Schema.Literals(["low", "medium", "high", "xhigh", "max"]);

export const PlannerSettings = Schema.Union([
  Schema.Struct({
    model: Schema.Literal("gpt-5.6-luna"),
    reasoningEffort: Schema.Literals(["none", "low", "medium", "high", "xhigh", "max"]),
    fast: Schema.Boolean,
  }),
  Schema.Struct({
    model: Schema.Literal("gpt-6-astra"),
    reasoningEffort: ReasoningEffort,
    fast: Schema.Boolean,
  }),
]);

export type PlannerSettings = typeof PlannerSettings.Type;

export const defaultPlannerSettings: PlannerSettings = {
  model: "gpt-6-astra",
  reasoningEffort: "low",
  fast: true,
};

/** App-owned speech history, separate from requests to execute work. */
export const SpokenMessage = Schema.Struct({
  id: Schema.String.check(Schema.isPattern(/^speech-[a-zA-Z0-9-]+$/), Schema.isMaxLength(100)),
  role: Schema.Literals(["user", "assistant"]),
  text: Schema.String.check(Schema.isMaxLength(8000)),
  after: Schema.NullOr(Schema.String),
});

export type SpokenMessage = typeof SpokenMessage.Type;

export const VoiceContext = Schema.Struct({
  input: Schema.Boolean,
  messages: Schema.Array(SpokenMessage).check(Schema.isMaxLength(48)),
}).check(Schema.makeFilter((value) => JSON.stringify(value).length <= 24000));

export const SendMessageRequest = Schema.Struct({
  message: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4000)),
  requestId: TripId,
  selectedTripId: Schema.NullOr(TripId),
  conversationId: ConversationId,
  settings: Schema.optionalKey(PlannerSettings),
  voice: Schema.optionalKey(VoiceContext),
});

export type SendMessageRequest = typeof SendMessageRequest.Type;

export const VoiceWorkRequest = Schema.Struct({
  requestId: TripId,
  conversationId: ConversationId,
});

/** Read-only receipt/result projection; never implies that speech was heard. */
export const VoiceWork = Schema.Struct({
  requestId: TripId,
  receiptId: Schema.NullOr(Schema.String),
  superseded: Schema.Boolean,
  submissionId: Schema.NullOr(Schema.String),
  runId: Schema.NullOr(Schema.String),
  state: Schema.Literals(["missing", "pending", "completed", "failed", "aborted"]),
  text: Schema.NullOr(Text),
});

export type VoiceWork = typeof VoiceWork.Type;

export const TextPlannerInput = Schema.Struct({
  message: SendMessageRequest.fields.message,
  selectedTripId: Schema.NullOr(TripId),
  publication: Schema.NullOr(PublishTripRequest),
  settings: Schema.optionalKey(PlannerSettings),
  previousMessages: Schema.optionalKey(
    Schema.Array(Schema.Struct({ role: Schema.Literals(["user", "assistant"]), text: Text })),
  ),
});

export const PlannerInput = Schema.Struct({
  ...TextPlannerInput.fields,
  voice: SendMessageRequest.fields.voice,
});

export const PlannerAnswer = Schema.Struct({ message: Text });

/** Temporary progress is replaceable; the canonical thread owns completed answers. */
export const PlannerProgress = Schema.Struct({
  submissionId: Schema.NullOr(Schema.String),
  attemptId: Schema.NullOr(Schema.String),
  revision: Schema.Natural,
  text: Text,
  startedAt: Schema.optionalKey(Schema.Natural),
  completedAt: Schema.optionalKey(Schema.Natural),
  tools: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      label: ShortText,
      state: Schema.Literals(["running", "complete", "failed", "incomplete"]),
      startedAt: Schema.optionalKey(Schema.Natural),
      completedAt: Schema.optionalKey(Schema.Natural),
    }),
  ).check(Schema.isMaxLength(24)),
});

export type PlannerProgress = typeof PlannerProgress.Type;

/** Bounded read-only projection of canonical records; times describe journal boundaries. */
export const PlannerActivity = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["tool", "failure", "usage", "status"]),
  text: Schema.String,
  timestamp: Schema.optionalKey(Schema.String),
  runId: Schema.optionalKey(Schema.String),
  elapsedMs: Schema.optionalKey(Schema.Natural),
  durationMs: Schema.optionalKey(Schema.Natural),
  durationLabel: Schema.optionalKey(ShortText),
  details: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        label: ShortText,
        text: Schema.String.check(Schema.isMaxLength(65_536)),
        truncated: Schema.Boolean,
      }),
    ).check(Schema.isMaxLength(8)),
  ),
});

export type PlannerActivity = typeof PlannerActivity.Type;

export const EditorActivity = Schema.Struct({
  id: Schema.String,
  state: Schema.Literals(["starting", "active", "idle", "failed", "unavailable"]),
  task: Text,
  progress: PlannerProgress,
  activity: Schema.Array(PlannerActivity).check(Schema.isMaxLength(40)),
});

export type EditorActivity = typeof EditorActivity.Type;

export const ResearchScoutActivity = Schema.Struct({
  ...EditorActivity.fields,
  title: ShortText,
  /** Latest successfully settled public summary; never a partial model preview. */
  finding: Schema.optionalKey(Schema.Struct({ id: Schema.String, text: Text })),
});

export type ResearchScoutActivity = typeof ResearchScoutActivity.Type;

export const PlannerSnapshot = Schema.Struct({
  app: Schema.optionalKey(Schema.NullOr(TripApp)),
  editor: Schema.optionalKey(Schema.NullOr(EditorActivity)),
  scouts: Schema.optionalKey(Schema.Array(ResearchScoutActivity).check(Schema.isMaxLength(8))),
  conversationId: Schema.NullOr(ConversationId),
  messages: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      role: Schema.Literals(["user", "assistant"]),
      text: Schema.String,
      tripId: Schema.NullOr(TripId),
      requestId: Schema.optionalKey(Schema.String),
      submissionId: Schema.optionalKey(Schema.String),
      content: Schema.optionalKey(TravelContent),
      supporting: Schema.optionalKey(Schema.Boolean),
      response: Schema.optionalKey(Schema.Boolean),
    }),
  ),
  trips: Schema.Array(SavedTrip),
  conversations: Schema.optionalKey(Schema.Array(ConversationSummary)),
  activity: Schema.Array(PlannerActivity),
  pending: Schema.Natural,
  pendingSubmissionIds: Schema.Array(Schema.String),
  queuedMessages: Schema.optionalKey(
    Schema.Array(
      Schema.Struct({
        requestId: Schema.String,
        text: Text,
      }),
    ),
  ),
  usage: Schema.Struct({
    model: Schema.String,
    inputTokens: Schema.NullOr(Schema.Natural),
    outputTokens: Schema.NullOr(Schema.Natural),
    estimatedCostMicrousd: Schema.NullOr(Schema.Natural),
  }),
});

export type PlannerSnapshot = typeof PlannerSnapshot.Type;

/** Safe diagnostics only; secrets and provider error bodies never cross RPC. */
export class PlannerError extends Schema.TaggedError<PlannerError>()("PlannerError", {
  code: Schema.Literals([
    "invalid",
    "not-found",
    "conflict",
    "storage",
    "publication",
    "unavailable",
  ]),
  message: Schema.String,
}) {}

export const PlannerRpcs = RpcGroup.make(
  Rpc.make("GetOpenAiConnection", { success: OpenAiConnection, error: PlannerError }),
  Rpc.make("ConnectOpenAi", {
    payload: ConnectOpenAi,
    success: OpenAiConnection,
    error: PlannerError,
  }),
  Rpc.make("DisconnectOpenAi", { success: OpenAiConnection, error: PlannerError }),
  Rpc.make("CreateTripApp", {
    payload: Schema.Struct({ tripId: TripId }),
    success: TripApp,
    error: PlannerError,
  }),
  Rpc.make("RetryTripAppBuild", {
    payload: Schema.Struct({ tripId: TripId }),
    success: TripApp,
    error: PlannerError,
  }),
  Rpc.make("RestoreTripApp", {
    payload: Schema.Struct({ tripId: TripId, commitId: AppCommit }),
    success: TripApp,
    error: PlannerError,
  }),
  Rpc.make("GetPlannerSettings", { success: PlannerSettings, error: PlannerError }),
  Rpc.make("SavePlannerSettings", {
    payload: PlannerSettings,
    success: PlannerSettings,
    error: PlannerError,
  }),
  Rpc.make("GetPlanner", {
    payload: Schema.Struct({ conversationId: Schema.NullOr(ConversationId) }),
    success: PlannerSnapshot,
    error: PlannerError,
  }),
  Rpc.make("SendMessage", {
    payload: SendMessageRequest,
    success: Schema.Struct({ accepted: Schema.Literal(true) }),
    error: PlannerError,
  }),
  Rpc.make("GetVoiceWork", {
    payload: VoiceWorkRequest,
    success: VoiceWork,
    error: PlannerError,
  }),
  Rpc.make("SaveTrip", { payload: SaveTripRpcRequest, success: Trip, error: PlannerError }),
  Rpc.make("PublishTrip", {
    payload: PublishTripRequest,
    success: PublishedSite,
    error: PlannerError,
  }),
);

export const ProgressRpcs = RpcGroup.make(
  Rpc.make("WatchProgress", {
    payload: Schema.Struct({ conversationId: ConversationId }),
    success: PlannerProgress,
    stream: true,
    error: PlannerError,
  }),
);

export const PublishedTripDocument = Schema.Struct({
  trip: Trip,
  html: Schema.String.check(Schema.isMaxLength(512 * 1024)),
});

export type PublishedTripDocument = typeof PublishedTripDocument.Type;

/** Host-owned Git adapter. Writes an immutable trip.json and safe index.html commit. */
export class TripSiteStore extends Context.Service<
  TripSiteStore,
  {
    readonly publish: (input: {
      readonly trip: Trip;
    }) => Effect.Effect<PublishedSite, PlannerError>;
    readonly load: (input: {
      readonly tripId: string;
      readonly revision: number;
    }) => Effect.Effect<PublishedTripDocument | null, PlannerError>;
  }
>()("travel-planner/TripSiteStore") {}
