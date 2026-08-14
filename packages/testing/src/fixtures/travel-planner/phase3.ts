import { AgentId, ConversationId, RunId, SubmissionId } from "@effect-agent/core";
import {
  BatchId,
  CanonicalBatch,
  ConversationCheckpoint,
  ConversationProjection,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  RecordId,
} from "@effect-agent/session";
import { Effect, Schema } from "effect";

import { TravelPlan, TripRequest } from "./definition.ts";
import { expectedTravelPlan, phase1Trip } from "./scenarios.ts";

/**
 * The Phase 3 profile persists Conversation history but deliberately does not
 * claim durable admission or recovery of accepted work.
 */
export class TravelPlannerPersistenceProfile extends Schema.Class<TravelPlannerPersistenceProfile>(
  "@effect-agent/testing/travel-planner/TravelPlannerPersistenceProfile",
)({
  deploymentClass: Schema.Literal("P"),
  durableAcceptedWork: Schema.Literal(false),
  canonicalSchemaVersion: Schema.Literal(1),
}) {}

export const phase3TravelPlannerProfile = TravelPlannerPersistenceProfile.make({
  deploymentClass: "P",
  durableAcceptedWork: false,
  canonicalSchemaVersion: 1,
});

export const phase3TravelPlannerConversationId = Schema.decodeSync(ConversationId)(
  "travel-planner-p3-conversation",
);
export const phase3TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-p3-producer",
);
export const phase3TravelPlannerRunId = Schema.decodeSync(RunId)("travel-planner-p3-run");

const deploymentId = Schema.decodeSync(DeploymentId)("travel-planner-p3-scripted");
const agentId = Schema.decodeSync(AgentId)("travel-planner");
const submissionId = Schema.decodeSync(SubmissionId)("travel-planner-p3-submission");

const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/** Redacted, deterministic definition identities for the current fixture version. */
export const phase3TravelPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digest("a"),
  model: digest("b"),
  tools: digest("c"),
});

const tripInput = Schema.encodeSync(TripRequest)(phase1Trip);
const travelPlanOutput = Schema.encodeSync(TravelPlan)(expectedTravelPlan);

const record = (recordId: string, createdAt: string, payload: unknown) =>
  Schema.decodeUnknownSync(RecordEnvelope)({
    recordId: Schema.decodeSync(RecordId)(recordId),
    family: "conversation",
    schemaVersion: 1,
    createdAt,
    deploymentId,
    payload,
  });

/**
 * The first atomic append establishes the Conversation and records its input.
 * Its encoded value is the redacted current-version persistence fixture.
 */
export const phase3TravelPlannerInitialBatch = CanonicalBatch.make({
  batchId: Schema.decodeSync(BatchId)("travel-planner-p3-initial"),
  producerId: phase3TravelPlannerProducerId,
  records: [
    record("travel-planner-p3-created", "2026-09-01T00:00:00.000Z", {
      _tag: "ConversationCreated",
      agentId,
      definitions: phase3TravelPlannerDefinitionDigests,
    }),
    record("travel-planner-p3-input", "2026-09-01T00:00:01.000Z", {
      _tag: "UserInputRecorded",
      submissionId,
      kind: "user",
      runId: phase3TravelPlannerRunId,
      input: tripInput,
    }),
  ],
});

/** The second append records the Schema-decoded itinerary and terminal Run result. */
export const phase3TravelPlannerCompletionBatch = CanonicalBatch.make({
  batchId: Schema.decodeSync(BatchId)("travel-planner-p3-completion"),
  producerId: phase3TravelPlannerProducerId,
  records: [
    record("travel-planner-p3-model", "2026-09-01T00:00:02.000Z", {
      _tag: "ModelCompleted",
      runId: phase3TravelPlannerRunId,
      output: travelPlanOutput,
    }),
    record("travel-planner-p3-completed", "2026-09-01T00:00:03.000Z", {
      _tag: "RunCompleted",
      runId: phase3TravelPlannerRunId,
      output: travelPlanOutput,
    }),
  ],
});

export const phase3TravelPlannerBatches = [
  phase3TravelPlannerInitialBatch,
  phase3TravelPlannerCompletionBatch,
] as const;

/** Portable current-version fixture; it contains no passenger identity or credentials. */
export const phase3TravelPlannerEncodedFixture = Schema.encodeSync(Schema.Array(CanonicalBatch))(
  phase3TravelPlannerBatches,
);

export class TravelPlannerProjectionError extends Schema.TaggedErrorClass<TravelPlannerProjectionError>()(
  "TravelPlannerProjectionError",
  { message: Schema.String },
) {}

/** Decode the itinerary projection rebuilt from canonical model-completion records. */
export const travelPlanFromProjection = (
  projection: ConversationProjection,
): Effect.Effect<TravelPlan, TravelPlannerProjectionError> => {
  const output = projection.modelOutputs.at(-1);
  if (output === undefined) {
    return Effect.fail(
      TravelPlannerProjectionError.make({
        message: "The canonical projection has no completed Travel Planner model output.",
      }),
    );
  }
  return Schema.decodeUnknownEffect(TravelPlan)(output).pipe(
    Effect.mapError((error) => TravelPlannerProjectionError.make({ message: error.message })),
  );
};

/** Build a disposable checkpoint bound to a validated canonical prefix. */
export const makePhase3TravelPlannerCheckpoint = (
  projection: ConversationProjection,
): ConversationCheckpoint =>
  Schema.decodeSync(ConversationCheckpoint)({
    schemaVersion: 1,
    conversationId: projection.conversationId,
    throughSequence: projection.throughSequence,
    tailDigest: projection.tailDigest,
    engineVersion: "phase-3-test-runtime",
    agentDefinitionDigest: phase3TravelPlannerDefinitionDigests.agent,
    modelDigest: phase3TravelPlannerDefinitionDigests.model,
    toolDigest: phase3TravelPlannerDefinitionDigests.tools,
    state: Schema.encodeSync(ConversationProjection)(projection),
    createdAt: "2026-09-01T00:00:04.000Z",
  });
