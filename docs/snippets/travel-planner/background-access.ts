import { Principal } from "@effect-agent/thread/SubmissionLedger";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { Effect, Layer, Schema } from "effect";
import { ThreadId } from "effect-agent/Identifiers";
import { WorkerError } from "effect-agent/Worker";

// This local example permits one user to manage workers from one conversation.
export const principal = Schema.decodeSync(Principal)("travel-user");
export const threadId = Schema.decodeSync(ThreadId)("travel-chat");

export const WorkerAccessLive = Layer.succeed(WorkerHostAuthorizer)({
  authorize: (request) =>
    request.principal === principal && request.sourceThreadId === threadId
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});
