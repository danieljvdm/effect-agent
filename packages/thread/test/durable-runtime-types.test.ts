import type { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { UpdateError } from "@effect-agent/core/AgentUpdates";
import { type ThreadId, type SubmissionId } from "@effect-agent/core/Identifiers";
import type { MessagingError } from "@effect-agent/core/Messaging";
import type { WorkerError } from "@effect-agent/core/Worker";
import type { MessagingHost } from "@effect-agent/engine/MessagingHost";
import type { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { type DurableBindingFailure } from "@effect-agent/thread/AgentRegistration";
import {
  type DurableAgentRuntime,
  type DurableAwaitFailure,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "@effect-agent/thread/DurableAgentRuntime";
import {
  type Settlement,
  type Principal,
  type SubmissionLedger,
  type LedgerError,
} from "@effect-agent/thread/SubmissionLedger";
import { type SubmissionStatus } from "@effect-agent/thread/SubmissionStatus";
import type { ThreadStore } from "@effect-agent/thread/ThreadStore";
import { expectTypeOf, it } from "@effect/vitest";
import { Context, Effect, Layer, Option, type Crypto, type DateTime } from "effect";

import type { DurableRuntimeFailpoint } from "../src/DurableFailpoint.ts";
import type { makeAgentUpdateRuntime } from "../src/internal/agent-updates.ts";
import type { makeMessagingRuntime } from "../src/internal/messaging-host.ts";
import type { makeWorkerRuntime, WorkerInputControl } from "../src/internal/worker-host.ts";
import type { WorkerRuntime } from "../src/internal/worker-runtime.ts";
import {
  WorkerConcurrencyResolver,
  type WorkerConcurrencyLimit,
  WorkerPolicyResolver,
} from "../src/WorkerHost.ts";

type Runtime = DurableAgentRuntime["Service"];
type Head = ReturnType<Runtime["processThreadHead"]>;
type Status = ReturnType<Runtime["submissionStatus"]>;
type Inspection = ReturnType<Runtime["inspectSubmissionStatus"]>;
type Recovery = ReturnType<Runtime["recoverSubmission"]>;

class PolicyEvidence extends Context.Service<PolicyEvidence, { readonly policy: AgentPolicy }>()(
  "test/PolicyEvidence",
) {}

const capturedPolicyLayer = Layer.effect(
  WorkerPolicyResolver,
  Effect.gen(function* () {
    const evidence = yield* PolicyEvidence;

    return {
      resolveSource: () => Effect.succeed(Option.some(evidence.policy)),
      resolveTarget: () => Effect.succeed(Option.some(evidence.policy)),
    };
  }),
);

const sourceConcurrencyLayer = Layer.effect(
  WorkerConcurrencyResolver,
  Effect.gen(function* () {
    const evidence = yield* PolicyEvidence;

    return {
      resolve: () =>
        Effect.succeed(Option.some({ maxActiveWorkersPerSource: evidence.policy.toolConcurrency })),
    };
  }),
);

it("keeps bounded worker operations and status reads typed without hidden requirements", () => {
  expectTypeOf<Layer.Services<typeof capturedPolicyLayer>>().toEqualTypeOf<PolicyEvidence>();
  expectTypeOf<Layer.Services<typeof sourceConcurrencyLayer>>().toEqualTypeOf<PolicyEvidence>();
  expectTypeOf<ReturnType<(typeof WorkerConcurrencyResolver.Service)["resolve"]>>().toEqualTypeOf<
    Effect.Effect<Option.Option<WorkerConcurrencyLimit>, WorkerError>
  >();
  expectTypeOf<ReturnType<SubagentHost["Service"]["resolveTargetPolicy"]>>().toEqualTypeOf<
    Effect.Effect<Option.Option<AgentPolicy>, WorkerError>
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof makeWorkerRuntime>>>().toEqualTypeOf<
    ThreadStore | SubmissionLedger | Crypto.Crypto | DurableRuntimeFailpoint | WorkerInputControl
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof makeAgentUpdateRuntime>>>().toEqualTypeOf<
    ThreadStore | Crypto.Crypto | DurableRuntimeFailpoint | WorkerRuntime
  >();
  expectTypeOf<Effect.Error<ReturnType<WorkerRuntime["Service"]["prepareUpdate"]>>>().toEqualTypeOf<
    UpdateError | LedgerError
  >();
  expectTypeOf<Effect.Services<ReturnType<typeof makeMessagingRuntime>>>().toEqualTypeOf<
    ThreadStore | Crypto.Crypto
  >();
  expectTypeOf<Parameters<Runtime["workerHost"]>>().toEqualTypeOf<
    [
      request: {
        readonly sourceThreadId: ThreadId;
        readonly principal: Principal;
        readonly sourceSubmissionId?: SubmissionId;
      },
    ]
  >();
  expectTypeOf<ReturnType<Runtime["workerHost"]>>().toEqualTypeOf<
    Effect.Effect<SubagentHost["Service"], WorkerError>
  >();
  expectTypeOf<ReturnType<Runtime["messagingHost"]>>().toEqualTypeOf<
    Effect.Effect<MessagingHost["Service"], MessagingError>
  >();
  expectTypeOf<Parameters<Runtime["processThreadHead"]>>().toEqualTypeOf<
    [threadId: ThreadId, options?: { readonly yieldAfter?: DateTime.Utc }]
  >();
  expectTypeOf<Parameters<Runtime["processThreadResolved"]>>().toEqualTypeOf<
    [threadId: ThreadId]
  >();
  expectTypeOf<Runtime["runResolvedWorker"]>().toEqualTypeOf<
    Effect.Effect<void, DurableWorkerFailure | DurableBindingFailure>
  >();
  expectTypeOf<Head>().toEqualTypeOf<
    Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure | DurableBindingFailure>
  >();
  expectTypeOf<Status>().toEqualTypeOf<Effect.Effect<SubmissionStatus, DurableAwaitFailure>>();
  expectTypeOf<Inspection>().toEqualTypeOf<Status>();
  expectTypeOf<Recovery>().toEqualTypeOf<Effect.Effect<RecoveryReport, DurableWorkerFailure>>();
});
