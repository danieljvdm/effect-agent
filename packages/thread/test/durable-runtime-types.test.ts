import { type ThreadId } from "@effect-agent/core/Identifiers";
import { type DurableBindingFailure } from "@effect-agent/thread/AgentRegistration";
import {
  type DurableAgentRuntime,
  type DurableAwaitFailure,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "@effect-agent/thread/DurableAgentRuntime";
import { type Settlement } from "@effect-agent/thread/SubmissionLedger";
import { type SubmissionStatus } from "@effect-agent/thread/SubmissionStatus";
import { expectTypeOf, it } from "@effect/vitest";
import type { DateTime, Effect, Option } from "effect";

type Runtime = DurableAgentRuntime["Service"];
type Head = ReturnType<Runtime["processThreadHead"]>;
type Status = ReturnType<Runtime["submissionStatus"]>;
type Inspection = ReturnType<Runtime["inspectSubmissionStatus"]>;
type Recovery = ReturnType<Runtime["recoverSubmission"]>;

it("keeps bounded worker operations and status reads typed without hidden requirements", () => {
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
