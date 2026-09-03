import type { ThreadId } from "@effect-agent/core";
import { expectTypeOf, it } from "@effect/vitest";
import type { Effect, Option } from "effect";

import type {
  DurableAgentRuntime,
  DurableAwaitFailure,
  DurableBindingFailure,
  DurableWorkerFailure,
  RecoveryReport,
  Settlement,
  SubmissionStatus,
} from "../src/index.ts";

type Runtime = DurableAgentRuntime["Service"];
type Head = ReturnType<Runtime["processThreadHead"]>;
type Status = ReturnType<Runtime["submissionStatus"]>;
type Inspection = ReturnType<Runtime["inspectSubmissionStatus"]>;
type Recovery = ReturnType<Runtime["recoverSubmission"]>;

it("keeps bounded worker operations and status reads typed without hidden requirements", () => {
  expectTypeOf<Parameters<Runtime["processThreadHead"]>>().toEqualTypeOf<[threadId: ThreadId]>();
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
