import { Effect, Schema, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";

import { TravelPlan, type TravelPlan as TravelPlanValue } from "@effect-agent/testing";
import { decodeErrorDetails } from "./error-details";
import {
  type DemoApprovalChoice,
  type DemoCommandKind,
  type DemoOperationalEvent,
  type DemoRunHandle,
  type DemoScenario,
} from "./operational-contracts";
import { DemoRunRpcClient, DemoRunRpcRuntime } from "./run-rpc-client";

export type DemoStatus = "idle" | "running" | "succeeded" | "failed" | "interrupted" | "suspended";

export interface DemoState {
  readonly status: DemoStatus;
  readonly scenario: DemoScenario;
  readonly runNumber: number;
  readonly handle: DemoRunHandle | null;
  readonly events: ReadonlyArray<DemoOperationalEvent>;
  readonly output: TravelPlanValue | null;
  readonly error: string | null;
  readonly controlError: string | null;
}

export const initialDemoState: DemoState = {
  status: "idle",
  scenario: "guided",
  runNumber: 0,
  handle: null,
  events: [],
  output: null,
  error: null,
  controlError: null,
};

/** Shared browser projection of the authoritative server event stream. */
export const demoStateAtom = Atom.make<DemoState>(initialDemoState);

const failureMessage = (error: unknown): string =>
  decodeErrorDetails(error).message ?? String(error);

const failControl = (context: Atom.FnContext, error: unknown): Effect.Effect<void> =>
  Effect.sync(() => {
    const current = context(demoStateAtom);
    context.set(demoStateAtom, {
      ...current,
      controlError: failureMessage(error),
    });
  });

/** Starts one bounded Phase 2 scenario and projects its streamed evidence. */
export const runOperationalDemoAtom = DemoRunRpcRuntime.fn<DemoScenario>()((scenario, context) => {
  const previous = context(demoStateAtom);
  context.set(demoStateAtom, {
    ...previous,
    status: "running",
    scenario,
    runNumber: previous.runNumber + 1,
    handle: null,
    events: [],
    output: null,
    error: null,
    controlError: null,
  });

  const projectEvent = Effect.fn("Demo.projectOperationalEvent")(function* (
    event: DemoOperationalEvent,
  ) {
    const current = context(demoStateAtom);
    const handle = "handle" in event ? event.handle : current.handle;
    let status = current.status;
    let output = current.output;
    let error = current.error;

    switch (event._tag) {
      case "DemoApprovalPending":
        status = "suspended";
        break;
      case "DemoApprovalSettled":
        status = "running";
        break;
      case "RunCompleted": {
        const candidate: unknown = event.output;
        output = yield* Schema.decodeUnknownEffect(TravelPlan)(candidate);
        status = "succeeded";
        break;
      }
      case "RunFailed":
        status = "failed";
        error = event.message;
        break;
      case "RunInterrupted":
        status = "interrupted";
        error = event.message;
        break;
      case "RunSuspended":
        status = "suspended";
        error = event.reason;
        break;
    }

    context.set(demoStateAtom, {
      ...current,
      status,
      handle,
      events: [...current.events, event],
      output,
      error,
    });
  });

  return Stream.unwrap(
    Effect.gen(function* () {
      const client = yield* DemoRunRpcClient;
      return client.StreamOperationalRun({ scenario });
    }),
  ).pipe(
    Stream.runForEach(projectEvent),
    Effect.scoped,
    Effect.tap(() =>
      Effect.sync(() => {
        const current = context(demoStateAtom);
        if (current.status === "running") {
          context.set(demoStateAtom, {
            ...current,
            status: "failed",
            error: "The operational stream ended without a terminal event.",
          });
        }
      }),
    ),
    Effect.tapError((cause) =>
      Effect.sync(() => {
        const current = context(demoStateAtom);
        context.set(demoStateAtom, {
          ...current,
          status: "failed",
          error: failureMessage(cause),
        });
      }),
    ),
    Effect.onInterrupt(() =>
      Effect.sync(() => {
        const current = context(demoStateAtom);
        context.set(demoStateAtom, {
          ...current,
          status: "interrupted",
        });
      }),
    ),
  );
});

export interface QueueDemoCommand {
  readonly kind: DemoCommandKind;
  readonly content: string;
}

/** Offers a command without interrupting the active stream or Tool batch. */
export const queueDemoCommandAtom = DemoRunRpcRuntime.fn<QueueDemoCommand>()((request, context) => {
  const handle = context(demoStateAtom).handle;
  if (handle === null) {
    return failControl(context, "No active Run is ready to accept input.");
  }
  context.set(demoStateAtom, {
    ...context(demoStateAtom),
    controlError: null,
  });
  return Effect.gen(function* () {
    const client = yield* DemoRunRpcClient;
    yield* client.QueueRunCommand({
      handle,
      kind: request.kind,
      content: request.content,
    });
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) => failControl(context, cause)),
  );
});

export interface ResolveDemoApproval {
  readonly requestId: string;
  readonly choice: DemoApprovalChoice;
}

/** Resolves a pending approval exactly once through a separate unary RPC. */
export const resolveDemoApprovalAtom = DemoRunRpcRuntime.fn<ResolveDemoApproval>()((
  request,
  context,
) => {
  const handle = context(demoStateAtom).handle;
  if (handle === null) {
    return failControl(context, "No active Run has a pending approval.");
  }
  context.set(demoStateAtom, {
    ...context(demoStateAtom),
    controlError: null,
  });
  return Effect.gen(function* () {
    const client = yield* DemoRunRpcClient;
    yield* client.ResolveRunApproval({
      handle,
      requestId: request.requestId,
      choice: request.choice,
    });
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) => failControl(context, cause)),
  );
});
