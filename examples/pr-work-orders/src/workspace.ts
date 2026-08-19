import { Context, Deferred, Effect, type Exit, Layer, Ref, Schema } from "effect";

import {
  type GitCommitSha,
  type PatchSnapshot,
  type PublishedWorkOrder,
  type PullRequestWorkOrder,
  type StalePullRequestHead,
  type WorkOrderCheckResult,
  type WorkOrderHostError,
  type WorkOrderHostResult,
  type WorkOrderRejected,
  type WorkOrderReport,
  type WorkspaceOperationFailure,
  type WorkspaceViolation,
} from "./contracts.ts";

export class WorkspaceSearchHit extends Schema.Class<WorkspaceSearchHit>(
  "@effect-agent/example-pr-work-orders/WorkspaceSearchHit",
)({
  path: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  line: Schema.Int.check(Schema.isGreaterThan(0)),
  text: Schema.String.check(Schema.isMaxLength(1_000)),
}) {}

export interface ImplementationWorkspace {
  readonly readFile: (
    path: string,
  ) => Effect.Effect<string, WorkspaceViolation | WorkspaceOperationFailure>;
  readonly search: (
    query: string,
  ) => Effect.Effect<
    ReadonlyArray<WorkspaceSearchHit>,
    WorkspaceViolation | WorkspaceOperationFailure
  >;
  readonly applyEdit: (input: {
    readonly path: string;
    readonly expected: string;
    readonly replacement: string;
  }) => Effect.Effect<void, WorkspaceViolation | WorkspaceOperationFailure>;
  readonly inspectPatch: Effect.Effect<PatchSnapshot, WorkspaceOperationFailure>;
  readonly requestCheck: (
    name: string,
  ) => Effect.Effect<WorkOrderCheckResult, WorkspaceOperationFailure>;
}

export class ImplementationWorkspaceService extends Context.Service<
  ImplementationWorkspaceService,
  ImplementationWorkspace
>()("@effect-agent/example-pr-work-orders/ImplementationWorkspace") {}

export interface AcquiredWorktree {
  readonly allowedPaths: ReadonlySet<string>;
  readonly modelWorkspace: ImplementationWorkspace;
  readonly inspectPatch: Effect.Effect<PatchSnapshot, WorkspaceOperationFailure>;
  readonly collectPatch: Effect.Effect<
    { readonly snapshot: PatchSnapshot; readonly patch: string },
    WorkspaceOperationFailure
  >;
  readonly runCheck: (
    name: string,
  ) => Effect.Effect<WorkOrderCheckResult, WorkspaceOperationFailure>;
  readonly observedChecks: Effect.Effect<ReadonlyArray<WorkOrderCheckResult>>;
  readonly commitAndPublish: (input: {
    readonly order: PullRequestWorkOrder;
    readonly report: WorkOrderReport;
    readonly patch: PatchSnapshot;
    readonly checks: ReadonlyArray<WorkOrderCheckResult>;
  }) => Effect.Effect<PublishedWorkOrder, WorkspaceOperationFailure | StalePullRequestHead>;
}

export class WorkOrderHost extends Context.Service<
  WorkOrderHost,
  {
    readonly requiredChecks: ReadonlyArray<string>;
    readonly currentHead: Effect.Effect<GitCommitSha, WorkspaceOperationFailure>;
    readonly authorizeDispatch: (
      order: PullRequestWorkOrder,
    ) => Effect.Effect<void, WorkOrderRejected | WorkspaceOperationFailure>;
    readonly requireCurrentHead: (
      order: PullRequestWorkOrder,
    ) => Effect.Effect<void, StalePullRequestHead | WorkspaceOperationFailure>;
    readonly withWorktree: <A, E, R>(
      order: PullRequestWorkOrder,
      use: (worktree: AcquiredWorktree) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | WorkOrderRejected | WorkspaceOperationFailure, R>;
  }
>()("@effect-agent/example-pr-work-orders/WorkOrderHost") {}

type AttemptExit = Exit.Exit<WorkOrderHostResult, WorkOrderHostError>;

type AttemptEntry =
  | {
      readonly _tag: "pending";
      readonly deferred: Deferred.Deferred<AttemptExit>;
    }
  | {
      readonly _tag: "settled";
      readonly exit: AttemptExit;
    };

const attemptKey = (order: PullRequestWorkOrder): string =>
  `${order.repository}#${order.pullRequestNumber}@${order.headSha}#${order.workOrderId}`;

export class WorkOrderAttemptPolicy extends Context.Service<
  WorkOrderAttemptPolicy,
  {
    readonly claim: (
      order: PullRequestWorkOrder,
    ) => Effect.Effect<
      { readonly _tag: "claimed" } | { readonly _tag: "duplicate"; readonly exit: AttemptExit }
    >;
    readonly complete: (order: PullRequestWorkOrder, exit: AttemptExit) => Effect.Effect<void>;
  }
>()("@effect-agent/example-pr-work-orders/WorkOrderAttemptPolicy") {
  static readonly layerMemory = Layer.effect(
    WorkOrderAttemptPolicy,
    Effect.gen(function* () {
      const claimed = yield* Ref.make<ReadonlyMap<string, AttemptEntry>>(new Map());
      const claim = Effect.fn("WorkOrderAttemptPolicy.claim")(function* (
        order: PullRequestWorkOrder,
      ) {
        const key = attemptKey(order);
        const deferred = yield* Deferred.make<AttemptExit>();
        const existing = yield* Ref.modify(claimed, (previous) => {
          const current = previous.get(key);
          if (current !== undefined) return [current, previous] as const;
          const pending = { _tag: "pending" as const, deferred };
          return [pending, new Map(previous).set(key, pending)] as const;
        });
        if (existing._tag === "settled") {
          return { _tag: "duplicate" as const, exit: existing.exit };
        }
        if (existing.deferred !== deferred) {
          return { _tag: "duplicate" as const, exit: yield* Deferred.await(existing.deferred) };
        }
        return { _tag: "claimed" as const };
      });
      const complete = Effect.fn("WorkOrderAttemptPolicy.complete")(function* (
        order: PullRequestWorkOrder,
        exit: AttemptExit,
      ) {
        const key = attemptKey(order);
        const previous = yield* Ref.modify(claimed, (map) => {
          const current = map.get(key);
          return [current, new Map(map).set(key, { _tag: "settled" as const, exit })] as const;
        });
        if (previous?._tag === "pending") {
          yield* Deferred.succeed(previous.deferred, exit);
        }
      });
      return WorkOrderAttemptPolicy.of({ claim, complete });
    }),
  );
}

export interface HostCheck {
  readonly name: string;
  readonly run: (
    worktreeRoot: string,
  ) => Effect.Effect<WorkOrderCheckResult, WorkspaceOperationFailure>;
}
