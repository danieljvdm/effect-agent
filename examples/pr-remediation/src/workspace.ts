import { type ReviewHandoff } from "@effect-agent/pr-review";
import { Context, Effect, Layer, Ref, Schema, type Scope } from "effect";

import {
  PatchSnapshot,
  PublishedRemediation,
  RemediationAttemptAlreadyClaimed,
  RemediationCheckResult,
  type RemediationReport,
  type RemediationTrigger,
  type RemediationTriggerRejected,
  type StalePullRequestHead,
  WorkspaceOperationFailure,
  type WorkspaceViolation,
} from "./contracts.ts";

export class WorkspaceSearchHit extends Schema.Class<WorkspaceSearchHit>(
  "@effect-agent/example-pr-remediation/WorkspaceSearchHit",
)({
  path: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  line: Schema.Int.check(Schema.isGreaterThan(0)),
  text: Schema.String.check(Schema.isMaxLength(1_000)),
}) {}

/** Narrow capability captured by model-visible tools for one acquired worktree. */
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
  ) => Effect.Effect<RemediationCheckResult, WorkspaceOperationFailure>;
}

export class ImplementationWorkspaceService extends Context.Service<
  ImplementationWorkspaceService,
  ImplementationWorkspace
>()("@effect-agent/example-pr-remediation/ImplementationWorkspace") {}

/** Host-only half of an acquired worktree. It is never installed as a Tool dependency. */
export interface AcquiredRemediationWorktree {
  readonly allowedPaths: ReadonlySet<string>;
  readonly modelWorkspace: ImplementationWorkspace;
  readonly inspectPatch: Effect.Effect<PatchSnapshot, WorkspaceOperationFailure>;
  readonly runCheck: (
    name: string,
  ) => Effect.Effect<RemediationCheckResult, WorkspaceOperationFailure>;
  readonly commitAndPublish: (input: {
    readonly handoff: ReviewHandoff;
    readonly report: RemediationReport;
    readonly patch: PatchSnapshot;
    readonly checks: ReadonlyArray<RemediationCheckResult>;
  }) => Effect.Effect<PublishedRemediation, WorkspaceOperationFailure | StalePullRequestHead>;
}

export class RemediationHost extends Context.Service<
  RemediationHost,
  {
    readonly requiredChecks: ReadonlyArray<string>;
    readonly authorizeTrigger: (
      trigger: RemediationTrigger,
    ) => Effect.Effect<
      void,
      RemediationTriggerRejected | StalePullRequestHead | WorkspaceOperationFailure
    >;
    readonly currentHead: Effect.Effect<string, WorkspaceOperationFailure>;
    readonly acquireWorktree: (
      handoff: ReviewHandoff,
    ) => Effect.Effect<AcquiredRemediationWorktree, WorkspaceOperationFailure, Scope.Scope>;
  }
>()("@effect-agent/example-pr-remediation/RemediationHost") {}

/** One claim per repository/PR/reviewed head for the lifetime of this Class-E host. */
export class RemediationAttemptPolicy extends Context.Service<
  RemediationAttemptPolicy,
  {
    readonly claim: (
      handoff: ReviewHandoff,
    ) => Effect.Effect<void, RemediationAttemptAlreadyClaimed>;
  }
>()("@effect-agent/example-pr-remediation/RemediationAttemptPolicy") {
  static readonly layerMemory = Layer.effect(
    RemediationAttemptPolicy,
    Effect.gen(function* () {
      const claimed = yield* Ref.make<ReadonlySet<string>>(new Set());
      const claim = Effect.fn("RemediationAttemptPolicy.claim")(function* (handoff: ReviewHandoff) {
        const key = `${handoff.repository}#${handoff.pullRequestNumber}@${handoff.reviewedHeadSha}`;
        const accepted = yield* Ref.modify(claimed, (previous) => {
          if (previous.has(key)) return [false, previous] as const;
          return [true, new Set([...previous, key])] as const;
        });
        if (!accepted) {
          return yield* RemediationAttemptAlreadyClaimed.make({
            repository: handoff.repository,
            pullRequestNumber: handoff.pullRequestNumber,
            reviewedHeadSha: handoff.reviewedHeadSha,
          });
        }
      });
      return RemediationAttemptPolicy.of({ claim });
    }),
  );
}

export interface HostCheck {
  readonly name: string;
  readonly run: (
    worktreeRoot: string,
  ) => Effect.Effect<RemediationCheckResult, WorkspaceOperationFailure>;
}
