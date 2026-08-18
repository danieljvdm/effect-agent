import { type Crypto, type Duration, Effect, Exit, Option, Ref, Schema } from "effect";

import {
  RequiredCheckFailed,
  ProposedWorkOrder,
  SettledWorkOrder,
  WorkOrderImplementationFailure,
  WorkOrderRejected,
  WorkOrderReleaseFailure,
  WorkOrderTimeout,
  WorkOrderValidationFailure,
  WorkspaceOperationFailure,
  type PublishedWorkOrder,
  type PullRequestWorkOrder,
  type WorkOrderCheckResult,
  type WorkOrderDigest,
  type WorkOrderHostError,
  type WorkOrderPreparationError,
  type WorkOrderHostResult,
  type WorkOrderPreparationResult,
  type WorkOrderReport,
  workOrderDigest,
  workOrderIdFor,
  workOrderIdentityOf,
} from "./contracts.ts";
import { WorkOrderMission } from "./implementation-agent.ts";
import {
  type ImplementationWorkspace,
  WorkOrderAttemptPolicy,
  WorkOrderHost,
} from "./workspace.ts";

const sorted = (values: Iterable<string>): ReadonlyArray<string> =>
  [...values].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));

const exactMultiset = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean =>
  left.length === right.length && sorted(left).join("\0") === sorted(right).join("\0");

const exactChecks = (
  reported: ReadonlyArray<WorkOrderCheckResult>,
  observed: ReadonlyArray<WorkOrderCheckResult>,
): boolean =>
  reported.length === observed.length &&
  reported.every((check, index) => {
    const actual = observed[index];
    return (
      actual !== undefined &&
      check.name === actual.name &&
      check.status === actual.status &&
      check.summary === actual.summary
    );
  });

const replayExit = <A, E>(exit: Exit.Exit<A, E>): Effect.Effect<A, E> =>
  Exit.isSuccess(exit) ? Effect.succeed(exit.value) : Effect.failCause(exit.cause);

const validateReportIdentity = (
  report: WorkOrderReport,
  digest: WorkOrderDigest,
  order: PullRequestWorkOrder,
): Effect.Effect<void, WorkOrderValidationFailure> => {
  if (report.workOrderDigest !== digest || report.headSha !== order.headSha) {
    return WorkOrderValidationFailure.make({
      reason: "work-order-mismatch",
      detail: "implementation report does not name the admitted work-order digest and head",
    });
  }
  return Effect.void;
};

const ImplementErrorTag = Schema.Struct({
  _tag: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
});

const describeImplementError = (error: unknown): string => {
  try {
    return Option.match(Schema.decodeUnknownOption(ImplementErrorTag)(error), {
      onNone: () => "implementation failed",
      onSome: ({ _tag }) => _tag,
    });
  } catch {
    return "implementation failed";
  }
};

export const runWorkOrder = <ImplementRequirements>(options: {
  readonly order: PullRequestWorkOrder;
  readonly implement: (
    mission: WorkOrderMission,
    workspace: ImplementationWorkspace,
  ) => Effect.Effect<WorkOrderReport, unknown, ImplementRequirements>;
  readonly timeout?: Duration.Duration | undefined;
}): Effect.Effect<
  WorkOrderHostResult,
  WorkOrderHostError,
  ImplementRequirements | Crypto.Crypto | WorkOrderAttemptPolicy | WorkOrderHost
> =>
  Effect.gen(function* () {
    const host = yield* WorkOrderHost;
    const attempts = yield* WorkOrderAttemptPolicy;
    const expectedId = yield* workOrderIdFor(workOrderIdentityOf(options.order));
    if (options.order.workOrderId !== expectedId) {
      return yield* WorkOrderRejected.make({
        reason: "work-order identity does not match the admitted snapshot",
      });
    }
    const digest = yield* workOrderDigest(options.order);
    yield* host.authorizeDispatch(options.order);
    const claim = yield* attempts.claim(options.order);
    if (claim._tag === "duplicate") {
      return yield* replayExit(claim.exit);
    }

    const publication = yield* Ref.make<PublishedWorkOrder | undefined>(undefined);
    return yield* Effect.gen(function* () {
      yield* host.requireCurrentHead(options.order);
      return yield* host
        .withWorktree(options.order, (worktree) =>
          Effect.gen(function* () {
            const implement = options
              .implement(
                WorkOrderMission.make({
                  order: options.order,
                  workOrderDigest: digest,
                  requiredChecks: host.requiredChecks,
                  checkExecution: "inline",
                }),
                worktree.modelWorkspace,
              )
              .pipe(
                Effect.mapError((error) =>
                  WorkOrderImplementationFailure.make({
                    reason: describeImplementError(error),
                  }),
                ),
              );
            const report = yield* options.timeout
              ? implement.pipe(
                  Effect.timeoutOrElse({
                    duration: options.timeout,
                    orElse: () =>
                      WorkOrderTimeout.make({
                        workOrderId: options.order.workOrderId,
                        headSha: options.order.headSha,
                      }),
                  }),
                )
              : implement;
            yield* validateReportIdentity(report, digest, options.order);
            const observedChecks = yield* worktree.observedChecks;
            if (report.disposition === "fixed" && !exactChecks(report.checks, observedChecks)) {
              return yield* WorkOrderValidationFailure.make({
                reason: "check-results-mismatch",
                detail: "model-reported checks differ from host-observed check capability results",
              });
            }

            const { snapshot: patch } = yield* worktree.collectPatch;
            if (report.disposition === "not-applicable" || report.disposition === "needs-human") {
              if (
                patch.changedPaths.length > 0 ||
                report.changedPaths.length > 0 ||
                report.patchDigest !== undefined
              ) {
                return yield* WorkOrderValidationFailure.make({
                  reason: "unexpected-patch",
                  detail: "non-publication dispositions must not propose a patch",
                });
              }
              return SettledWorkOrder.make({
                workOrderId: options.order.workOrderId,
                workOrderDigest: digest,
                headSha: options.order.headSha,
                disposition: report.disposition,
                summary: report.summary,
              });
            }

            if (patch.changedPaths.length === 0) {
              return yield* WorkOrderValidationFailure.make({
                reason: "empty-patch",
                detail: "fixed disposition produced no host-visible patch",
              });
            }
            if (patch.changedPaths.some((path) => !worktree.allowedPaths.has(path))) {
              return yield* WorkOrderValidationFailure.make({
                reason: "path-not-allowed",
                detail: "host-collected patch contains a path outside the work-order allowlist",
              });
            }
            if (!exactMultiset(report.changedPaths, patch.changedPaths)) {
              return yield* WorkOrderValidationFailure.make({
                reason: "changed-paths-mismatch",
                detail: "model-reported changed paths differ from the host-collected patch",
              });
            }
            if (report.patchDigest === undefined || report.patchDigest !== patch.digest) {
              return yield* WorkOrderValidationFailure.make({
                reason: "patch-digest-mismatch",
                detail: "model-reported patch digest differs from the host-collected patch",
              });
            }

            const checkResults = yield* Effect.forEach(host.requiredChecks, (name) =>
              worktree.runCheck(name),
            );
            const failed = checkResults.find((check) => check.status === "failed");
            if (failed !== undefined) {
              return yield* RequiredCheckFailed.make({
                check: failed.name,
                summary: failed.summary,
              });
            }
            const { snapshot: afterChecks } = yield* worktree.collectPatch;
            if (
              afterChecks.digest !== patch.digest ||
              !exactMultiset(afterChecks.changedPaths, patch.changedPaths)
            ) {
              return yield* WorkOrderValidationFailure.make({
                reason: "check-mutated-patch",
                detail: "a required check changed the patch after model settlement",
              });
            }
            const published = yield* worktree.commitAndPublish({
              order: options.order,
              report,
              patch: afterChecks,
              checks: checkResults,
            });
            yield* Ref.set(publication, published);
            return published;
          }),
        )
        .pipe(
          Effect.catch((error: WorkOrderHostError) =>
            Effect.gen(function* () {
              if (!Schema.is(WorkspaceOperationFailure)(error)) {
                return yield* Effect.fail(error);
              }
              const published = yield* Ref.get(publication);
              if (published === undefined || !error.operation.startsWith("release ")) {
                return yield* error;
              }
              const observedHeadSha = yield* host.currentHead.pipe(
                Effect.orElseSucceed(() => undefined),
              );
              return yield* WorkOrderReleaseFailure.make({
                operation: error.operation,
                reason: error.reason,
                publication: published,
                ...(observedHeadSha === undefined ? {} : { observedHeadSha }),
              });
            }),
          ),
        );
    }).pipe(Effect.onExit((exit) => attempts.complete(options.order, exit)));
  });

/**
 * Validate one implementation proposal through the host-owned patch seam, but
 * defer required checks and publication to credential-separated Actions jobs.
 * Durable admission is owned by the ingress journal before this operation is
 * invoked, so this seam deliberately has no in-process attempt policy.
 */
export const prepareWorkOrder = <ImplementRequirements>(options: {
  readonly order: PullRequestWorkOrder;
  readonly implement: (
    mission: WorkOrderMission,
    workspace: ImplementationWorkspace,
  ) => Effect.Effect<WorkOrderReport, unknown, ImplementRequirements>;
  readonly timeout?: Duration.Duration | undefined;
}): Effect.Effect<
  WorkOrderPreparationResult,
  WorkOrderPreparationError,
  ImplementRequirements | Crypto.Crypto | WorkOrderHost
> =>
  Effect.gen(function* () {
    const host = yield* WorkOrderHost;
    const expectedId = yield* workOrderIdFor(workOrderIdentityOf(options.order));
    if (options.order.workOrderId !== expectedId) {
      return yield* WorkOrderRejected.make({
        reason: "work-order identity does not match the admitted snapshot",
      });
    }
    const digest = yield* workOrderDigest(options.order);
    yield* host.authorizeDispatch(options.order);
    yield* host.requireCurrentHead(options.order);
    return yield* host.withWorktree(options.order, (worktree) =>
      Effect.gen(function* () {
        const implement = options
          .implement(
            WorkOrderMission.make({
              order: options.order,
              workOrderDigest: digest,
              requiredChecks: host.requiredChecks,
              checkExecution: "deferred",
            }),
            worktree.modelWorkspace,
          )
          .pipe(
            Effect.mapError((error) =>
              WorkOrderImplementationFailure.make({
                reason: describeImplementError(error),
              }),
            ),
          );
        const report = yield* options.timeout
          ? implement.pipe(
              Effect.timeoutOrElse({
                duration: options.timeout,
                orElse: () =>
                  WorkOrderTimeout.make({
                    workOrderId: options.order.workOrderId,
                    headSha: options.order.headSha,
                  }),
              }),
            )
          : implement;
        yield* validateReportIdentity(report, digest, options.order);
        const observedChecks = yield* worktree.observedChecks;
        if (report.disposition === "fixed" && !exactChecks(report.checks, observedChecks)) {
          return yield* WorkOrderValidationFailure.make({
            reason: "check-results-mismatch",
            detail: "model-reported checks differ from host-observed check capability results",
          });
        }
        const { patch, snapshot } = yield* worktree.collectPatch;
        if (report.disposition === "not-applicable" || report.disposition === "needs-human") {
          if (
            snapshot.changedPaths.length > 0 ||
            report.changedPaths.length > 0 ||
            report.patchDigest !== undefined
          ) {
            return yield* WorkOrderValidationFailure.make({
              reason: "unexpected-patch",
              detail: "non-publication dispositions must not propose a patch",
            });
          }
          return SettledWorkOrder.make({
            workOrderId: options.order.workOrderId,
            workOrderDigest: digest,
            headSha: options.order.headSha,
            disposition: report.disposition,
            summary: report.summary,
          });
        }
        if (snapshot.changedPaths.length === 0) {
          return yield* WorkOrderValidationFailure.make({
            reason: "empty-patch",
            detail: "fixed disposition produced no host-visible patch",
          });
        }
        if (snapshot.changedPaths.some((path) => !worktree.allowedPaths.has(path))) {
          return yield* WorkOrderValidationFailure.make({
            reason: "path-not-allowed",
            detail: "host-collected patch contains a path outside the work-order allowlist",
          });
        }
        if (!exactMultiset(report.changedPaths, snapshot.changedPaths)) {
          return yield* WorkOrderValidationFailure.make({
            reason: "changed-paths-mismatch",
            detail: "model-reported changed paths differ from the host-collected patch",
          });
        }
        if (report.patchDigest === undefined || report.patchDigest !== snapshot.digest) {
          return yield* WorkOrderValidationFailure.make({
            reason: "patch-digest-mismatch",
            detail: "model-reported patch digest differs from the host-collected patch",
          });
        }
        return ProposedWorkOrder.make({
          order: options.order,
          workOrderDigest: digest,
          patch,
          patchDigest: snapshot.digest,
          changedPaths: snapshot.changedPaths,
          requiredChecks: host.requiredChecks,
        });
      }),
    );
  });
