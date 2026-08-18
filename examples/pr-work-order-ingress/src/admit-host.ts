import {
  PatchDigest,
  StalePullRequestHead,
  WorkOrderAttemptPolicy,
  WorkspaceOperationFailure,
  WorkOrderCheckResult,
  WorkOrderHost,
  WorkOrderReport,
  type ImplementationWorkspace,
  type PatchSnapshot,
  type WorkOrderMission,
} from "@effect-agent/example-pr-work-orders";
import { Effect, Layer, Schema } from "effect";

import { IngressPolicy } from "./contracts.ts";
import { GitHubApi } from "./github.ts";
import { WorkOrderImplementer } from "./ingress.ts";

const EMPTY_DIGEST = Schema.decodeUnknownSync(PatchDigest)(
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
);

const emptyPatch: PatchSnapshot = {
  digest: EMPTY_DIGEST,
  changedPaths: [],
  preview: "",
  truncated: false,
};

export const admitOnlyImplementerLayer = Layer.succeed(
  WorkOrderImplementer,
  WorkOrderImplementer.of({
    run: (mission: WorkOrderMission) =>
      Effect.succeed(
        WorkOrderReport.make({
          workOrderDigest: mission.workOrderDigest,
          headSha: mission.order.headSha,
          disposition: "not-applicable",
          changedPaths: [],
          checks: [],
          summary:
            "Live dispatch is admitted and presented. This entrypoint does not yet apply a patch.",
        }),
      ),
  }),
);

export const admitOnlyHostLayer = Layer.effect(
  WorkOrderHost,
  Effect.gen(function* () {
    const github = yield* GitHubApi;
    const policy = yield* IngressPolicy;
    const workspace: ImplementationWorkspace = {
      readFile: () => Effect.succeed(""),
      search: () => Effect.succeed([]),
      applyEdit: () => Effect.void,
      inspectPatch: Effect.succeed(emptyPatch),
      requestCheck: (name) =>
        Effect.succeed(
          WorkOrderCheckResult.make({ name, status: "passed", summary: "admit-only" }),
        ),
    };
    return WorkOrderHost.of({
      requiredChecks: [],
      currentHead: github.currentHead(policy.repository, policy.pullRequestNumber).pipe(
        Effect.mapError((error) =>
          WorkspaceOperationFailure.make({
            operation: "read pull-request head",
            reason: error.reason,
          }),
        ),
      ),
      authorizeDispatch: () => Effect.void,
      requireCurrentHead: (order) =>
        github.currentHead(order.repository, order.pullRequestNumber).pipe(
          Effect.mapError((error) =>
            WorkspaceOperationFailure.make({
              operation: "require current head",
              reason: error.reason,
            }),
          ),
          Effect.flatMap((headSha) =>
            headSha === order.headSha
              ? Effect.void
              : StalePullRequestHead.make({ expected: order.headSha, actual: headSha }),
          ),
        ),
      withWorktree: (_order, run) =>
        run({
          allowedPaths: new Set(),
          modelWorkspace: workspace,
          inspectPatch: workspace.inspectPatch,
          runCheck: workspace.requestCheck,
          observedChecks: Effect.succeed([]),
          commitAndPublish: () =>
            WorkspaceOperationFailure.make({
              operation: "commit and publish",
              reason: "this entrypoint admits and replies; it does not publish commits",
            }),
        }),
    });
  }),
);

export const admitOnlyAttemptPolicyLayer = WorkOrderAttemptPolicy.layerMemory;
