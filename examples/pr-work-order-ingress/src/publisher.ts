import {
  PatchDigest,
  StalePullRequestHead,
  WorkspaceOperationFailure,
} from "@effect-agent/example-pr-work-orders";
import { Context, Crypto, Effect, Encoding, Layer, Schema } from "effect";

import {
  type GitHubApiFailure,
  type PublisherArtifacts,
  PublisherVerificationFailure,
  PublicationUncertainty,
} from "./contracts.ts";
import { GitHubApi } from "./github.ts";

const digestPatch = (
  crypto: Crypto.Crypto,
  patch: string,
): Effect.Effect<PatchDigest, WorkspaceOperationFailure> =>
  crypto.digest("SHA-256", new TextEncoder().encode(patch)).pipe(
    Effect.map(Encoding.encodeHex),
    Effect.flatMap(Schema.decodeUnknownEffect(PatchDigest)),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "digest publisher patch",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );

export const verifyPublisherArtifacts = Effect.fn("verifyPublisherArtifacts")(function* (
  artifacts: PublisherArtifacts,
  currentHead: typeof artifacts.expectedHeadSha,
  crypto: Crypto.Crypto,
): Effect.fn.Return<
  void,
  PublisherVerificationFailure | StalePullRequestHead | WorkspaceOperationFailure
> {
  const digest = yield* digestPatch(crypto, artifacts.patch);
  if (digest !== artifacts.patchDigest) {
    return yield* PublisherVerificationFailure.make({
      reason: "digest-mismatch",
      detail: "publisher-computed patch digest differs from the host-validated digest",
    });
  }
  if (artifacts.changedPaths.some((path) => !artifacts.allowedPaths.includes(path))) {
    return yield* PublisherVerificationFailure.make({
      reason: "path-not-allowed",
      detail: "publisher artifacts name a path outside the work-order allowlist",
    });
  }
  if (artifacts.requiredChecks.some((check) => check.status !== "passed")) {
    return yield* PublisherVerificationFailure.make({
      reason: "check-evidence",
      detail: "publisher artifacts do not include passing required-check evidence",
    });
  }
  if (
    artifacts.workOrderId.length === 0 ||
    artifacts.repository.length === 0 ||
    artifacts.pullRequestNumber <= 0
  ) {
    return yield* PublisherVerificationFailure.make({
      reason: "identity-mismatch",
      detail: "publisher artifacts do not name a complete work-order identity",
    });
  }
  if (currentHead !== artifacts.expectedHeadSha) {
    return yield* StalePullRequestHead.make({
      expected: artifacts.expectedHeadSha,
      actual: currentHead,
    });
  }
});

export class IsolatedPublisher extends Context.Service<
  IsolatedPublisher,
  {
    readonly publish: (
      artifacts: PublisherArtifacts,
    ) => Effect.Effect<
      typeof artifacts.expectedHeadSha,
      | PublisherVerificationFailure
      | StalePullRequestHead
      | PublicationUncertainty
      | WorkspaceOperationFailure
      | GitHubApiFailure
    >;
  }
>()("@effect-agent/example-pr-work-order-ingress/IsolatedPublisher") {
  static readonly layer = Layer.effect(
    IsolatedPublisher,
    Effect.gen(function* () {
      const github = yield* GitHubApi;
      const crypto = yield* Crypto.Crypto;
      const publish = Effect.fn("IsolatedPublisher.publish")(function* (
        artifacts: PublisherArtifacts,
      ) {
        const current = yield* github.currentHead(
          artifacts.repository,
          artifacts.pullRequestNumber,
        );
        yield* verifyPublisherArtifacts(artifacts, current, crypto);
        return yield* github
          .updateHead({
            repository: artifacts.repository,
            pullRequestNumber: artifacts.pullRequestNumber,
            expectedHeadSha: artifacts.expectedHeadSha,
            patchDigest: artifacts.patchDigest,
          })
          .pipe(
            Effect.catchTag("GitHubApiFailure", (error) =>
              PublicationUncertainty.make({
                reason: error.reason,
              }),
            ),
          );
      });
      return IsolatedPublisher.of({ publish });
    }),
  );
}
