import { GitCommitSha, StalePullRequestHead } from "@effect-agent/example-pr-work-orders";
import { Context, Effect, type FileSystem, Layer, type Path, Schema, type Scope } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process";

import {
  IsolatedPublishWorkerRequest,
  IsolationViolation,
  type PublisherRequest,
  PublisherVerificationFailure,
  PublicationUncertainty,
} from "./contracts.ts";
import { spawnIsolatedWorker } from "./isolation.ts";
import {
  PUBLISH_FAILPOINT_ENV,
  PUBLISHER_STATE_DIR_ENV,
  type PublishFailpointLocation,
} from "./worker-contracts.ts";

const PublisherWorkerOutcome = Schema.Union([
  Schema.TaggedStruct("published", {
    headSha: GitCommitSha,
  }),
  PublisherVerificationFailure,
  StalePullRequestHead,
  IsolationViolation,
  PublicationUncertainty,
]);

export class IsolatedPublisher extends Context.Service<
  IsolatedPublisher,
  {
    readonly publish: (
      request: PublisherRequest,
    ) => Effect.Effect<
      GitCommitSha,
      | PublisherVerificationFailure
      | StalePullRequestHead
      | IsolationViolation
      | PublicationUncertainty,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
    >;
  }
>()("@effect-agent/example-pr-work-order-ingress/IsolatedPublisher") {
  static readonly layer = (options: {
    readonly stateDir: string;
    readonly failpoint?: PublishFailpointLocation | undefined;
  }) =>
    Layer.succeed(
      IsolatedPublisher,
      IsolatedPublisher.of({
        publish: (request) =>
          Effect.gen(function* () {
            const payload = yield* spawnIsolatedWorker({
              role: "publish",
              request: IsolatedPublishWorkerRequest.make({
                patch: request.patch,
                trust: request.trust,
              }),
              env: {
                [PUBLISHER_STATE_DIR_ENV]: options.stateDir,
                ...(options.failpoint === undefined
                  ? {}
                  : { [PUBLISH_FAILPOINT_ENV]: options.failpoint }),
              },
            }).pipe(
              Effect.mapError((error) =>
                error._tag === "IsolationViolation"
                  ? error
                  : IsolationViolation.make({
                      process: "publish",
                      reason: "publisher worker failed to start",
                    }),
              ),
            );
            const decoded = yield* Schema.decodeUnknownEffect(PublisherWorkerOutcome)(payload).pipe(
              Effect.mapError(() =>
                IsolationViolation.make({
                  process: "publish",
                  reason: "publisher worker returned an invalid report",
                }),
              ),
            );
            if (decoded._tag === "published") return decoded.headSha;
            return yield* decoded;
          }),
      }),
    );
}
