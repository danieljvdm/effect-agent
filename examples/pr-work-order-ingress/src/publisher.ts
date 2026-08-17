import { StalePullRequestHead } from "@effect-agent/example-pr-work-orders";
import { Context, Effect, type FileSystem, Layer, type Path, Schema, type Scope } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process";

import {
  GITHUB_WRITE_TOKEN_ENV,
  IsolationViolation,
  type PublisherRequest,
  PublisherVerificationFailure,
  type PublicationUncertainty,
} from "./contracts.ts";
import { spawnIsolatedWorker } from "./isolation.ts";

const PublisherWorkerOutcome = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("published"),
    headSha: Schema.String,
  }),
  PublisherVerificationFailure,
  StalePullRequestHead,
  IsolationViolation,
]);

export const changedPathsFromPatch = (patch: string): ReadonlyArray<string> => {
  const paths = new Set<string>();
  for (const line of patch.split("\n")) {
    const git = /^diff --git a\/(.+) b\/(.+)$/.exec(line);
    if (git?.[2] !== undefined && git[2] !== "/dev/null") paths.add(git[2]);
  }
  return [...paths];
};

export class IsolatedPublisher extends Context.Service<
  IsolatedPublisher,
  {
    readonly publish: (
      request: PublisherRequest,
    ) => Effect.Effect<
      string,
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
    readonly writeToken?: string | undefined;
  }) =>
    Layer.succeed(
      IsolatedPublisher,
      IsolatedPublisher.of({
        publish: (request) =>
          Effect.gen(function* () {
            const payload = yield* spawnIsolatedWorker({
              role: "publish",
              request: { ...request, stateDir: options.stateDir },
              env:
                options.writeToken === undefined
                  ? {}
                  : { [GITHUB_WRITE_TOKEN_ENV]: options.writeToken },
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
