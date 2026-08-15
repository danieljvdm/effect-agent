import { Cause, Effect, Exit, FileSystem, Path, Schema } from "effect";

export class ReleaseArtifactDirectoryError extends Schema.TaggedError<ReleaseArtifactDirectoryError>()(
  "ReleaseArtifactDirectoryError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    destination: Schema.String,
    message: Schema.String,
    operation: Schema.Literals(["create-parent", "create-staging", "commit", "cleanup"]),
  },
) {}

const artifactDirectoryError =
  (destination: string, operation: ReleaseArtifactDirectoryError["operation"]) =>
  (cause: unknown) =>
    ReleaseArtifactDirectoryError.make({
      cause,
      destination,
      message: `Could not ${operation.replace("-", " ")} release artifacts at ${destination}: ${String(cause)}`,
      operation,
    });

/**
 * Prepare one release tree in a same-volume sibling and commit it atomically.
 *
 * The operation owns acquisition, use, commit, and cleanup together so every
 * filesystem failure remains in the typed channel. A failed or interrupted
 * preparation removes partial output; after a successful rename there is no
 * staging path left to clean.
 */
export const prepareReleaseArtifactDirectory = <A, E, R>(
  destination: string,
  prepare: (directory: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | ReleaseArtifactDirectoryError, R | FileSystem.FileSystem | Path.Path> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = path.dirname(destination);
    const acquire = Effect.gen(function* () {
      yield* fs
        .makeDirectory(parent, { recursive: true })
        .pipe(Effect.mapError(artifactDirectoryError(destination, "create-parent")));
      const directory = yield* fs
        .makeTempDirectory({
          directory: parent,
          prefix: ".effect-agent-release-staging-",
        })
        .pipe(Effect.mapError(artifactDirectoryError(destination, "create-staging")));
      return directory;
    });

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const directory = yield* acquire;
        const useExit = yield* restore(
          Effect.gen(function* () {
            const result = yield* prepare(directory);
            yield* fs
              .rename(directory, destination)
              .pipe(Effect.mapError(artifactDirectoryError(destination, "commit")));
            return result;
          }),
        ).pipe(Effect.exit);
        if (Exit.isSuccess(useExit)) return useExit.value;

        const cleanupExit = yield* fs
          .remove(directory, { force: true, recursive: true })
          .pipe(Effect.mapError(artifactDirectoryError(destination, "cleanup")), Effect.exit);
        if (Exit.isFailure(cleanupExit)) {
          return yield* Effect.failCause(Cause.combine(useExit.cause, cleanupExit.cause));
        }
        return yield* Effect.failCause(useExit.cause);
      }),
    );
  });
