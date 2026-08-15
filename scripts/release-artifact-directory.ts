import { Effect, FileSystem, Path } from "effect";

/**
 * Lease a same-volume staging directory for one prepared release tree.
 *
 * The lease always attempts forceful cleanup when its Scope closes. After a
 * successful rename into the final destination that cleanup is a no-op;
 * before the rename it removes partial output on failure or interruption.
 */
export const acquireReleaseArtifactStagingDirectory = Effect.fn("releaseArtifactDirectory.acquire")(
  function* (destination: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const parent = path.dirname(destination);
    yield* fs.makeDirectory(parent, { recursive: true });
    return yield* Effect.acquireRelease(
      fs.makeTempDirectory({
        directory: parent,
        prefix: ".effect-agent-release-staging-",
      }),
      (directory) => fs.remove(directory, { force: true, recursive: true }).pipe(Effect.orDie),
    );
  },
);
