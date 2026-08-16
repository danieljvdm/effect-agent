import { Cause, Effect, FileSystem, Path, Schema } from "effect";
declare const ReleaseArtifactDirectoryError_base: Schema.Class<ReleaseArtifactDirectoryError, Schema.TaggedStruct<"ReleaseArtifactDirectoryError", {
    readonly cause: Schema.optionalKey<Schema.Defect>;
    readonly destination: Schema.String;
    readonly message: Schema.String;
    readonly operation: Schema.Literals<readonly ["create-parent", "create-staging", "commit", "cleanup"]>;
}>, Cause.YieldableError>;
export declare class ReleaseArtifactDirectoryError extends ReleaseArtifactDirectoryError_base {
}
/**
 * Prepare one release tree in a same-volume sibling and commit it atomically.
 *
 * The operation owns acquisition, use, commit, and cleanup together so every
 * filesystem failure remains in the typed channel. A failed or interrupted
 * preparation removes partial output; after a successful rename there is no
 * staging path left to clean.
 */
export declare const prepareReleaseArtifactDirectory: <A, E, R>(destination: string, prepare: (directory: string) => Effect.Effect<A, E, R>) => Effect.Effect<A, E | ReleaseArtifactDirectoryError, R | FileSystem.FileSystem | Path.Path>;
export {};
