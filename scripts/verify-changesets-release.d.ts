import { Cause, Effect, FileSystem, Path, Schema } from "effect";
declare const InvalidCommitSha_base: Schema.Class<InvalidCommitSha, Schema.TaggedStruct<"InvalidCommitSha", {
    readonly label: Schema.String;
    readonly value: Schema.String;
}>, Cause.YieldableError>;
export declare class InvalidCommitSha extends InvalidCommitSha_base {
    get message(): string;
}
declare const VerificationCommandError_base: Schema.Class<VerificationCommandError, Schema.TaggedStruct<"VerificationCommandError", {
    readonly command: Schema.String;
    readonly exitCode: Schema.Int;
    readonly output: Schema.String;
}>, Cause.YieldableError>;
export declare class VerificationCommandError extends VerificationCommandError_base {
    get message(): string;
}
declare const ReleaseTreeMismatch_base: Schema.Class<ReleaseTreeMismatch, Schema.TaggedStruct<"ReleaseTreeMismatch", {
    readonly expectedTree: Schema.String;
    readonly actualTree: Schema.String;
    readonly changedPaths: Schema.String;
}>, Cause.YieldableError>;
export declare class ReleaseTreeMismatch extends ReleaseTreeMismatch_base {
    get message(): string;
}
declare const VerificationCleanupError_base: Schema.Class<VerificationCleanupError, Schema.TaggedStruct<"VerificationCleanupError", {
    readonly cause: Schema.optionalKey<Schema.Defect>;
    readonly message: Schema.String;
    readonly operation: Schema.String;
}>, Cause.YieldableError>;
export declare class VerificationCleanupError extends VerificationCleanupError_base {
}
export declare const verifyChangesetsRelease: (options: {
    readonly baseSha: string;
    readonly headSha: string;
    readonly repositoryRoot?: string;
    readonly changesetBinary?: string;
    readonly bunBinary?: string;
}) => Effect.Effect<undefined, import("effect/PlatformError").BadArgument | InvalidCommitSha | import("effect/PlatformError").PlatformError | ReleaseTreeMismatch | VerificationCleanupError | VerificationCommandError, import("effect/unstable/process/ChildProcessSpawner").ChildProcessSpawner | FileSystem.FileSystem | Path.Path | import("effect/Scope").Scope>;
export {};
