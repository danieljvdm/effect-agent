import { Cause, Effect, FileSystem, Schema } from "effect";
declare const ReleaseManifestSwapError_base: Schema.Class<ReleaseManifestSwapError, Schema.TaggedStruct<"ReleaseManifestSwapError", {
    readonly cause: Schema.optionalKey<Schema.Defect>;
    readonly manifestPath: Schema.String;
    readonly message: Schema.String;
    readonly operation: Schema.Literals<readonly ["install", "restore"]>;
}>, Cause.YieldableError>;
export declare class ReleaseManifestSwapError extends ReleaseManifestSwapError_base {
}
export declare const PublishManifest: Schema.StructWithRest<Schema.Struct<{
    readonly name: Schema.String;
    readonly version: Schema.String;
    readonly private: Schema.optionalKey<Schema.Boolean>;
    readonly exports: Schema.optionalKey<Schema.$Record<Schema.String, Schema.String>>;
    readonly dependencies: Schema.optionalKey<Schema.$Record<Schema.String, Schema.String>>;
    readonly optionalDependencies: Schema.optionalKey<Schema.$Record<Schema.String, Schema.String>>;
    readonly peerDependencies: Schema.optionalKey<Schema.$Record<Schema.String, Schema.String>>;
}>, readonly [Schema.$Record<Schema.String, Schema.Unknown>]>;
export declare const PreparedTarballPath: Schema.refine<string, Schema.String>;
export declare const PreparedReleasePackage: Schema.Struct<{
    readonly name: Schema.String;
    readonly version: Schema.String;
    readonly distTag: Schema.String;
    readonly tarball: Schema.NullOr<Schema.refine<string, Schema.String>>;
}>;
export declare const PreparedReleaseManifest: Schema.Struct<{
    readonly version: Schema.Literal<1>;
    readonly packages: Schema.$Array<Schema.Struct<{
        readonly name: Schema.String;
        readonly version: Schema.String;
        readonly distTag: Schema.String;
        readonly tarball: Schema.NullOr<Schema.refine<string, Schema.String>>;
    }>>;
}>;
export declare const withTemporaryManifest: <A, E, R>(manifestPath: string, originalBytes: string, publishBytes: string, use: Effect.Effect<A, E, R>) => Effect.Effect<A, E | ReleaseManifestSwapError, R | FileSystem.FileSystem>;
export {};
