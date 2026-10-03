import type { R2Error } from "alchemy/Cloudflare/R2/BucketTypes";
import { Context, type Effect } from "effect";

/** App tools can declare storage without loading the native Cloudflare entrypoints. */
export class AppBuildBucket extends Context.Service<
  AppBuildBucket,
  {
    get(key: string): Effect.Effect<
      {
        readonly size: number;
        readonly bodyUsed: boolean;
        readonly readable: ReadableStream<Uint8Array>;
        text(): Effect.Effect<string, R2Error>;
      } | null,
      R2Error
    >;
    head(key: string): Effect.Effect<Pick<R2Object, "size" | "customMetadata"> | null, R2Error>;
    put(
      key: string,
      value: string | Uint8Array,
      options: R2PutOptions,
    ): Effect.Effect<void, R2Error>;
  }
>()("trip-app/AppBuildBucket") {}
