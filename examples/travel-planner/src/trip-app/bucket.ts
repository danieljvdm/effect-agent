import type { ReadWriteBucketClient } from "alchemy/Cloudflare/R2/ReadWriteBucket";
import { Context } from "effect";

/** App tools can declare storage without loading the native Cloudflare entrypoints. */
export class AppBuildBucket extends Context.Service<AppBuildBucket, ReadWriteBucketClient<never>>()(
  "trip-app/AppBuildBucket",
) {}
