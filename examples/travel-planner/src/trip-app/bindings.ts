import { Schema } from "effect";
import { R2, Workflow } from "effect-cf";
import * as Sandbox from "effect-cf/sandbox";

import { AppBuildRequest, AppCommit } from "../domain.ts";
import { AppBuildBucket } from "./bucket.ts";

export const AppBuildBucketLive = R2.layer(AppBuildBucket, { binding: "APP_BUILDS" });

export class SiteBuildBinding extends Workflow.Tag<SiteBuildBinding>()("trip-app/SiteBuild", {
  payload: AppBuildRequest,
  result: Schema.Struct({ commitId: AppCommit }),
}) {}

export class AppBuildSandbox extends Sandbox.Tag<AppBuildSandbox>()("trip-app/AppBuildSandbox") {}
