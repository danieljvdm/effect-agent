import { Schema } from "effect";
import { R2, Workflow } from "effect-cf";
import * as Sandbox from "effect-cf/sandbox";

import { AppBuildRequest, AppCommit } from "../domain.ts";

export class SiteBuildBinding extends Workflow.Tag<SiteBuildBinding>()("trip-app/SiteBuild", {
  payload: AppBuildRequest,
  result: Schema.Struct({ commitId: AppCommit }),
}) {}

export class AppBuildBucket extends R2.Tag<AppBuildBucket>()("trip-app/AppBuildBucket") {}

export class AppBuildSandbox extends Sandbox.Tag<AppBuildSandbox>()("trip-app/AppBuildSandbox") {}
