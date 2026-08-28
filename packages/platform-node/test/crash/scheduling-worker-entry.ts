import { NodeRuntime } from "@effect/platform-node";

import { schedulingCrashWorker } from "./scheduling-worker.ts";

NodeRuntime.runMain(schedulingCrashWorker, { disableErrorReporting: true });
