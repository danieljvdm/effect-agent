import { Schema } from "effect";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";

import { RunEvent } from "@effect-agent/core";
import { OpenAiDemoRunRequest } from "./contracts";

/** Sanitized expected failure from the remote ephemeral Run. */
export class DemoRunRpcFailure extends Schema.TaggedErrorClass<DemoRunRpcFailure>()(
  "DemoRunRpcFailure",
  {
    errorTag: Schema.NonEmptyString,
    message: Schema.String,
  },
) {}

/** Streams one server-bound Travel Planner Run as semantic events. */
export class StreamDemoRun extends Rpc.make("StreamDemoRun", {
  payload: OpenAiDemoRunRequest,
  success: RunEvent,
  error: DemoRunRpcFailure,
  stream: true,
}) {}

/** Canonical RPC definitions shared by the demo server and browser client. */
export const DemoRunRpcs = RpcGroup.make(StreamDemoRun);
