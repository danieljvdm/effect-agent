import { RunEvent } from "@effect-agent/core/RunEvent";
import * as Rpc from "effect/unstable/rpc/Rpc";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";

import { DemoRunSelection } from "./contracts";
import {
  DemoControlAccepted,
  DemoControlFailure,
  DemoOperationalEvent,
  DemoRunFailure,
  QueueRunCommandRequest,
  ResolveRunApprovalRequest,
  StartLiveTravelChatRequest,
  StartOperationalRunRequest,
} from "./operational-contracts";

/** Streams one general-chat turn with bounded prior thread context. */
export class StreamChatRun extends Rpc.make("StreamChatRun", {
  payload: DemoRunSelection,
  success: RunEvent,
  error: DemoRunFailure,
  stream: true,
}) {}

/** Streams one interactive Phase 2 Run and its operational evidence. */
export class StreamOperationalRun extends Rpc.make("StreamOperationalRun", {
  payload: StartOperationalRunRequest,
  success: DemoOperationalEvent,
  error: DemoRunFailure,
  stream: true,
}) {}

/** Streams one OpenAI-coordinated travel Run with fixture Tool handlers. */
export class StreamLiveTravelChatRun extends Rpc.make("StreamLiveTravelChatRun", {
  payload: StartLiveTravelChatRequest,
  success: DemoOperationalEvent,
  error: DemoRunFailure,
  stream: true,
}) {}

/** Offers steering or follow-up input to one active ephemeral Run. */
export class QueueRunCommand extends Rpc.make("QueueRunCommand", {
  payload: QueueRunCommandRequest,
  success: DemoControlAccepted,
  error: DemoControlFailure,
}) {}

/** Resolves one pending approval request exactly once. */
export class ResolveRunApproval extends Rpc.make("ResolveRunApproval", {
  payload: ResolveRunApprovalRequest,
  success: DemoControlAccepted,
  error: DemoControlFailure,
}) {}

/** Schema-owned RPC definitions shared by the demo server and browser client. */
export const DemoRunRpcs = RpcGroup.make(
  StreamChatRun,
  StreamOperationalRun,
  StreamLiveTravelChatRun,
  QueueRunCommand,
  ResolveRunApproval,
);
