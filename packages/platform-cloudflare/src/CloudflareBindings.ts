import { Effect } from "effect";
import type { ThreadId } from "effect-agent/identifiers";

import * as Host from "./CloudflareHostBindings.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";

export * from "./CloudflareHostBindings.ts";

/** Invoke a placed Thread using effect-cf's current native invocation channel. */
export const callThreadObject = <A, E>(
  threadId: ThreadId,
  invoke: (target: Host.ThreadObjectClient) => Promise<A>,
  onError: (cause: unknown) => E,
) => Host.callThreadObject(threadId, invoke, onError).pipe(Effect.provide(effectCfRpcLayer));
