import { expectTypeOf } from "@effect/vitest";
import { Context, type Layer, Schema } from "effect";
import type { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import type { LifecyclePublicationHandler } from "effect-agent/lifecycle-publication";

import type { ThreadMutationGate } from "../src/Alarm.ts";
import * as ThreadObject from "../src/ThreadObject.ts";

class PublicationDependency extends Context.Service<
  PublicationDependency,
  { readonly ready: boolean }
>()("publication-types/PublicationDependency") {}
class PublicationSetupError extends Schema.TaggedError<PublicationSetupError>()(
  "PublicationSetupError",
  {},
) {}

declare const application: Layer.Layer<DurableAgentRuntime>;
declare const lifecyclePublication: Layer.Layer<
  LifecyclePublicationHandler,
  PublicationSetupError,
  ThreadMutationGate | PublicationDependency
>;

// c68edc7aa1cdedb92ad6f31b63ba048bb718b2c6 leaked the gate supplied by the host.
const hosted = ThreadObject.layerInHost(application, { lifecyclePublication });
const registered = ThreadObject.layer([], { lifecyclePublication });

expectTypeOf<Extract<Layer.Services<typeof hosted>, ThreadMutationGate>>().toEqualTypeOf<never>();
expectTypeOf<
  Extract<Layer.Services<typeof registered>, ThreadMutationGate>
>().toEqualTypeOf<never>();
expectTypeOf<
  Extract<Layer.Services<typeof hosted>, PublicationDependency>
>().toEqualTypeOf<PublicationDependency>();
expectTypeOf<
  Extract<Layer.Error<typeof hosted>, PublicationSetupError>
>().toEqualTypeOf<PublicationSetupError>();
