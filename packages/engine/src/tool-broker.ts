import type { Effect } from "effect";
import { Context, Schema } from "effect";
import type { Tool, Toolkit } from "effect/unstable/ai";

/**
 * Programmatic Tool invocation seam for Code Mode (runtime spec §12.1,
 * ADR-0017 decision 3). The interpreter provides `ToolBroker` locally in the
 * same pattern as `AgentSpawner` and `DurableStep`: a fail-closed Run-level
 * default shadowed by a live per-outer-Tool-Call service, excluded from
 * `AgentRuntimeRequirements`. The live service is bound to the outer Tool
 * Call's identity, the Run's policy context, and the already-held scheduling
 * permit; per-call input from generated code is data only, and the broker
 * allocates every sequence index from its own monotonic per-pass state.
 */

/** One programmatic invocation request. Data only: the broker owns identity. */
export interface ProgrammaticToolInput {
  /** The exact Effect AI Tool name resolved by the capability's namespace map. */
  readonly toolName: string;
  /** Schema-encoded (wire-form) parameters, exactly as they cross the sandbox. */
  readonly encodedArguments: unknown;
}

/** The handler ran and settled with its encoded success value. */
export interface ProgrammaticCallSuccess {
  readonly _tag: "ProgrammaticCallSuccess";
  /** Broker-owned zero-based index of this call within the pass (RUN-016). */
  readonly index: number;
  readonly encodedResult: unknown;
}

/**
 * The handler settled with the Tool's declared, Schema-encoded typed failure
 * (an Effect AI `failureMode: "return"` result). The program may catch and
 * branch on it.
 */
export interface ProgrammaticCallFailure {
  readonly _tag: "ProgrammaticCallFailure";
  readonly index: number;
  readonly encodedResult: unknown;
}

/**
 * The call never produced a settled handler result: a broker preflight
 * rejected it (concurrency, budget, unknown Tool, approval-requiring Tool,
 * invalid parameters), the handler failed in its typed error channel, or the
 * result failed encoding or the broker-owned size bound. `errorTag` and
 * `message` are the same bounded projection the direct path uses for
 * `ToolCallFailed` events; `index` is present only when the handler was
 * actually started (preflight rejections consume no identity and no budget).
 */
export interface ProgrammaticCallError {
  readonly _tag: "ProgrammaticCallError";
  readonly index: number | undefined;
  readonly errorTag: string;
  readonly message: string;
}

/**
 * Total outcome of one programmatic invocation; defects stay defects. Trusted applications may
 * install `CurrentToolFailureObserver` to receive non-propagating failures before the outcome
 * returns, with the live Cause retained before projection. Neither Causes nor observation values
 * cross the broker boundary.
 */
export type ProgrammaticCallOutcome =
  | ProgrammaticCallSuccess
  | ProgrammaticCallFailure
  | ProgrammaticCallError;

/** Broker-owned per-pass policy for results crossing back into the sandbox. */
export interface ToolBrokerPassOptions {
  /**
   * Maximum UTF-8 byte size of one encoded success result at the sandbox
   * boundary. The broker owns this bound (runtime spec §12.1); the executor
   * enforces its own transport bound independently.
   */
  readonly maxResultBytes: number;
  /**
   * Optional broker-owned redaction pass applied to every encoded success
   * result before the size bound. It must be total; a defect stays a defect.
   */
  readonly redactResult?: ((encodedResult: unknown) => Effect.Effect<unknown>) | undefined;
}

/**
 * One open programmatic pass, bound to one outer Tool Call. Calls are
 * strictly sequential: an invocation issued while another from the same pass
 * is unsettled fails with a typed concurrency error outcome.
 */
export interface ToolBrokerPass {
  readonly invoke: (input: ProgrammaticToolInput) => Effect.Effect<ProgrammaticCallOutcome>;
}

/** The broker was used outside a live Tool batch; there is nothing to bind to. */
export class ToolBrokerUnavailableError extends Schema.TaggedError<ToolBrokerUnavailableError>()(
  "ToolBrokerUnavailableError",
  {
    message: Schema.String,
  },
) {}

/**
 * The pass options are invalid — for example a `maxResultBytes` that is not
 * a positive safe integer, which would make the size comparison fail open.
 */
export class ToolBrokerConfigurationError extends Schema.TaggedError<ToolBrokerConfigurationError>()(
  "ToolBrokerConfigurationError",
  {
    message: Schema.String,
  },
) {}

/**
 * The engine-owned broker service. `openPass` fixes the capability-supplied
 * Toolkit and per-pass result policy once for the whole pass and captures the
 * handler services present at the call site, so nothing inside business
 * execution — least of all generated code — can substitute handlers, policy,
 * or identity afterwards.
 */
export interface ToolBrokerService {
  readonly openPass: <Tools extends Record<string, Tool.Any>>(
    toolkit: Toolkit.WithHandler<Tools>,
    options: ToolBrokerPassOptions,
  ) => Effect.Effect<
    ToolBrokerPass,
    ToolBrokerUnavailableError | ToolBrokerConfigurationError,
    Tool.HandlerServices<Tools[keyof Tools]>
  >;
}

/**
 * Engine-provided programmatic Tool broker (RUN-016, RUN-017). Declare it
 * with `.addDependency(ToolBroker)` on a Tool that performs programmatic
 * invocation; the interpreter supplies it, and an application Layer must not.
 */
export class ToolBroker extends Context.Service<ToolBroker, ToolBrokerService>()(
  "@effect-agent/engine/ToolBroker",
) {}
