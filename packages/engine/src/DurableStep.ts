import { ToolCallId } from "@effect-agent/core/Identifiers";
import { Context, Schema } from "effect";
import type { Effect, Option } from "effect";
import type { Tool } from "effect/unstable/ai";

/**
 * Declared external execution class of one Tool, used by the durable runtime
 * to decide which Tool Calls need the prepared/settled uncertainty protocol:
 *
 * - `"readonly"`: the handler performs no external mutation. A crash between
 *   start and settlement is a free re-run; no prepared record is written.
 * - `"idempotent"`: the handler declares an external idempotency contract
 *   (it owns key derivation). Prepared/settled records are written and
 *   recovery may re-execute without reconciliation proof.
 * - `"uncertain"`: no external contract is declared. Prepared/settled records
 *   are written and recovery must reconcile or record an Unknown Outcome.
 */
export type ToolExecutionClassValue = "readonly" | "idempotent" | "uncertain";

/**
 * Effect AI Tool annotation key declaring a Tool's execution class, applied
 * with `Tool.annotate(ToolExecutionClass, "...")`.
 *
 * Unannotated Tools default to `"uncertain"`: security and durability
 * decisions are fail-closed, so the framework never infers a safer class
 * from a Tool's shape.
 */
export const ToolExecutionClass = Context.Reference<ToolExecutionClassValue>(
  "@effect-agent/engine/ToolExecutionClass",
  { defaultValue: () => "uncertain" },
);

/** Read a Tool's declared execution class, falling back to the fail-closed `"uncertain"` default. */
export const getToolExecutionClass = (tool: Tool.Any): ToolExecutionClassValue =>
  Context.get(tool.annotations, ToolExecutionClass);

/** Stable identity of one Step within one Tool Call: `(toolCallId, stepName)`. */
export interface RunStepKey {
  readonly toolCallId: ToolCallId;
  readonly stepName: string;
}

/** A previously committed Step result in its canonical encoded form. */
export interface PersistedStepResult {
  readonly encodedOutput: unknown;
}

/**
 * Dependency-neutral Step persistence seam implemented by a durable
 * coordinator and consumed by the engine-provided `DurableStep` service.
 *
 * `lookup` returns the committed result for a Step identity when one exists;
 * `commit` durably records one successful encoded output. Only success is
 * ever committed — recording failures would replay a transient failure
 * forever. Both operations are keyed by the deterministic `RunStepKey`, so
 * replays and racing writers dedupe on record identity in the adapter.
 */
export interface RunStepHook<Error = never, Requirements = never> {
  readonly lookup: (
    key: RunStepKey,
  ) => Effect.Effect<Option.Option<PersistedStepResult>, Error, Requirements>;
  readonly commit: (
    key: RunStepKey,
    encodedOutput: unknown,
  ) => Effect.Effect<void, Error, Requirements>;
}

/**
 * Typed failure of one `DurableStep.do` call.
 *
 * `duplicate-step-name` is an identity conflict: Step names must be
 * deterministic and unique within one Tool Call, and reuse would silently
 * replay another Step's recorded result. `recorded-result-invalid` is the
 * replay-with-different-content conflict: a committed result that no longer
 * decodes through the declared output Schema is never silently accepted.
 */
export class DurableStepError extends Schema.TaggedError<DurableStepError>()("DurableStepError", {
  stepName: Schema.String,
  reason: Schema.Literals([
    "duplicate-step-name",
    "lookup-failed",
    "recorded-result-invalid",
    "output-encoding-failed",
    "commit-failed",
    "no-active-tool-call",
  ]),
  message: Schema.String.check(Schema.isMaxLength(4_096)),
  toolCallId: Schema.optionalKey(ToolCallId),
  /** Diagnostic cause for the live Effect only; Run events retain the fixed public message. */
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

/**
 * Service value provided locally by the engine to each Tool Call's handler.
 *
 * `do` runs one named Step with exactly-once-recorded, at-least-once-executed
 * semantics: a committed result decodes through `output` and returns without
 * executing the body; otherwise the body runs (a crash mid-body re-executes
 * on the next Attempt), the success is encoded through `output`, and only
 * then committed. The Schema argument is the canonical codec for the recorded
 * result, not decoration. A Step never makes a non-idempotent external API
 * exactly-once — the body must use the external system's idempotency key,
 * reconciliation API, or compensating workflow.
 */
export interface DurableStepService {
  readonly do: <Output extends Schema.Top, BodyError, BodyServices>(
    name: string,
    output: Output,
    execute: Effect.Effect<Output["Type"], BodyError, BodyServices>,
  ) => Effect.Effect<
    Output["Type"],
    DurableStepError | BodyError,
    BodyServices | Output["DecodingServices"] | Output["EncodingServices"]
  >;
}

/**
 * Engine-owned Durable Step seam.
 *
 * Declaring this service in `Tool.make({ dependencies: [DurableStep] })` is
 * what makes a Tool a Durable Tool: its handler divides external effects into
 * named Steps and may be re-entered after interruption. The engine provides
 * the service locally to every Tool Call, bound to that call's identity; it
 * is never satisfied from an application Layer and is excluded from the
 * runtime's public requirements. Without a durable runtime the engine
 * provides an ephemeral pass-through that executes each Step once and
 * records nothing — the durable claim attaches to the runtime, not the Tool.
 */
export class DurableStep extends Context.Service<DurableStep, DurableStepService>()(
  "@effect-agent/engine/DurableStep",
) {}
