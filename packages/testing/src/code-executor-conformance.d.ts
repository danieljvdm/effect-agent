import { CodeExecutor, SandboxImplementation } from "@effect-agent/sandbox";
import { Effect, Schema } from "effect";
declare const CodeExecutorConformanceViolation_base: Schema.Class<
  CodeExecutorConformanceViolation,
  Schema.TaggedStruct<
    "CodeExecutorConformanceViolation",
    {
      readonly caseName: Schema.String;
      readonly message: Schema.String;
    }
  >,
  import("effect/Cause").YieldableError
>;
/**
 * Shared `CodeExecutor` conformance (TEST-015). Every adapter — the
 * deterministic `unisolated` substitute and each isolated adapter — runs
 * `codeExecutorConformanceCases` verbatim. Enforcement cases that only genuine
 * isolation can prove (ambient network denial, synchronous CPU runaway
 * termination) are NOT here; they belong to isolated adapters only
 * (testing spec §8.1).
 *
 * Cases assume the live `Clock` (the wall-clock case uses a short real
 * deadline) and take one fresh executor pass per case, so a suite may share
 * one executor Layer across cases.
 */
export declare class CodeExecutorConformanceViolation extends CodeExecutorConformanceViolation_base {}
export interface CodeExecutorConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<void, CodeExecutorConformanceViolation, CodeExecutor>;
}
export interface CodeExecutorConformanceOptions {
  /** The posture the adapter under test must stamp on results and errors. */
  readonly implementation: SandboxImplementation;
}
export declare const codeExecutorConformanceCases: (
  options: CodeExecutorConformanceOptions,
) => ReadonlyArray<CodeExecutorConformanceCase>;
export {};
