/**
 * The Effect Agent umbrella: the framework's complete pure surface — schema-
 * first Agent authoring (`@effect-agent/core`), the bounded ephemeral
 * interpreter (`@effect-agent/engine`), and operational capabilities
 * (`@effect-agent/capabilities`) — as one dependency-clean package, mirroring
 * how `effect` fronts the `@effect/*` satellites.
 *
 * Platform adapters stay scoped where their dependencies live:
 * `@effect-agent/platform-node`, the storage adapters,
 * `@effect-agent/sandbox-local`, `@effect-agent/platform-cloudflare`, and the
 * `@effect-agent/testing` dev kit.
 */
export * from "@effect-agent/capabilities";
export * from "@effect-agent/core";
export * from "@effect-agent/engine";

// Explicit re-exports resolve the star-export ambiguities so these names
// stay present on the umbrella: `capabilities` re-exports the two core-owned
// delegation-naming helpers, and both `engine` (type only) and `capabilities`
// (Schema value + type) declare CommandDrainPolicy and RunSchedulingOverride;
// the Schema forms win.
export { delegationToolPrefix, isDelegationToolName } from "@effect-agent/core";
export { CommandDrainPolicy, RunSchedulingOverride } from "@effect-agent/capabilities";
