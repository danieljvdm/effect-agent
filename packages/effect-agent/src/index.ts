/**
 * Agent authoring, execution, and capabilities through one package.
 * Each export belongs to one constituent package; those packages must not
 * forward each other's names. Effect itself uses module namespaces at its
 * root; this umbrella retains the framework's flat named imports.
 *
 * Platform adapters stay scoped where their dependencies live:
 * `@effect-agent/platform-node`, the storage adapters,
 * `@effect-agent/sandbox-local`, `@effect-agent/platform-cloudflare`, and the
 * `@effect-agent/testing` dev kit.
 */
export * from "@effect-agent/capabilities";
export * from "@effect-agent/core";
export * from "@effect-agent/engine";
