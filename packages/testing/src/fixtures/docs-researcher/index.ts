// ---------------------------------------------------------------------------
// Docs Researcher fixture (P7 internal agent #3): S2 durable delegation on DN
// with MCP-discovered content tools. See definition.ts / mcp.ts / harness.ts.
//
// Authoring friction note (WP7 input; real observations from writing this
// agent):
//
// 1. MCP discovery and Agent authoring do not meet in the type system. The
//    connector's validated Toolkit arrives as `Toolkit.Any`, while
//    `Agent.define` needs the statically typed toolkit — so the binding
//    between "what discovery served" and "what the child was authored
//    against" had to be re-proved by hand (`assertDiscoveryMatchesAuthoredToolkit`
//    re-deriving `Tool.getJsonSchema` on both sides). A framework helper that
//    checks a `McpConnection` against a static Toolkit value (or a typed
//    `connectMcp(request, expectedToolkit)`) would remove a whole class of
//    look-alike-toolkit mistakes.
// 2. The durable delegation declaration (`SubagentRuntimeOptions.durable
//    .targetDigests`) and the host's `DurableWorkerBinding.make(binding,
//    digests)` registration must agree byte-for-byte, but nothing shares the
//    value: the fixture exports both a string form and a decoded
//    `DefinitionDigests` form of the same digests to keep them from drifting.
//    One authoritative exported value consumed by both sides would be better.
// 3. Prompt-aware scripted models are boilerplate-heavy: every durable fixture
//    (S2 travel planner, P6 planner, this one) re-implements "counters outside
//    the Layer + decide-from-prompt". A shared `makePromptAwareCountingModel`
//    in the testing package is an easy WP7 simplification.
// 4. The delegation surface itself (Subagent.define + projections + policy)
//    was pleasant to author a second time — bounds and declassification live
//    exactly where a reviewer looks for them.
// ---------------------------------------------------------------------------

export * from "./definition.ts";
export * from "./harness.ts";
export * from "./mcp.ts";
