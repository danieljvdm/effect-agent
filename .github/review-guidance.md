This is the effect-agent framework repository: an Effect v4, schema-first agent runtime whose security decisions fail closed and whose model output is untrusted input. Review architecture first, correctness second, style never — the toolchain owns formatting and lint.

Architecture review method — apply to every changed service, Layer, tool, handler, or schema:

- Ownership and authority. Every capability has exactly one owner. Flag hidden authority: direct use of time, randomness, IDs, configuration, credentials, HTTP, persistence, or mutable globals inside business logic instead of an owned service; the module selecting a concrete Layer must own that implementation choice.
- Hidden requirements. Dependencies are yielded from context, never drilled as constructor arguments, dependency bags, or captured values. Flag Effect.provide or concrete Layer construction inside business operations or tool handlers (composition roots and program edges only), and any runtime lookup that bypasses the visible R channel.
- Typed channels. Expected failures stay enumerated in E. Flag catch-alls that swallow error tags, silent widening to unknown/Error/any, expected failures escaping as defects, and defects downgraded to expected failures. Preserve irreducible foreign causes as diagnostic fields inside concrete domain errors instead of broadening a public error channel.
- Schema boundaries. Untrusted or external data is decoded once, at the earliest boundary that owns its meaning. Flag type assertions crossing a Schema boundary, unvalidated JSON.parse, structural probes where a decode belongs, and a second Schema for the same logical model where a transformation would do.
- Resource safety. Every acquired resource belongs to a Scope. Flag daemon fibers, missing finalizers on new acquire paths, and unbounded concurrency — tool and task batches use structured concurrency with Semaphore permits, never a detached Promise scheduler.
- Pass-through abstractions. Apply the deletion test: a wrapper that only yields a service and forwards one method, or an abstraction whose removal leaves equally clear code, should be folded into its owner. Do not propose a wrapper, service, or Schema merely for symmetry.
- Public surface. Public asynchronous operations return Effect or Stream, never naked Promises; Promise conversion exists only at external boundaries.

Framework invariants — violations are blocking:

- No code, comment, or document may claim exactly-once external side-effect execution; recovery is at-least-once. The word "durable" must name the DN or DC assembly.
- The canonical log is append-only; projections and checkpoints are disposable derivatives.
- Dependency direction is inward-only: core <- engine <- capabilities; core <- sandbox <- sandbox-local; core <- sandbox <- capabilities (the CodeExecutor port); core <- engine <- session <- storage adapters; platform packages outermost; core + engine + capabilities <- effect-agent (umbrella) <- pr-review. An inward package importing an outward one is blocking.
- Effect AI primitives are used directly — no framework-owned copies of Tool, Toolkit, LanguageModel, Prompt, Response, or Model; provider SDK values never become canonical records.
- Model output and delegated child results are untrusted input: flag any trust of model-supplied paths, ids, ranges, or anchors without fail-closed validation, and any automatic replay of an unresolved ordinary tool call.
- Node platform assumptions must not enter core domain modules.

Testing changes: tests are deterministic — no wall-clock sleeps; typed exits are asserted, not just success; a shared test Layer's name matches the behavior it honestly provides, and partial fixtures stay local. New state transitions need deterministic tests; new durable mutations need before/after failpoints.

Repository facts that pre-empt false findings: the committed `action/dist` bundle is excluded from your view by configuration, and `bun run check` fails whenever it is stale — never report bundle staleness or absence. Lockfiles, changelogs, and dev-kit-managed skill copies are likewise excluded by configuration, not missing from the change. Coverage-exception rows in docs/REQUIREMENTS.md MUST be deleted once an executable test title references the ID — the coverage gate fails on stale rows, so a removed row alongside new ID-titled tests is the required disposition, never lost traceability.

Severity mapping: blocking = the framework invariants above, hidden authority over external effects, unchecked external data, and expected failures escaping the typed channel. Important = dependency drilling, hidden Layer requirements, duplicated capabilities, unjustified assertions, pass-through abstractions, and dishonest test substitutes. Nit = naming and co-location. End honest: prefer an explicit keep over an invented finding.
