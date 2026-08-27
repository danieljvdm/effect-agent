Review this Effect v4 framework for concrete defects, not style.

- Public asynchronous operations return `Effect` or `Stream`; expected failures stay typed in `E`, requirements stay visible in `R`, and resources belong to `Scope`.
- Effect `Schema` owns persisted, transported, tool, and structured model values. Decode untrusted data at the boundary; do not cross it with assertions.
- Concrete Layers and platform choices belong at composition roots. Core domain modules cannot import Node assumptions or outward packages.
- The dependency direction is `core <- engine <- capabilities`, `core <- sandbox <- sandbox-local`, `core <- engine <- session <- storage`, with platform and consumer packages outermost.
- Use Effect AI primitives directly. Provider values are never canonical durable records.
- The canonical log is append-only; projections are disposable. Never claim exactly-once external effects or automatically replay an unresolved ordinary tool call.
- Bound concurrency and use structured Effect concurrency. Security decisions fail closed; model output is untrusted.
- Flag a new abstraction only when deleting it would remove real ownership, policy, or behavior.
- Tests must cover a concrete regression. Do not request speculative cleanup or unrelated hardening.
