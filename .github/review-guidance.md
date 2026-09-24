Review this Effect v4 framework for precise contracts, necessary scope, and meaningful regression evidence. Find actionable behavioral and repository-policy defects, including on less common supported paths. A passing build, a detailed PR description, or complete diff coverage does not establish those contracts. There is no finding quota; a clean review is valid after investigating the material risks.

## Review the supported contract

- Reconstruct the promised behavior from public APIs, documentation, base behavior, and callers. Treat the implementation and PR's proof claims as proposals to verify. Follow changed behavior through unchanged consumers and guards; resolve material questions with source reads before concluding there are no findings.
- Represent distinct supported wire shapes with explicit schemas and normalize at the boundary. Check absent, null, incomplete, failed, and completed values where the protocol distinguishes them. Supporting another valid shape must not admit malformed payloads, invent missing data, or make required canonical fields optional. Defaults need a protocol or existing-contract basis.
- Reuse canonical or generated schemas and derive variants from their literals. Investigate copied status lists or parallel representations when they can disagree; identify the concrete mismatch rather than requesting deduplication on appearance alone. Preserve useful partial data on failure.
- Trace the same changed contract across related paths: streaming and complete results, run and stream APIs, fresh execution and recovery, checkpoint replay and canonical replay. Investigate only paths that share the affected contract. For tool calls, check parameter completion, validation, call/result pairing, escaping, and duplicate prevention.
- For new durable records or transitions, follow append, accounting, replay, checkpoint creation, and retirement. Check failure or ownership loss on both sides of the mutation: which work may repeat, which committed fact prevents repetition, and whether recovery preserves usage and authorization. Check supported old data and Runs without the new feature when shared recovery behavior changes.
- Trace schema decoding/encoding and user callbacks through their error handlers to the public `E` and `R` types. Look for erased requirements, widened errors, swallowed defects or interruption, and resources whose finalizers no longer run. Check the supported failure path, not just the successful return type.

## Architecture and ownership

- Public asynchronous operations return `Effect` or `Stream`; expected failures stay typed in `E`, requirements stay visible in `R`, and resources belong to `Scope`.
- Effect `Schema` owns persisted, transported, tool, and structured model values. Decode untrusted data at the boundary; do not cross it with assertions.
- Concrete Layers and platform choices belong at composition roots. Core domain modules cannot import Node assumptions or outward packages.
- Storage adapters, workflow, testing, and PR-review packages depend inward on `effect-agent`; platform packages compose it with selected adapters. Within effect-agent, dependencies point inward: `core <- engine <- capabilities <- durable` and `core <- sandbox <- capabilities`.
- Use Effect AI primitives directly. Provider values are never canonical durable records.
- The canonical log is append-only; projections are disposable. Never claim exactly-once external effects or automatically replay an unresolved ordinary tool call.
- Bound concurrency and use structured Effect concurrency. Security decisions fail closed; model output is untrusted.

## Make scope earn its place

- Inspect each added public API, helper, service, configuration option, and compatibility branch for the responsibility it owns. Prefer an existing primitive or a local guard when it provides the same supported behavior. Preserve mechanisms that enforce distinct ownership, policy, isolation, or lifecycle guarantees.
- Unnecessary mechanisms are reviewable when you can name the added caller burden, duplicate source of truth, or maintenance obligation and show a concrete simpler replacement that preserves the contract. Explain what can be removed and why no supported behavior or ownership boundary needs it. Complexity or line count alone is not a finding; do not request unrelated cleanup or speculative extensibility.

## Make regression evidence earn its place

- Default to no new test automation. Require a concrete uncovered failure and a reason existing checks or direct workflow evidence are insufficient before accepting new tests, fixtures, assertions or matrix growth. A current incident, provenance link or public-library label alone is insufficient. Never request post-implementation unit tests or a larger integration/E2E substitute for unnecessary tests. Read `.agents/skills/testing/SKILL.md`; do not request speculative cleanup or unrelated hardening.
- Check whether existing or changed tests exercise the boundary where a suspected failure occurs. A decoder regression needs the actual decoding path; a tool lifecycle regression needs the relevant event sequence; a recovery claim needs the persisted/reopened boundary. A PR's reported local proof is evidence to inspect, not proof of every adjacent contract.
- Use independent observable expectations. A test that repeats implementation constants, mocks away the failing boundary, or requires one internal mechanism may pass with the bug present. Identify the specific failure it misses or the valid implementation it wrongly rejects, rather than requesting generic coverage.
- Favor representative cases for distinct outcomes. Cross-products need interacting dimensions; repeated fixtures and incidental assertions need distinct regression value. Recommend removing redundant scaffolding only while preserving meaningful boundary, security, recovery, and cleanup coverage.
- Check changed inference, success/failure narrowing, and nullable or required fields against the declared public contract. Existing types and checks may already cover the risk. Missing test files alone are not a finding; follow the repository's testing policy and name the concrete uncovered regression before requesting automation.

## Keep explanations proportional

- Enduring contracts belong in API comments; guides explain ownership and use. Check that material behavior changes and supported limitations are described where consumers encounter them, including less common supported uses.
- Changesets should use one or two consumer-facing imperative sentences; PR descriptions should state the concrete failure and resulting behavior. Suggest wording changes only when they correct a misleading contract, missing migration requirement, or material ambiguity. Do not spend findings on prose preferences or investigation-history cleanup alone.

## Dependencies passed as parameters

Treat dependency drilling introduced or extended by the diff as an actionable architecture defect, not a style nit. Inspect changed function signatures, options objects, and helper factories for service instances, clients, stores, runtime bindings, or effectful callbacks passed as dependencies. A single dependency parameter counts; it need not be a large `deps` bag or pass through multiple helpers.

- Operations acquire required services with `yield* Service`, keeping requirements visible in `Effect` or `Stream`'s `R` channel. Flag helpers that take a service as an argument even when their caller yielded it first. Moving the same dependency into an options object or factory closure does not fix the problem.
- Recommend the existing service or inward port, with concrete implementations provided at the composition root. Do not introduce a wrapper service solely to hold a dependency bag.
- Keep request/domain data and pure transformation callbacks as explicit arguments. Layer configuration and foreign runtime values entering an adapter's construction boundary are legitimate. A service implementation may acquire dependencies during Layer construction and close over them when those requirements remain visible in the Layer's input type. These exceptions do not justify passing services through business operations or internal helpers.
- Cite the changed parameter and its use, explain the requirement hidden from `R` and the caller burden, and name where the dependency should be yielded or provided. Report dependency drilling as P1, a merge-blocking architecture contract violation: it bypasses Effect's requirement tracking and Layer composition. Working runtime behavior does not lower its priority. No production crash or separate behavioral failure is required. Do not audit unrelated pre-existing signatures.

Before recording a finding, check the strongest guard or documented exception that could disprove it. Cite the changed location and supported trigger or explicit policy breach, explain the consequence, and give a focused correction. Keep independent root causes separate; do not repeat one issue across every affected path.

Severity: dependency drilling is explicitly P1 as defined above. Otherwise use P0 for unconditional critical failures, P1 for core failures, lost required work, or unsafe supported operations, P2 for actionable nonblocking defects (including demonstrated unnecessary mechanisms), and P3 sparingly for minor issues. Report established findings without inflating severity or inventing failures to produce a nonempty review.
