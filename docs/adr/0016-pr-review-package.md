# ADR-0016: Package the pull-request reviewer with a prebuilt GitHub Action

- Status: Accepted (owner-directed, 2026-08-14)
- Related decisions: [D-023](../DECISIONS.md#d-023--project-identity-and-distribution),
  [D-024](../DECISIONS.md#d-024--repository-toolchain-and-shape),
  [D-025](../DECISIONS.md#d-025--slim-toolchain-and-canonical-effect-source),
  [D-027](../DECISIONS.md#d-027--agent-definition-and-model-binding),
  [D-028](../DECISIONS.md#d-028--leaf-example-workspaces),
  [D-034](../DECISIONS.md#d-034--packaged-pull-request-reviewer-and-prebuilt-action)

## Context

`examples/pr-review` grew into the repository's most complete consumer: a bounded, read-only
reviewer with fail-closed publication (every finding anchor re-validated against the parsed
diff), a deterministic offline profile, and the first real S1 attached-delegation consumer (the
fan-out variant). The owner directed that it be made adoptable and adaptable by other
repositories.

Constraints in force: D-025 gates new packages on roadmap phases, and the roadmap table is
discharged; the umbrella `effect-agent` package had no recorded consumer edge; `dist` output is
gitignored and produced only at publish time; `@effect-agent/testing` must not become a
production dependency; a node-runtime GitHub Action must run from a bare checkout with no install
step.

## Decision

1. **Create `packages/pr-review` (`@effect-agent/pr-review`)** as the first package outside a
   numbered roadmap phase, under this owner decision (D-034). The phase gate itself is unchanged.
2. **Public surface**: schema-first review contracts (`ReviewMission`, `CodeReview`,
   `ReviewFinding`), the two ports (`PullRequestSource`, `ReviewPublisher`), GitHub REST
   adapters, the pure publication planner, both reviewer shapes, and a configuration factory —
   `PrReview.make` (guidance, policy override, findings bound, ignore globs, extra `readonly`
   tools) and `PrReview.makeFanOut` (static child guidance; packaged child/delegation bounds so
   the reservation and the child policy cannot drift). Every knob widens what goes into a review;
   anchor validation, the findings bound, and publication-after-settlement are unconditional.
3. **Entries**: `.` stays platform-free; `./testing` ships the fixture source, collecting
   publisher, and prompt-keyed scripted models with no `@effect-agent/testing` dependency;
   `./action` and `./cli` are the platform-node host entrypoints. Provider convenience bindings
   (OpenAI, Anthropic) are package dependencies because the host entrypoints need them; the
   factory itself accepts any Effect AI Model (D-027).
4. **Umbrella dependency edge**: the package depends on `effect-agent` (workspace) rather than
   the three scoped constituents — deliberately dogfooding the public surface. The dependency
   direction gains one outward tier: `core + engine + capabilities <- effect-agent <- pr-review`.
5. **Prebuilt Action**: `action/action.yml` (`using: node24`) runs the committed bundle
   `action/dist/index.mjs`, built from the package's action entry by `scripts/build-action.ts`
   (Bun's bundler via the sanctioned Bun script runtime). The committed bundle is a recorded
   exception to the dist-is-gitignored convention; `bun run check` rebuilds it to a scratch path
   and fails when stale.
6. **Claims**: deployment class E everywhere; no durability wording; posting a review is a single
   external mutation after Run settlement and is never claimed exactly-once (DUR-003). A non-PR
   or draft event is a typed skip. Model output — including delegated child findings — remains
   untrusted input end to end.
7. **The example remains, demoted to a consumer**: `examples/pr-review` now demonstrates the
   adaptation path (guidance, an extra readonly tool, ignore globs) against the published
   surface, per D-028.

## Consequences

- `docs/TOOLCHAIN.md`'s "Adding a package" checklist ran unchanged; the workspace tree,
  dependency direction, and package counts are updated alongside this ADR.
- The new package publishes on the beta dist-tag like its siblings (D-023). One manual step
  remains for the owner: registering `release.yml` as the npm trusted publisher for
  `@effect-agent/pr-review` on npmjs.com before the first CI publish.
- SEC-005's unified tool risk/redaction metadata surface is still `deferred`; the package uses
  the existing `ToolExecutionClass` annotation and enforces `readonly` for every extra tool at
  factory construction (fail-closed: unannotated tools read as `uncertain` and are rejected).
- The bundle freshness gate ties `bun run check` to the Bun version's bundler output; a Bun
  upgrade that changes emitted bytes requires one `bun run action:build` commit.
- FRICTION items recorded by the example were re-confirmed as public-API pressure: the factory
  builds bindings structurally because `Agent.withModel`'s conditional model type cannot
  re-resolve inside a generic body, and the run path still re-decodes `AgentResult.output`.

## Rejected alternatives

- **Keep the reviewer example-only and tell adopters to copy it.** Rejected: 2k lines of
  fail-closed logic is exactly the code that should not be copy-forked, and the example cannot be
  published (examples are private leaf workspaces by D-028).
- **A separate repository consuming the published packages.** Rejected for now: it would split
  the test surface from the framework it evidences and forfeit the in-repo gates; revisit at
  open-source preparation if the action's release cadence diverges from the framework's.
- **A composite Action that installs the workspace** (the previous `examples/pr-review`
  action.yml). Rejected as the public shape: it only works inside this monorepo's checkout and
  costs a full workspace install per review; kept lessons, replaced by the prebuilt bundle.
- **An npm `bin` for `npx` one-shots.** Rejected for now: the release script rewrites only
  `exports`, and a `bin` pointing at `dist` would be broken in the source-first working tree.
  The CLI ships as the `./cli` entry; revisit with the release tooling at open-source
  preparation.
- **Making the fan-out child/delegation bounds configurable.** Rejected: the `SubagentPolicy`
  reservation and the child `AgentPolicy` repeat the same numbers with no consistency check
  (FRICTION item 9); exposing them as a published API invites drift the framework cannot yet
  detect.

## Validation

- `packages/pr-review` test suites: ported flat and fan-out suites (including the S1 context
  isolation, declassification, containment, and E/R compile proofs), plus factory capability
  tests (guidance injection with intact contract, ignore-glob surface removal, findings-bound
  enforcement, readonly enforcement and cross-layer handler resolution for extra tools) and
  action harness tests (input parsing, typed skips, step outputs, verdict gate).
- `bun run check` includes the action bundle freshness gate; the bundle smoke-runs under Node 24
  and fails typed outside a pull-request environment.
- The repository's own PR-review workflow dogfoods `./action` with the fan-out variant.
