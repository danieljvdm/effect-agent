# @effect-agent/pr-review

A bounded, fail-closed GitHub pull-request reviewer built on the effect-agent
public surface. Read-only Agents review host-partitioned pull-request evidence
through typed ports; the host validates every finding anchor against the real
diff and posts one review after the run settles.

**Deployment class E (ephemeral).** One `AgentRuntime.run` per invocation, no
durability claim, and review posting is never exactly-once: a failed or
truncated run posts nothing.

## Use

```ts
import { Effect, Layer } from "effect";
import {
  PrReview,
  gitHubReviewLayers,
  resolveReviewTarget,
  makeOpenAiReviewModel,
  openAiClientLayer,
} from "@effect-agent/pr-review";

const reviewer = PrReview.make({ model: makeOpenAiReviewModel() });

const program = Effect.gen(function* () {
  const target = yield* resolveReviewTarget({ repository: "acme/api", number: 123 });
  return yield* reviewer
    .run({ post: true })
    .pipe(Effect.provide(Layer.merge(gitHubReviewLayers(target), openAiClientLayer)));
});
```

The run's requirement channel keeps every real dependency visible: the
`PullRequestSource` and `ReviewPublisher` ports, the provider client, and the
handler Layer of any extra tool you add. Anthropic is equally supported
(`makeAnthropicReviewModel`, `anthropicClientLayer`), and the factory accepts
any Effect AI Model.

## Adapt

```ts
const reviewer = PrReview.make({
  model: makeAnthropicReviewModel("claude-sonnet-5"),
  guidance: (mission) => [
    "This is an Effect codebase. Flag naked Promises in public APIs.",
    mission.changedFileCount > 50 ? "Large PR: prioritize breadth over nit depth." : "",
  ],
  ignore: ["**/*.lock", "dist/**"],
  maxFindings: 10,
  policy: { maxTurns: 20, maxToolCalls: 40, maxDuration: "10 minutes", toolConcurrency: 2 },
  extraTools: [MyReadonlyTool], // must be annotated ToolExecutionClass "readonly"
});
```

Every knob widens what goes INTO the review. What leaves it is not
configurable: model output is untrusted input, so finding anchors are
re-validated against the parsed unified diff (invalid ones are demoted into
the review body with the reason named, never trusted), the findings bound is
enforced host-side, and publication happens only through the
`ReviewPublisher` port after the run settles. `PrReview.makeFanOut` builds the
assured host-scheduled variant. The host deterministically partitions bounded
evidence, classifies high-risk units, requires a general discovery pass for
every unit, adds a fresh specialist discovery pass for every unit, and uses
deterministic host-classified risk categories as explicit focus labels. A
fresh verifier must confirm or reject every discovered candidate. Every pass
runs as a bounded evidence-only child dispatched directly by host code with
Effect structured concurrency — there is no coordinator model and no
delegation tool, so nothing depends on a model copying the plan correctly. A
pass that fails (child fault, malformed or misdirected output) is retried
once; a pass that stays failed is reported and its unit's paths carry forward
as retryable scope. Verifiers receive the exact candidate claims and the
complete bounded unit, including neighboring evidence that can falsify a
locally plausible claim; they do not receive discovery reasoning.
The host builds publishable findings and concerns from exact confirmed
candidate IDs and deduplicates byte-identical cross-pass claims in
deterministic plan order; a discovery claim anchored outside its assigned
evidence is discarded and counted, never published and never pass-fatal. The
summary and verdict are composed deterministically from the confirmed
severities. Shared `guidance` reaches both discovery and verification;
`maxFindings` remains a host-enforced publication cap.

GitHub may omit the `patch` field for large textual files as well as binary
files. The GitHub source recovers a missing patch by reading bounded, strict
UTF-8 base/head content: additions require the head, deletions require the
base, and other changes require both. Reviewers receive that content through
the ordinary diff-read tool with non-anchorable `B`/`H` line labels and must
report defects as review-body concerns. Invalid UTF-8, binary NUL content,
missing sides, and files beyond the per-side read bound leave a path with no
reviewable textual evidence. Such paths keep input coverage incomplete and are
carried for as long as they are part of the pull request — an unreviewable
change must never authorize a green check; exclude them deliberately with
ignore globs when that is intended.

## Assurance model

The public result deliberately separates two claims:

- **Input coverage** says every reviewable required path in the selected
  scope was assigned every deterministic bounded evidence shard and
  separately names partially assigned and over-capacity paths, the exact
  unassigned-shard count with a bounded identifier sample, undiffable paths,
  and source truncation. A large textual diff is split across shards and
  units instead of being called partial merely for exceeding one prompt
  chunk. Input coverage does not say the model understood the evidence.
- **Review assurance** says every scheduled general discovery pass, required
  independent specialist pass, and candidate-verification pass settled after
  at most one retry. It reports discovered, confirmed, rejected, unsettled,
  and discarded (invalid-anchor) candidate counts plus failed pass IDs. A
  failed pass is a reviewer-side gap: its unit's paths carry forward as
  retryable scope and the next incremental run re-reviews exactly them.

`settled` assurance means the configured work completed; it never means the
review is exhaustive or the pull request is defect-free. Risk classification
is deterministic host policy over paths and bounded text and intentionally
favors redundant work, but it cannot recognize every semantically risky
change. The specialist pass is context-independent redundancy, not a claim of
provider or model diversity. Running it for every unit prevents classifier
silence from suppressing scrutiny; the category labels still cannot prove
that every semantic risk was recognized. The legacy aggregate `coverage` field remains
for compatibility; new hosts and UI use `inputCoverage` and `assurance`.
The flat reviewer has path-input accounting but no independent verifier, so
its assurance is `unverified` and the Action check cannot report success from
that shape.

## What a posted review looks like

The body opens with a host-derived callout tier — `[!CAUTION]` when any
finding is blocking, `[!IMPORTANT]` for important findings, an ℹ️ blockquote
for nits, `✅` for a clean approval — computed from the validated severities,
never from model prose. Directly under it, one host-derived stats line names
the changeset size (`N files (+adds / −dels)`), the severity tally, and a
deterministic 1–5 review-effort estimate computed from the changeset shape
alone. Fan-out reviews add a second host-derived line naming input assignment,
discovery/specialist/verification settlement, and candidate dispositions.
Below the summary, a collapsed **📝 Walkthrough** table carries the
model's one-sentence per-file change summaries — walkthrough paths are
validated like finding anchors, so entries naming files outside the changeset
are dropped. Under fan-out only summaries a
successfully settled general pass actually reported for its own unit's paths
survive, so a child cannot smuggle or invent entries. Non-anchored `concerns`
(deletion plans, rollout sequencing,
coverage gaps, scope questions — things with no diff line to point at) render
as severity-tagged sections; demoted findings and findings carried from
unchanged scope collapse into counted `<details>` sections.

Each inline comment is headed by its severity plus an optional model-claimed
category chip (`**[⚠️ important · security] …**`), carries a committable
GitHub `suggestion` fence when independent verification settled the model's
suggestion as committable replacement source, and ends with a
collapsed **🤖 Prompt for AI agents** — a copy-paste instruction derived
host-side from the validated finding, opening with a fixed preamble telling
the receiving agent to treat the finding content as untrusted review data.
The review body repeats every finding — anchored, demoted, and carried — in
one consolidated prompt block so nothing needs to be collected by hand. The
footer names the model binding, the observed token usage, and links to the
workflow run; an invisible metadata comment pins the reviewed head commit so
later readers know when line callouts have gone stale.

## Swap a port

Tools observe the pull request only through `PullRequestSource`; publication
happens only through `ReviewPublisher`. Provide your own Layers to review
anything diff-shaped or publish anywhere else — the GitHub REST adapters are
one implementation, not the contract.

## Test what you adapted

```ts
import {
  fixturePullRequestSourceLayer,
  collectingReviewPublisherLayer,
  makeOfflineReviewerModel,
  makePromptKeyedModel,
} from "@effect-agent/pr-review/testing";
```

Deterministic in-memory adapters for both ports plus prompt-keyed scripted
models that walk the real tool surface — no network, no credentials, every
ordinary gate.

## Incremental Action reviews

Every completed Action review that can be signed carries bounded, versioned,
HMAC-authenticated continuity state: the exact PR/base/head lineage, profile
and settled-scope fingerprints, the still-unresolved findings and concerns,
any retryable unreviewed paths, and whether the run fully settled. The
baseline is monotone by design — a flaky or failed pass carries exactly its
own scope forward instead of freezing the baseline and reopening everything
reviewed since (the non-converging loop of issue #131). A later Action run
validates the state and reviews the GitHub comparison from that reviewed head
to the current head plus the carried unreviewed paths, not the complete
base...HEAD diff. Unchanged settled scope is not sent back to the model;
unchanged unresolved findings remain active; changed or reverted paths
invalidate their prior findings and receive fresh discovery and verification.
Whole overflow files are reviewed in bounded installments across pushes
through the same carry; a single file beyond total plan capacity and
undiffable paths stay explicitly incomplete instead — an unreviewable tail
never moves behind a green check. Non-anchored concerns are carried conservatively
until a full audit because they cannot be mapped safely to one path.

The state marker must be terminal, signed with the configured stable
`PR_REVIEW_STATE_SECRET`, authored by the configured review-posting bot, and
pinned to the reviewed commit. The expected author defaults to
`github-actions[bot]`; set `PR_REVIEW_AUTHOR_LOGIN` (or the Action's
`review-author` input) to `<app-slug>[bot]` when posting with a custom GitHub
App token. State lookup, authentication, schema, identity, ancestry, profile,
and comparison checks are fail-closed for scope selection: missing, stale,
incompatible, or truncated state/comparisons produce a visible full-diff
fallback. An ancestor base advance remains incremental and adds overlapping
PR paths as affected context; a materially changed base lineage falls back to
full unless the authenticated settled-scope fingerprint still matches. The
schema field retains the compatibility name `acceptedScopeFingerprint`. That
fingerprint hashes the ignore-filtered effective diff, patchless base/head
evidence, PR framing, and review-shaping profile while excluding commit IDs,
base ancestry, and unified-diff hunk coordinates. Re-running the same head or a
patch-equivalent rebase skips model execution by default ONLY when the stored
state fully settled — a state carrying unreviewed scope is always retried.
The preserved skip keeps its stored blocking/success conclusion and posts no
duplicate review comments. Missing/corrupt state and any fingerprint or
profile mismatch review conservatively.

After a new state-bearing Action review posts, prior marker-bearing bot reviews
are retired by default: their bodies become collapsed, superseded history,
resolved findings are struck through, and matching inline comments are
minimized as outdated. Only strictly older reviews from the same GitHub actor
are eligible, so copied markers and newer concurrent reviews are untouched.
The machine-state comments remain byte-identical and terminal, so an edited
body still participates in incremental state recovery.
This cosmetic pass is fail-open and can be disabled with the Action input
`retire-stale-reviews: "false"`.

Authentication is an explicit Effect service supplied by the Action host;
WebCrypto import/sign/verify failures stay typed. The terminal marker is
schema-branded and capped at 24,000 characters. If signing fails or state
exceeds that bound, the completed review is posted without continuity state
and with a bounded warning, so the next run safely performs a full review.

`review-mode: final` is the explicit bounded merge-readiness audit. It ignores
prior assurance state, plans fresh discovery over the full current PR
diff, verifies new candidates, and resets the incremental baseline; normal
`synchronize` events use `incremental` and do not perform this audit.
Because equivalence is based on the textual review surface, a base change that
alters runtime meaning without changing the diff/context is not detectable;
request `final` mode (or disable unchanged skipping) for that case.

## Hosts

- **GitHub Actions**: the repository ships a prebuilt node-runtime action
  supporting a committed review-profile document via `guidance-file` (this
  repository's own profile lives at `.github/review-guidance.md`)
  (`action/` at the repo root) — `uses` it with an API-key secret and nothing
  else. Fan-out is the Action default because the flat compatibility shape
  has no independent verifier and cannot settle review assurance. While a run executes it maintains one sticky, fail-open "review in
  progress" comment updated in place with the settled outcome
  (`progress-comment` input, default on; at-least-once with generation-fenced
  writes and best-effort duplicate cleanup — strict single-comment behavior
  comes from a per-PR workflow concurrency group), and its logs render one
  compact line per event (`log-level` input, default `Info`). For custom reviewers in
  CI, `@effect-agent/pr-review/action` exports `runReviewAction` (event
  resolution, typed draft/non-PR skips, bounded range selection, step
  outputs, and conservative check gate) to harness your own `reviewer.run`;
  pass `progressComment: true` to opt a custom harness into the sticky
  progress comment. A custom fingerprint-only harness remains source-compatible
  but cannot skip model work: unchanged skipping requires the profile fingerprint
  and authenticated state that prove the prior configured work settled.
- **CLI**: `bun src/cli.ts --repo owner/name --pr 123 [--post] [--provider anthropic] [--fan-out]`
  (also exported as the `./cli` entry).

Environment: `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` for the model,
`PR_REVIEW_STATE_SECRET` to authenticate incremental state,
`PR_REVIEW_AUTHOR_LOGIN` to match a custom review-posting bot (defaults to
`github-actions[bot]`),
`GITHUB_TOKEN` to post (optional for public-repository reads), and the
standard `GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL`
variables inside Actions.

## Bounds, spelled out

Finite `AgentPolicy` on every definition plus run-level `UsageBudgetLimits`
(tokens, tool calls, cost, duration). Reading either file version beyond 200k
bytes or characters is refused typed; patchless content is reviewable only
when its complete B/H rendering fits 220k characters. The changeset surface is
bounded at 300 files:
files beyond the bound are not fetched, and the review body reports
`Input exposed N of M changed files` instead of claiming completeness.
Fan-out capacity overflow is reported, never dropped. Every fan-out unit has
at most 12 files, 12 complete evidence shards, and 240,000 evidence characters;
one path may span multiple shards or units when necessary. If the eight-unit
plan capacity is exhausted, every affected path and the exact shard count are
reported, with a deterministic identifier sample bounded to one plan's capacity;
at most eight units produce at most 24 attached children (general and
specialist discovery plus one verification batch per unit), with child
concurrency capped at four. Any blocking active finding fails the Action
check. Any input gap—including an unassigned evidence shard—failed or
exhausted configured pass, mismatched candidate batch, or unsettled
verification is non-success rather than green. A settled
clean result is evidence that these bounded passes completed, not proof that
no defect exists.
