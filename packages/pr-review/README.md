# @effect-agent/pr-review

A bounded, fail-closed GitHub pull-request reviewer built on effect-agent's public APIs. Read-only
Agents review host-partitioned pull-request evidence
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
  describeReviewModel,
  fullReviewExecutionContextLayer,
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
    .pipe(
      Effect.provide(
        fullReviewExecutionContextLayer("explicit direct full review").pipe(
          Layer.provideMerge(Layer.merge(gitHubReviewLayers(target), openAiClientLayer)),
        ),
      ),
    );
});
```

The run's requirement channel keeps every real dependency visible: the
`PullRequestSource`, `ReviewPublisher`, `ReviewAdjudicationHost`, and
`ReviewExecutionContext` ports, the provider client, and the handler Layer of
any extra tool you add. Direct callers provide an explicit full-review context
as above; the packaged action supplies its selected incremental or full range.
Anthropic is equally supported
(`makeAnthropicReviewModel`, `anthropicClientLayer`), and the factory accepts
any Effect AI Model.

The built-in OpenAI binding can opt each request into Fast mode:

```ts
const reviewer = PrReview.make({
  model: makeOpenAiReviewModel("gpt-5.6-sol", undefined, "fast"),
  modelLabel: describeReviewModel("openai", "gpt-5.6-sol", undefined, "fast"),
});
```

`fast` is the only packaged service-tier value. The helper sends it as the
OpenAI Responses `service_tier`; omitting it leaves the request unset so the
OpenAI project default applies. The Action and CLI reject a service tier when
the selected provider is not OpenAI. The model label includes the tier and
therefore changes both ordinary and profile fingerprints.

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

Model output is untrusted. The host revalidates anchors, enforces `maxFindings`, and publishes only
after the run settles. `PrReview.makeFanOut` partitions bounded evidence and schedules general and
specialist discovery for every unit, followed by independent candidate verification. Verifiers
receive the exact claims and enough neighboring evidence to reject a plausible mistake, but no
discovery reasoning.

Each failed pass retries once, then carries its unit forward. The host accepts only confirmed
candidate IDs, discards anchors outside assigned evidence, deduplicates claims in plan order, and
derives the verdict from validated severities. Shared `guidance` reaches discovery and verification.

When GitHub omits a textual patch, the source reads bounded, strict UTF-8 base/head content and
labels it with non-anchorable `B`/`H` lines. Invalid UTF-8, binary NUL content, missing sides, and
oversized files remain unreviewable and keep input coverage incomplete. Exclude them with ignore
globs only when that is intentional.

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
that every semantic risk was recognized. Hosts and UI use `inputCoverage` and `assurance`.
The flat reviewer has path-input accounting but no independent verifier, so
its assurance is `unverified` and the Action check cannot report success from
that shape.

## Maintainer adjudication

A maintainer can settle a finding without changing code, from the pull request itself:

- On a finding's inline thread, reply `/adjudicate accepted-risk|refuted|obsolete[: reason]`.
  The thread names the target, so the verb alone suffices.
- For an unanchored concern, comment
  `/adjudicate <disposition> "<exact concern title>"[: reason]` in the PR conversation. The quoted
  title is required and must match exactly.

An adjudicated identity leaves active findings, verdict counts, and the check conclusion; it
renders in a collapsed "Adjudicated" section instead, and the final audit distinguishes fixed,
adjudicated, and still-open items. The reviewer prompt names each adjudication so the model does
not re-raise it without materially new evidence. Identity is exact — path, line range, and title
for findings, title alone for concerns — so a materially different finding at the same location is
untouched. Adjudications persist in the signed review state, and the skip-unchanged path re-reads
them, so an adjudication lifts a blocking check without a new commit.

**Authorization is fail-closed**: only comments whose `author_association` is OWNER, MEMBER, or
COLLABORATOR adjudicate. Anything else — third parties, bots, malformed commands — is ignored
(logged at debug). Later adjudications of the same identity win by comment creation order, bounded
at 20 stored entries (oldest dropped with a logged notice). If either GitHub listing surface fails,
or one thread exceeds the bounded authorized-command history, fresh collection is discarded and
the stored set stands unchanged.

**Rejected alternative — parsing free-text rebuttals** ("this is fine because …" replies): only an
explicit, authorized verb is auditable and fail-closed. Inferring intent from prose would let model
output or third-party comments silently suppress findings, and nobody could later say which comment
dismissed what.

## What a posted review looks like

The host derives callouts, statistics, walkthroughs, inline comments, and the final prompt from
validated findings. Model prose cannot choose the verdict or smuggle paths outside the changeset.
The [Action guide](../../action/README.md) documents the rendered review and workflow outputs.

## Swap a port

Tools observe the pull request only through `PullRequestSource`; publication
happens only through `ReviewPublisher`. Provide your own Layers to review
any diff-shaped input or publish elsewhere. The GitHub REST adapters are
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
models that call the real Tools. Tests need no network or credentials and exercise every ordinary
gate.

## Action and CLI

The prebuilt GitHub Action handles signed incremental state, fail-closed scope selection, sticky
progress, stale-review retirement, and final audits. See its [setup and behavior guide](../../action/README.md)
and [input reference](../../action/action.yml). Custom hosts can import `runReviewAction` from
`@effect-agent/pr-review/action`.

Run the CLI with
`bun src/cli.ts --repo owner/name --pr 123 [--post] [--provider anthropic] [--service-tier fast] [--fan-out]`.

## Bounds, spelled out

Every definition has a finite `AgentPolicy` and run-level `UsageBudgetLimits`. The reviewer rejects
file versions beyond 200k bytes or characters and patchless B/H renderings beyond 220k characters.
It reads at most 300 changed files. Each fan-out unit holds at most 12 files, 12 complete evidence
shards, and 240,000 evidence characters. At most eight units produce 24 attached children with
child concurrency capped at four. The result reports every capacity overflow. Any blocking
finding, input gap, failed pass, exhausted pass, mismatched candidate batch, or unsettled
verification prevents a green check.
