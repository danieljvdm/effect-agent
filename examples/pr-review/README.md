# Example: pull-request reviewer

A PR-review bot built on the public framework path: one bounded, read-only
Agent reviews a GitHub pull request and the host posts the validated result
back as a pull-request review. It runs as a GitHub Action via
[`action.yml`](action.yml) and the repository workflow
`.github/workflows/pr-review.yml`.

It ships two reviewer variants over the same `ReviewMission -> CodeReview`
contract: the flat single-agent reviewer, and a subagent **fan-out** variant
(the repository's first real S1 attached-delegation consumer) for changesets
one context window cannot hold.

## Shape

- **Definition** (`src/review-agent.ts`): `Agent.define("pr-reviewer")` with a
  `ReviewMission` input, a structured `CodeReview` output, and three
  `readonly` Effect AI Tools — `list_changed_files`, `read_file_diff`
  (R-numbered annotated hunks), and `read_file` (head-version slices). Every
  bound is finite (`AgentPolicy`), and run-level `UsageBudgetLimits` add cost
  and token ceilings.
- **Ports** (`src/source.ts`, `src/github.ts`): tools observe the pull
  request only through `PullRequestSource`; publication happens only through
  `ReviewPublisher`. GitHub REST adapters provide both for real runs; an
  in-memory fixture source and a collecting publisher serve tests and dry
  runs.
- **Fail-closed publication** (`src/render.ts`): model output is untrusted
  input. Every finding anchor is re-validated against the parsed unified
  diff; findings that fail validation are demoted into the review body
  instead of trusted. Suggestions publish as GitHub ```suggestion blocks.
- **No agent-loop mutation**: the reviewer cannot post anything. The host
  posts one review after the run settles — so a failed or truncated run
  publishes nothing, and the bot defaults to `COMMENT` events
  (`--apply-verdict` opts into APPROVE/REQUEST_CHANGES).

## Fan-out variant (S1 attached delegation)

The flat reviewer reads every diff in one context window, so a large pull
request hits its 24-tool-call bound and truncates (observed on a 310-file PR).
The fan-out variant (`src/fan-out-review-agent.ts`, `src/review-units.ts`,
`src/fan-out-profiles.ts`) removes that wall without removing any bound:

- **Deterministic unit planning**: `planReviewUnits` groups the changeset by
  path order (directory affinity) into size-budgeted units — at most
  8 units of at most 12 files. Grouping is host code surfaced through the
  read-only `list_review_units` tool, not model prose. Files without a
  textual diff and files beyond the fan-out capacity are reported in the
  plan, never silently dropped.
- **One child per unit, plan-validated**: `delegate_file_review` is a real
  `Subagent.define` delegation targeting the `FileReviewer` child, whose
  toolkit is only `read_file_diff` + `read_file` (no changeset listing) over
  a plan-scoped source view (`fanOutScopedSourceLayer`). Coordinator output
  is untrusted: a delegation request must name a planned unit with exactly
  its planned paths or it fails typed before any child budget is reserved,
  and a child report claiming an unplanned unit — or findings outside its
  unit's paths — fails typed at the declassification boundary. Children are
  bounded by `SubagentPolicy` (≤ 8 children, ≤ 3 concurrent, per-child
  turn/tool/duration budgets aligned with the child's own `AgentPolicy`,
  plus explicit aggregate token/cost caps).
- **Failure containment, honestly reported**: `Subagent.define` fixes
  `failureMode: "error"`, which would abort the whole run on one failed
  unit. The coordinator's toolkit therefore carries a same-name Tool value
  with `failureMode: "return"` (handlers resolve by Tool name), so a typed
  unit failure reaches the model as a failed tool result; the coordinator
  must name it in its summary ("unit-002 unreviewed: AgentPolicyError") and
  never retries it.
- **Host-enforced merge, same trust boundary**: the coordinator's findings
  array is a proposal only. `projectResult` collects every validated child
  finding host-side, and `finalizeFanOutReview` replaces the model's
  findings with the mechanical merge (`rankAndDedupeFindings`) and
  re-derives the verdict — the coordinator can neither invent a finding,
  drop a blocking one, nor approve past one. The host then validates every
  anchor against the parsed diff before publishing, unchanged.

Offline tests script BOTH models (coordinator and children) prompt-keyed, so
fan-out, merging, honest unit failure, and budget enforcement all run
deterministically on every gate.

## Profiles

| Profile                       | Model                       | Gate                                           |
| ----------------------------- | --------------------------- | ---------------------------------------------- |
| Offline (every ordinary gate) | prompt-aware scripted model | none — no network, no credentials              |
| Live (opt-in)                 | `gpt-5.6-sol` by default    | `EFFECT_AGENT_LIVE=1` **and** `OPENAI_API_KEY` |

Run the live smoke: `EFFECT_AGENT_LIVE=1 OPENAI_API_KEY=... bun run test`.

## CLI

```sh
bun src/cli.ts --repo owner/name --pr 123            # dry run: prints the plan
bun src/cli.ts --repo owner/name --pr 123 --post     # posts the review
bun src/cli.ts --repo owner/name --pr 123 --fan-out  # subagent fan-out variant
bun src/cli.ts --model gpt-5.6-terra --post          # inside Actions: target from event payload
```

Environment: `OPENAI_API_KEY` (required for the model),
`GITHUB_TOKEN` (required to post; optional for public-repository reads),
`GITHUB_REPOSITORY` / `GITHUB_EVENT_PATH` / `GITHUB_API_URL` (provided by
GitHub Actions).

## GitHub Action

```yaml
permissions:
  contents: read
  pull-requests: write
steps:
  - uses: actions/checkout@v4
  - uses: ./examples/pr-review
    with:
      openai-api-key: ${{ secrets.OPENAI_API_KEY }}
      github-token: ${{ secrets.GITHUB_TOKEN }}
      fan-out: "true" # optional: subagent fan-out variant (default flat reviewer)
```

The repository workflow skips silently when the `OPENAI_API_KEY` secret is
absent, so forks and credential-less clones stay green.

## Honest limits

- Ephemeral deployment class `E`: one `AgentRuntime.run` per invocation, no
  durability claim, no exactly-once anything (the one external mutation is a
  single review POST after the run).
- The reviewer reads only files in the changeset — head versions, not the
  full tree.
- Pull requests beyond 300 changed files or 200k-character files are refused
  typed, never silently truncated.
- The fan-out variant's capacity is 8 units × 12 files; diffable files
  beyond it are reported as unreviewed in the plan and summary. The run-level
  `UsageBudget` observes only the coordinator's own usage — children are
  bounded by the delegation `SubagentPolicy` and the child `AgentPolicy`,
  not silently by the parent's budget.
