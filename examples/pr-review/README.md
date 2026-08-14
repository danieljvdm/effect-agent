# Example: pull-request reviewer

A PR-review bot built on the public framework path: one bounded, read-only
Agent reviews a GitHub pull request and the host posts the validated result
back as a pull-request review. It runs as a GitHub Action via
[`action.yml`](action.yml) and the repository workflow
`.github/workflows/pr-review.yml`.

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

## Profiles

| Profile                       | Model                        | Gate                                              |
| ----------------------------- | ---------------------------- | ------------------------------------------------- |
| Offline (every ordinary gate) | prompt-aware scripted model  | none — no network, no credentials                 |
| Live (opt-in)                 | `claude-sonnet-5` by default | `EFFECT_AGENT_LIVE=1` **and** `ANTHROPIC_API_KEY` |

Run the live smoke: `EFFECT_AGENT_LIVE=1 ANTHROPIC_API_KEY=... bun run test`.

## CLI

```sh
bun src/cli.ts --repo owner/name --pr 123            # dry run: prints the plan
bun src/cli.ts --repo owner/name --pr 123 --post     # posts the review
bun src/cli.ts --model claude-opus-4-8 --post        # inside Actions: target from event payload
```

Environment: `ANTHROPIC_API_KEY` (required for the model),
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
      anthropic-api-key: ${{ secrets.ANTHROPIC_API_KEY }}
      github-token: ${{ secrets.GITHUB_TOKEN }}
```

The repository workflow skips silently when the `ANTHROPIC_API_KEY` secret is
absent, so forks and credential-less clones stay green.

## Honest limits

- Ephemeral deployment class `E`: one `AgentRuntime.run` per invocation, no
  durability claim, no exactly-once anything (the one external mutation is a
  single review POST after the run).
- The reviewer reads only files in the changeset — head versions, not the
  full tree.
- Pull requests beyond 300 changed files or 200k-character files are refused
  typed, never silently truncated.
