# Effect Agent pull-request work-order Action

This Action turns one authorized human reply to one inline review comment into one bounded
implementation attempt. It is deliberately separate from the read-only PR-review Action.

Do not invoke the Action as a single step. The five jobs below are part of its security contract:

- `admit` authenticates the GitHub event and persists its claim in a signed GitHub reply;
- `implement` has the model credential, a writable isolated exact-head checkout, and only a
  contents-read GitHub token; it has no GitHub write token;
- `checks` has neither provider nor publisher credentials and runs checks without network access;
- `publish` has the contents write token, but no PR checkout or provider credential; and
- `present` has only the pull-request write permission needed to update the existing journal reply.

The Action is precompiled. Pin `uses:` to a full 40-character commit SHA. A release tag or package
upgrade is not an immutable Action pin.

The journal is external GitHub repository state, not an Effect Agent DN or DC assembly. Recovery
depends on GitHub retaining and serving the authenticated review reply; the Action rereads that
reply across jobs and workflow runs, while short-lived Actions artifacts carry only one run's
bounded phase envelopes.

## Kommunikasie setup

For `reve-ai/kommunikasie`, add these repository secrets:

| Secret                       | Value                                                                                                                                                             |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `PR_WORK_ORDER_STATE_SECRET` | A stable random value of at least 32 characters, for example the output of `openssl rand -hex 32`. Do not rotate it while admitted attempts may still be running. |
| `OPENAI_API_KEY`             | The model credential used only by the `implement` job.                                                                                                            |

Add these repository variables:

| Variable                             | Kommunikasie value                                                               | Meaning                                                                                                                                 |
| ------------------------------------ | -------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `PR_WORK_ORDER_AUTHORIZED_ACTOR_IDS` | `3450486`                                                                        | Comma-separated stable numeric GitHub user IDs. Verify each login with `gh api users/LOGIN --jq .id`; never authorize by mutable login. |
| `PR_WORK_ORDER_SUPPORT_PATHS`        | `[]`                                                                             | Additional exact normalized paths the model may change. Keep this empty unless a specific support file is intentionally in scope.       |
| `PR_WORK_ORDER_CHECKS`               | `[{"name":"check","command":"vp","args":["run","check"],"timeoutSeconds":3600}]` | Trusted, host-authored checks. This matches Kommunikasie's Vite+ validation task.                                                       |

On github.com, `41898282` is the stable actor ID for `github-actions[bot]`; the workflow pins that
value as the only acceptable journal author. GitHub Enterprise Server users must look up and pin
their installation's bot actor ID instead.

Create `.github/workflows/effect-agent-pr-work-order.yml` on the default branch with the following
contents. The Action pin below is the commit that contains the reviewed Action source and matching
`dist/index.mjs` artifact.

```yaml
name: Effect Agent PR work order

on:
  pull_request_review_comment:
    types: [created]

permissions: {}

concurrency:
  group: effect-agent-pr-work-order-${{ github.repository_id }}-${{ github.event.comment.id }}
  cancel-in-progress: false

jobs:
  admit:
    name: Authenticate and persist admission
    if: ${{ github.event.comment.body == '@effect-agent fix this' && github.event.comment.in_reply_to_id }}
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions:
      contents: read
      pull-requests: write
    outputs:
      should-run: ${{ steps.work-order.outputs.should-run }}
    steps:
      - id: work-order
        name: Authenticate, authorize, and claim the dispatch
        uses: danieljvdm/effect-agent/work-order-action@9005bc66b88639fa5b0a16ff921359e58dc8cc43
        with:
          phase: admit
          artifact-directory: state
          github-token: ${{ github.token }}
          state-secret: ${{ secrets.PR_WORK_ORDER_STATE_SECRET }}
          state-author-id: "41898282"
          authorized-actor-ids: ${{ vars.PR_WORK_ORDER_AUTHORIZED_ACTOR_IDS }}

      - name: Transfer immutable admission
        if: ${{ steps.work-order.outputs.should-run == 'true' }}
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
        with:
          name: pr-work-order-${{ github.run_id }}-admission
          path: state/admission.json
          if-no-files-found: error
          retention-days: 1

  implement:
    name: Propose bounded patch
    needs: admit
    if: ${{ needs.admit.outputs.should-run == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 12
    permissions:
      contents: read
    outputs:
      candidate: ${{ steps.work-order.outputs.candidate }}
    steps:
      - name: Check out exact pull-request head without credentials
        uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4
        with:
          repository: ${{ github.repository }}
          ref: ${{ github.event.pull_request.head.sha }}
          path: worktree
          persist-credentials: false

      - name: Download immutable admission
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093 # v4
        with:
          name: pr-work-order-${{ github.run_id }}-admission
          path: state

      - id: work-order
        name: Run the bounded model implementer
        uses: danieljvdm/effect-agent/work-order-action@9005bc66b88639fa5b0a16ff921359e58dc8cc43
        with:
          phase: implement
          artifact-directory: state
          repository-path: worktree
          authorized-actor-ids: ${{ vars.PR_WORK_ORDER_AUTHORIZED_ACTOR_IDS }}
          provider: openai
          model: gpt-5.6-sol
          openai-api-key: ${{ secrets.OPENAI_API_KEY }}
          support-paths: ${{ vars.PR_WORK_ORDER_SUPPORT_PATHS }}
          checks: ${{ vars.PR_WORK_ORDER_CHECKS }}
          max-duration-minutes: "8"

      - name: Transfer implementation outcome
        if: ${{ always() }}
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
        with:
          name: pr-work-order-${{ github.run_id }}-implementation
          path: |
            state/proposal.json
            state/settlement.json
            state/implementation-terminal.json
          if-no-files-found: warn
          retention-days: 1

  checks:
    name: Validate in credential-free checkout
    needs: [admit, implement]
    if: ${{ needs.implement.outputs.candidate == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 75
    permissions:
      contents: read
    outputs:
      validated: ${{ steps.work-order.outputs.validated }}
    steps:
      - name: Check out exact pull-request head without credentials
        uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262 # v4
        with:
          repository: ${{ github.repository }}
          ref: ${{ github.event.pull_request.head.sha }}
          path: worktree
          persist-credentials: false

      - name: Download proposal and admission
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093 # v4
        with:
          pattern: pr-work-order-${{ github.run_id }}-*
          path: state
          merge-multiple: true

      - id: work-order
        name: Reproduce patch and run required checks
        uses: danieljvdm/effect-agent/work-order-action@9005bc66b88639fa5b0a16ff921359e58dc8cc43
        with:
          phase: checks
          artifact-directory: state
          repository-path: worktree
          checks: ${{ vars.PR_WORK_ORDER_CHECKS }}
          check-container-image: ghcr.io/voidzero-dev/vite-plus:0.2.6@sha256:de284eb61eb6ee5fe1da3824032ed6fb37827eecd597d0d796cacd4434f806ea

      - name: Transfer host check evidence
        if: ${{ always() }}
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
        with:
          name: pr-work-order-${{ github.run_id }}-checks
          path: |
            state/checked.json
            state/checks-terminal.json
          if-no-files-found: warn
          retention-days: 1

  publish:
    name: Atomically publish accepted patch
    needs: [admit, implement, checks]
    if: ${{ needs.checks.outputs.validated == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions:
      contents: write
      pull-requests: read
    steps:
      - name: Download accepted patch envelope
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093 # v4
        with:
          pattern: pr-work-order-${{ github.run_id }}-*
          path: state
          merge-multiple: true

      - name: Revalidate and compare-and-swap the pull-request head
        uses: danieljvdm/effect-agent/work-order-action@9005bc66b88639fa5b0a16ff921359e58dc8cc43
        with:
          phase: publish
          artifact-directory: state
          github-token: ${{ github.token }}
          state-secret: ${{ secrets.PR_WORK_ORDER_STATE_SECRET }}
          state-author-id: "41898282"
          authorized-actor-ids: ${{ vars.PR_WORK_ORDER_AUTHORIZED_ACTOR_IDS }}
          support-paths: ${{ vars.PR_WORK_ORDER_SUPPORT_PATHS }}
          checks: ${{ vars.PR_WORK_ORDER_CHECKS }}

      - name: Transfer publication outcome
        if: ${{ always() }}
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02 # v4
        with:
          name: pr-work-order-${{ github.run_id }}-publication
          path: state/publication-terminal.json
          if-no-files-found: warn
          retention-days: 1

  present:
    name: Settle the authenticated thread reply
    needs: [admit, implement, checks, publish]
    if: ${{ always() && needs.admit.outputs.should-run == 'true' }}
    runs-on: ubuntu-latest
    timeout-minutes: 10
    permissions:
      pull-requests: write
    steps:
      - name: Download all available host artifacts
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093 # v4
        with:
          pattern: pr-work-order-${{ github.run_id }}-*
          path: state
          merge-multiple: true

      - name: Update the one authenticated journal reply
        uses: danieljvdm/effect-agent/work-order-action@9005bc66b88639fa5b0a16ff921359e58dc8cc43
        with:
          phase: present
          artifact-directory: state
          github-token: ${{ github.token }}
          state-secret: ${{ secrets.PR_WORK_ORDER_STATE_SECRET }}
          state-author-id: "41898282"
          publication-attempted: ${{ needs.checks.outputs.validated == 'true' }}
```

The workflow file must already exist on the default branch. It runs only the immutable external
Action pin before checking out a pull-request head, so a pull request cannot replace the code that
authorizes or publishes its own patch. Keep all third-party Action and container references pinned
to immutable digests when updating this example.

To use the flow, an authorized user replies exactly `@effect-agent fix this` to a path-bearing
inline review comment on a same-repository pull request at its current head. Each reply is one
explicit attempt. A redelivery reuses the authenticated stored outcome and does not run the model
again; another exact reply creates another work order.

## Operational limits

- Fork pull requests, stale heads, stale source anchors, non-inline comments, mutable actor logins,
  and unsupported paths fail closed.
- Publication currently supports modifications to existing regular UTF-8 files only. It rejects
  additions, deletions, renames, copies, executable/mode changes, symlinks, submodules, binary
  patches, and paths outside the exact source/support allowlist.
- Dependency installation invokes the pinned image's Vite+ binary by absolute path with lifecycle
  scripts disabled and a PATH that excludes the pull-request checkout. Required checks run in a
  read-only, capability-dropped container with no network and no GitHub or provider secret.
- `not-applicable` and `needs-human` are successful no-publication settlements. The host never
  treats raw model prose as authoritative status and never resolves the review thread.
- GitHub's atomic expected-head commit provides compare-and-swap publication, not exactly-once
  external effects. A lost response after a publication request is reported as publication
  uncertainty and is not automatically replayed.
