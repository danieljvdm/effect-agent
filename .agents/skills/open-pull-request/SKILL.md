---
name: open-pull-request
description: Prepare, open, update, or land pull requests with brief summaries, architecture diagrams, API examples, and useful screenshots or videos.
---

# Pull requests

Give the reviewer a high-level account of what changed and why. Usually a
short paragraph or a few bullets is enough. Add a risk, limitation, or manual
deploy step only when it affects their decision. Skip code tours, investigation
history, routine CI recaps, and prescribed sections or accordions. Follow a
required repository PR template when present.

This is a public library. Keep PR descriptions self-contained and omit private
tracker links and issue IDs, including Linear references. Link to public code
or guides when they save the reviewer a search. For bugs, explain the observable
failure and why the fix addresses it; expand only when the cause is subtle.

Keep company/customer names, account identifiers, and private workspace links
out of PR text and linked evidence unless the user explicitly requests them.

## Explain architecture and APIs

Pick the smallest view that makes the change clear, and place it beside the
short explanation it supports. Prefer a diagram or example over a long prose
description; simple changes can stay prose-only.

- For changes to component ownership, boundaries, or data flow, include a
  focused Mermaid architecture chart. Use a sequence diagram when call order
  matters. Name the actual components, label the interactions, and make the
  changed responsibility or path clear without mapping the whole system.
- For new or changed APIs, show a concrete caller example: an HTTP request and
  response, or a typed function/SDK call and its result. Include the inputs,
  outputs, and error behavior relevant to the change. Use a small before/after
  diff when callers must migrate; show the complete example when the API is new.

Use fenced Mermaid and code blocks directly in the PR. A call tree or pseudocode
can replace a chart when it explains the change more clearly. Match diagrams
and examples to the final implementation, use safe fixture data, and distinguish
illustrative or expected output from output actually observed during validation.
Mark omitted context and schematic examples clearly. Include both a chart and
an API example when they answer different review questions, not just to fill
sections.

## Capture visible behavior

For UI or visible features, capture the final running implementation during
verification and reuse it for the PR. A screenshot is the default; use a short
video when the sequence matters, such as an agent exchange or animation. Both
are rarely needed. Nonvisual changes need no screenshots or recordings.

Use existing capture tools; load `playwright-cli` when available for browser capture. Keep
recordings focused, usually under 30 seconds, without changing product timing.
Use safe fixture data and review the image or whole clip once for correctness
and private content. Treat published assets as public; unreviewed media stays
local. If inspection is unavailable, use a safe alternative or report the
blocker. Do not build viewers, extract frame galleries, or reconstruct GitHub.

If the repository provides a `publish-pr-asset` task, publish reviewed media with:

```sh
vp run publish-pr-asset -- <file> <label> --caption "What this shows"
```

Use the returned Markdown in the PR. If the task is absent, use an existing
approved publication path or supported attachment tool, or report the exact local path; do not add a publisher
as part of opening a PR. Keep originals until publication succeeds. If it fails,
report the exact local path. Never extract browser cookies, expose credentials
in arguments, create asset branches, or invent an upload service.

Include nonvisual evidence only when it adds to the diff and CI. Mention
validation commands for unusual checks, checks CI cannot run, or results that
explain the behavior.

## Open or update the PR

For an already verified change, aim to publish within two minutes:

1. Read repository contribution instructions and any required PR template.
   Check the base, full branch diff, and working tree for accidental changes.
   Reuse review, validation, and evidence already completed for unchanged inputs;
   `AGENTS.md` owns required checks. Validate the final branch state, including
   commit-hook changes, and ground claims in the diff, CI, or verified artifacts.
2. Use Conventional Commits for commits and the title:
   `type(scope): imperative summary`. Follow repository conventions, omit an
   unhelpful scope, and keep commits focused. Commit and push the intended
   changes, preserving unrelated work. Rewrite only your own unshared commits;
   get approval before rewriting user-authored or published history.
3. Open or update the PR with the short body, useful diagrams or API examples,
   and existing evidence. With `gh`, use `--body-file` for multiline text.
4. Read back base/head, title, body, links, attached artifacts, and check status
   once with `gh pr view`, then return the URL. Report pending or failed checks
   accurately. No GitHub browser inspection or wait for CI is required to open it.

Follow `AGENTS.md` for merge approval and required checks; opening a PR does
not authorize merging it.

When an existing draft PR is the subject, interpret "open it" or "ready it"
as making it ready for review unless the user asks to view it. State the intended
transition before acting; use `gh pr ready` rather than opening a browser.
