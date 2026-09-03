---
name: open-pull-request
description: Prepare, open, update, or land pull requests with brief summaries, architecture diagrams, API examples, and useful screenshots or videos.
---

# Pull requests

Give the reviewer a high-level account of what changed and why. Usually a
short paragraph or a few bullets is enough. Add a risk, limitation, or manual
deploy step only when it affects their decision. Skip code tours, investigation
history, routine CI recaps, and prescribed sections or accordions.

Write for public-library contributors. Keep company/customer names, account
identifiers, and private workspace links out of PR text and linked evidence
unless the user explicitly requests them.

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
Include both a chart and an API example when they answer different review
questions, not just to fill sections.

## Capture visible behavior

For UI or visible features, capture the final running implementation during
verification and reuse it for the PR. A screenshot is the default; use a short
video when the sequence matters, such as an agent exchange or animation. Both
are rarely needed. Nonvisual changes need no screenshots or recordings.

Use existing capture tools; load `playwright-cli` when available for browser
capture. Keep recordings focused, usually under 30 seconds, without changing
product timing.
Use safe fixture data and review the image or whole clip once for correctness
and private content. Treat published assets as public; unreviewed media stays
local. If inspection is unavailable, use a safe alternative or report the
blocker. Do not build viewers, extract frame galleries, or reconstruct GitHub.

Publish reviewed media through an existing repository task or supported
attachment tool. Use the returned Markdown in the PR. Keep originals until
publication succeeds; if no upload path is available or it fails, report the
exact local path. Never extract browser cookies, expose credentials in arguments,
create asset branches, or invent an upload service.

## Open or update the PR

For an already verified change, aim to publish within two minutes:

1. Check the base, branch diff, and working tree for accidental changes. Reuse
   review, validation, and evidence already completed for unchanged inputs;
   `AGENTS.md` owns required checks.
2. Use Conventional Commits for commits and the title. Commit and push the
   intended changes, preserving unrelated work and published history.
3. Open or update the PR with the short body, useful diagrams or API examples,
   and existing evidence. With `gh`, use `--body-file` for multiline text.
4. Read back base/head, title, and body once with `gh pr view`, then return the
   URL. No GitHub browser inspection or wait for CI is required to open it.

Follow `AGENTS.md` for merge approval and required checks; opening a PR does
not authorize merging it.

When an existing draft PR is the subject, interpret "open it" or "ready it"
as making it ready for review unless the user asks to view it. State the intended
transition before acting; use `gh pr ready` rather than opening a browser.
