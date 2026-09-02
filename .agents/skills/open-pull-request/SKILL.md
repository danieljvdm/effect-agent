---
name: open-pull-request
description: Prepare, open, update, or land pull requests. Write brief summaries and capture or publish screenshots and videos for review.
---

# Pull requests

Give the reviewer a high-level account of what changed and why. Usually a
short paragraph or a few bullets is enough. Add a risk, limitation, or manual
deploy step only when it affects their decision. Skip code tours, investigation
history, routine CI recaps, and prescribed sections or accordions.

Write for public-library contributors. Keep company/customer names, account
identifiers, and private workspace links out of PR text and linked evidence
unless the user explicitly requests them.

For UI or visible features, capture the final running implementation during
verification and reuse it for the PR. A screenshot is the default; use a short
video when the sequence matters, such as an agent exchange or animation. Both
are rarely needed. Nonvisual changes need no media.

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

For an already verified change, aim to publish within two minutes:

1. Check the base, branch diff, and working tree for accidental changes. Reuse
   review, validation, and evidence already completed for unchanged inputs;
   `AGENTS.md` owns required checks.
2. Use Conventional Commits for commits and the title. Commit and push the
   intended changes, preserving unrelated work and published history.
3. Open or update the PR with the short body and existing evidence. With `gh`,
   use `--body-file` for multiline text.
4. Read back base/head, title, and body once with `gh pr view`, then return the
   URL. No GitHub browser inspection or wait for CI is required to open it.

Follow `AGENTS.md` for merge approval and required checks; opening a PR does
not authorize merging it.

When an existing draft PR is the subject, interpret "open it" or "ready it"
as making it ready for review unless the user asks to view it. State the intended
transition before acting; use `gh pr ready` rather than opening a browser.
