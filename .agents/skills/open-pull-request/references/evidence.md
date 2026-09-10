# Capture visible behavior

For UI or visible features, capture the final running implementation during
verification and reuse it for the PR. A screenshot is the default; use a short
video when the sequence matters, such as an agent exchange or animation. Both
are rarely needed. Nonvisual changes need no screenshots or recordings.

For changes to existing visible behavior, prefer a before/after table with
published captures when the comparison helps reviewers see the change. Use the
same viewport, fixture data, and interaction state for both captures. A new
feature can use a final-state capture without a contrived before image.

Use existing capture tools; load `playwright-cli` for browser capture. Keep
recordings focused, usually under 30 seconds, without changing product timing.
Use safe fixture data and review the image or whole clip once for correctness
and private content. Treat published assets as public; unreviewed media stays
local. If inspection is unavailable, use a safe alternative or report the
blocker. Do not build viewers, extract frame galleries, or reconstruct GitHub.

Use the repository's existing media publisher when available. Otherwise inspect
`gh pr create --help` or `gh pr edit --help` for supported attachments, and use
that capability when authenticated access allows it. Do not assume a particular
publisher script exists in every consumer repository.

Use the returned link or Markdown in the PR. Keep originals until publication succeeds;
if it fails, report the exact local path. Never extract browser cookies, expose
credentials in arguments, create asset branches, or invent an upload service.
