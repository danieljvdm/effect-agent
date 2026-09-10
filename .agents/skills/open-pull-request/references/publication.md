# Open or update the PR

Check authenticated push access to the target repository before publication.
Use the existing branch and review workflow. For an already verified change:

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
