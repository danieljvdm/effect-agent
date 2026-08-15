---
name: open-pull-request
description: Open pull requests with conventional commits, reviewer-complete descriptions, links to vital code, and concrete evidence. Use whenever preparing or opening a pull request, including checking commit history, explaining a bug or architectural change, drafting the title or body, and attaching screenshots or other evidence.
---

# Open a Pull Request

Produce a PR that a reviewer can understand and trust without access to the
task conversation.

## Prepare the branch

1. Read the repository's contribution instructions and PR template. Identify
   the intended base branch, then inspect the full commit range, diff, and
   working tree. Finish when the PR scope contains no accidental changes and
   the description will cover the branch as it exists, not merely the latest
   task.
2. Use Conventional Commits for every commit you create and for the PR title:
   `type(scope): imperative summary`. Follow repository-specific types and
   scopes, and omit the scope when it adds no useful context. Otherwise use a
   precise standard type such as `feat`, `fix`, `refactor`, `docs`, `test`,
   `build`, `ci`, or `chore`. Keep each commit to one logical concern. Rewrite
   only commits you created and know are unshared; get approval before
   rewriting user-authored or published history.
3. Run the repository's required validation on the final branch state, then
   collect the strongest available evidence of the changed behavior. Identify
   the few files, symbols, or modules a reviewer must understand and prepare
   links that resolve in the rendered PR. Finish when every claim can be traced
   to the diff, CI, or a verified artifact.

## Write for the reviewer

Write clear, compact English for someone with little context. Give the reviewer
enough explanation to agree with both the problem and the solution; do not
sacrifice causal or architectural context for brevity. Lead with the observable
outcome, then explain why the change was needed and how the important pieces
fit together. Prefer concrete nouns and expand uncommon acronyms. Describe
behavior and impact rather than narrating the task conversation.

Link the vital implementation points from the summary or architecture section.
Use descriptive link text that names each piece by its role, such as the request
router or cache invalidation boundary, and verify every link after opening the
PR. Link the core pieces a reviewer should inspect, not every touched file.

For every bug fix, include a **What went wrong** section in plain English. State
the incorrect behavior, its actual root cause and causal chain, and why the
change fixes it. Make uncertainty or incomplete coverage explicit. A result
such as "fixed stale state" is not a diagnosis; explain how the stale state was
created or allowed to survive.

When the change alters architecture, identify the affected components and
boundaries, what each one owns after the change, and any important change to
control flow, data flow, public contracts, or persistence. Link to the core
implementation of each affected piece. Use a dedicated **Architecture** section
when this would make the change easier to review; otherwise include the context
in the summary.

Use the repository's required template when present. Otherwise use this small
shape and omit empty sections:

```md
## Summary

- <What changes for a user, operator, or developer>
- <Why it matters and the shape of the solution, with links to vital code>

## What went wrong

<For a bug fix: explain the symptom, root cause, causal chain, and why this fix
addresses it.>

## Architecture

- <When applicable: explain the changed components, ownership, and flow, with
  links to their core implementations.>

## Evidence

- <Screenshot, before/after output, request/response, trace, or other verified
  artifact>
```

Keep the body proportional to the change: a small change may need two useful
bullets, while a subtle bug or architectural change may need several paragraphs.
Omit conditional sections that do not apply. Make the title specific enough to
stand alone in release notes and conventional enough to become the squash
commit without editing.

## Show useful evidence

Evidence is something the reviewer can inspect, not an assertion that the
change works.

- For a runnable UI or visual feature, capture and attach a screenshot or short
  recording of the actual final state. Use a representative viewport, add a
  short caption, and check the artifact for secrets or personal data.
- For CLI, API, or automation behavior, include concise terminal output, a
  request/response example, generated artifact, or execution log when it proves
  the behavior more clearly than the CI result alone.
- For a bug fix or behavior change, prefer before/after evidence when it is
  practical and materially clarifies the result.
- For internal-only changes, include focused regression output, a trace, a
  generated artifact, or another result that demonstrates the changed behavior
  when available.

Routine validation commands that CI always runs, such as `vp check` or standard
format, lint, typecheck, and test commands, add no useful context to the PR body.
Let CI report them. Mention a command or CI result only when it is unusual,
cannot run in CI, or its output itself helps the reviewer understand the change.

Include only evidence that was actually produced and verified. When expected
visual proof cannot be produced, state the concrete reason briefly instead of
silently substituting a claim. Choose the smallest set of evidence that makes
the changed behavior easy to inspect. Omit the section when no evidence adds
information beyond routine CI.

## Open and verify

Open the PR against the intended base with the conventional title and prepared
body. Then read back the rendered PR and verify the base/head branches, title,
description, links, screenshots, and check results. Finish only when the PR is
reviewable as rendered and return its URL.
