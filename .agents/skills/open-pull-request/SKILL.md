---
name: open-pull-request
description: Prepare, open, or revise pull requests with conventional titles and descriptions proportional to the change. Use when drafting a PR title or body, preparing a branch for review, or opening a PR.
---

# Open a pull request

Help the reviewer understand what changes and why. Default to a short
description; add detail where it helps them assess the change.

## Prepare the branch

Read repository contribution instructions and any required PR template. Inspect
the intended base, full commit range, diff, and working tree so the description
covers the branch as it exists. Resolve accidental changes before opening.

Use Conventional Commits for commits you create and the PR title:
`type(scope): imperative summary`. Follow repository conventions and omit the
scope when it adds nothing. Keep commits focused. Rewrite only your own unshared
commits; get approval before rewriting user-authored or published history.

Complete required validation on the final branch state before opening the PR,
including any changes made by commit hooks. Ground claims in the diff, CI, or
verified artifacts.

## Write the description

Choose the shape from the change. Follow a required repository template;
otherwise there are no mandatory headings or sections.

- Small fixes and maintenance usually need one short paragraph. State the
  observable problem and how the change fixes it. A bug's cause can fit in the
  same sentence; expand only when the reasoning is subtle.
- For architecture or API changes, lead with the practical difference and show
  a compact before/after code example. Use actual base and head code, trimming
  to the relevant call site, contract, or implementation. Explain the ownership
  or flow change that the snippets cannot show. Include migration steps when
  callers must act.
- For subtle correctness changes, add the causal explanation, tradeoff, or
  limitation a reviewer needs to judge the fix. Use headings only when they
  make that explanation easier to scan.

A small fix can be this short:

> Clearing the search box left the previous filter active because the update
> skipped empty strings. Always apply the new value so clearing shows all rows.

Code examples should show the change with enough context to understand it.
Mark omissions or schematic examples clearly. One representative example is
usually enough; link to a guide for full setup or additional variants. For
changes that code cannot explain well, use the smallest useful before/after
output or diagram instead.

Link to implementation points when they save the reviewer a search. Keep the
body focused on behavior, rationale, and necessary migration. Leave the file
inventory to the diff and routine validation results to CI.

## Include evidence when it helps

Add a screenshot for a visible UI change, or concise output, a request/response,
or a focused regression result when it makes the behavior easier to assess.
Use only artifacts actually produced and verified, and check them for secrets
or personal data. If needed proof is unavailable, state the limitation briefly.

Evidence can sit beside the explanation. Omit it when it adds nothing beyond
the diff and CI. Mention validation commands only for unusual checks, checks CI
cannot run, or results that explain the behavior.

## Open and verify

Open against the intended base with the prepared conventional title and body.
Read back the rendered PR and check its branches, description, links, attached
artifacts, and check status. Return the URL once it is reviewable, reporting any
pending or failed checks accurately.
