# Explain architecture and APIs

Pick the smallest view that makes the change clear, and place it beside the
short explanation it supports. Prefer a diagram or example over a long prose
description; simple changes can stay prose-only.

- For meaningful architecture changes to component ownership, boundaries, or
  data flow, use a focused Mermaid chart only when it explains the change more
  clearly than short prose. A sequence diagram can help when interactions between
  components change. Name actual components and changed interactions without
  mapping the whole system. Routine fixes and local branching need no diagram.
- Include a small code excerpt or caller example only when it clarifies a changed
  contract or subtle behavior. For an API change, this can be an HTTP request and
  response or a typed call and its result. Show only relevant inputs, outputs,
  and errors. A before/after diff can explain a caller migration; an obvious
  edit needs no sample.

Use fenced Mermaid and code blocks directly in the PR. A call tree or pseudocode
can replace a chart when it explains the change more clearly. Match diagrams
and examples to the final implementation, use safe fixture data, and distinguish
illustrative or expected output from output actually observed during validation.
Include both a chart and an API example when they answer different review
questions, not just to fill sections.

## Performance claims

Support performance claims with a before/after table comparing the target-branch
baseline and PR candidate. Identify the revisions, workload, measurement
conditions, units, and relevant variability so reviewers can interpret the
comparison. Report measured results; label estimates and avoid claiming gains
without a comparable baseline.
