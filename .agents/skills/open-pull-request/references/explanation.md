# Explain architecture and APIs

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
Mark omitted context and schematic examples clearly.
Include both a chart and an API example when they answer different review
questions, not just to fill sections.
