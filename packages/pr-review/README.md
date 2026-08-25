# @effect-agent/pr-review

A small, provider-neutral review agent.

The package accepts an already-collected diff, makes one model call with no
tools, validates the returned paths and line anchors, and returns a structured
report with token usage. It knows nothing about GitHub, webhooks, comments,
providers, retries, or review history; those belong to the host channel.

```ts
const reviewer = makeReviewer({ model, guidance });
const outcome = yield * reviewer.review(request);
```
