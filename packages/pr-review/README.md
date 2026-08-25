# @effect-agent/pr-review

A small, provider-neutral review agent.

The package accepts an already-collected diff, makes one model call with no
tools, validates the returned paths and line anchors, and returns a structured
report with cached, uncached, cache-write, and output token usage plus, when
supplied by the host, an estimated cost. It knows nothing about GitHub,
webhooks, comments, providers, retries, or review history; those belong to the
host channel.
Each finding carries a severity and problem category so a host can render a
scannable review without another model call.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd });
const outcome = yield * reviewer.review(request);
```
