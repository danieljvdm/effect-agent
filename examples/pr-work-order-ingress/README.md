# PR work-order ingress

Private proof that a GitHub mention reply or reaction becomes one
[work-order](../../docs/spec/pr-work-order-ingress.md), is claimed in a
file-backed attempt store, and is published only after isolated checks and an
independent publisher compare-and-swap.

Reply `@effect-agent fix this` to one inline review comment to dispatch.
The Actions workflow admits that event and replies on the thread. It does
not apply a patch. It does not belong on `@effect-agent/pr-review`.
