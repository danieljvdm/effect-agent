# PR work-order ingress

Operational implementation behind the separately named
[`work-order-action/`](../../work-order-action/) workflow. An exact inline reply becomes one
[work order](../../docs/spec/pr-work-order-ingress.md), is persistently claimed in an authenticated
bot-authored thread journal, runs the bounded model implementer, executes required checks in a
networkless container, and publishes only through an independent compare-and-swap publisher.

Reply `@effect-agent fix this` to one inline review comment to dispatch.
The five-job Actions workflow updates one bounded host-authored thread reply and never resolves the
thread. It does not belong on `@effect-agent/pr-review`.
