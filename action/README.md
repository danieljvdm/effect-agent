# Effect Agent PR Review action

This directory is the published GitHub Action path. It contains the consumer
contract in `action.yml` and the committed JavaScript bundle in `dist/`.

The private
[`@effect-agent/pr-review-action`](../packages/pr-review-action) workspace
owns the source and tests. The public
[`@effect-agent/pr-review`](../packages/pr-review) package remains provider-
and transport-neutral. See the
[PR-review specification](../docs/spec/pr-review.md) for channel semantics.

Rebuild the committed bundle with `vp run action:build`.
