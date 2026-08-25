# Effect Agent PR Review action

This directory is the committed JavaScript bundle for the GitHub channel in
`examples/pr-review`. The channel fetches a bounded diff, invokes the
provider-neutral `@effect-agent/pr-review` package once, validates anchors,
and posts one review.

Rebuild it with `vp run action:build`.
