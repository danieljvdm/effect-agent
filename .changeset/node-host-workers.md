---
"@effect-agent/platform-node": minor
---

Start a scoped worker pool with `NodeDurableHost.layer(registrations, options)` and supervise it with `NodeDurableHost.run`. Close admission when the pool exits and stop workers before releasing runtime resources.
