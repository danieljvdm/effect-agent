---
"@effect-agent/platform-cloudflare": patch
---

Run Dynamic Worker Code Mode host callbacks on retained independent Effect fibers so guest RPC
callbacks can complete without deadlocking the in-flight worker RPC. Bound callback execution by
the pass deadline and host-call limits, and close, interrupt, and settle callback work on teardown.
