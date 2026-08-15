---
"effect-agent": patch
"@effect-agent/engine": patch
"@effect-agent/platform-cloudflare": patch
---

Export privacy-safe canonical Tool spans and bounded terminal logs from the engine, including
value-level failures and delayed terminal event/trace commit, while isolating complete span-
lifecycle defects through Effect's error reporter.
Add an explicit host-provided Cloudflare observability Layer at the Worker
factory boundary, with background flushes after native RPC, wake, and alarm spans through
`ctx.waitUntil`, plus a typed exporter error that retains the host cause without delaying delivery.
Coalesce background export requests per
Conversation Object into capped two-attempt cycles with at most one queued cycle, preserve typed
export Causes until the final always-fulfilled `waitUntil` bridge, and keep synchronous background
registration failure synchronously observable without invoking an unowned exporter or replacing
native delivery.
Automatic Cloudflare diagnostics expose only bounded framework classifications; retained exporter,
defect, interrupt, and platform Causes never enter framework logs.
