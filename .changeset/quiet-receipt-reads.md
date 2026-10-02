---
"effect-agent": patch
"@effect-agent/platform-cloudflare": patch
---

Read finalized settlements without hydrating recovery state. Add `awaitSettlementRecord` to the durable runtime and Cloudflare client to retrieve a receipt's canonical outcome and encoded result without transferring its Thread history.
