---
"effect-agent": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Admit background workers through one destination RPC and read child completion receipts concurrently only when capacity could block admission.

BEHAVIOR CHANGE: Upgrade the framework and Cloudflare packages together; custom routed hosts must install `routedWorkerAdmissionLayer` and provide `WakeScheduler` and `DurableRuntimeFailpoint` to owner-side port handlers under their maintenance gate. Existing records and retry receipts require no reset.
