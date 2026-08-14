---
"@effect-agent/session": patch
"@effect-agent/storage-cloudflare": patch
"@effect-agent/platform-cloudflare": patch
---

Exclude incomplete application Tool turns from later Run history while preserving
the owning Run's canonical declaration for durable recovery. Republish the
Cloudflare storage and host packages so their exact internal dependency pins
select the corrected session runtime.
