---
"@effect-agent/storage-cloudflare": patch
"effect-agent": patch
---

Keep SQL memory admission work bounded as retained history grows, charge replacements by their byte delta, and preserve existing exact-result receipts. Add optional withdrawal capacity reserves within hard storage limits; deploy exclusively upgraded writers before relying on reserves.
