---
"effect-agent": minor
"@effect-agent/platform-cloudflare": minor
---

Resume retained requests using explicit replay contracts and back off failed and blocked Cloudflare maintenance without abandoning child obligations or changing receipts. BEHAVIOR CHANGE: bound durable execution duration per active Attempt, retain actual duration exhaustion, and deploy matching runtime and storage packages before writing the new record.
