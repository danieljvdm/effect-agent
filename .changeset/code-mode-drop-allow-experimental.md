---
"@effect-agent/platform-cloudflare": patch
---

Stop requesting the `allowExperimental` Worker Loader option for Code Mode dynamic workers — it made every pass fail to load unless the calling worker had the `experimental` compatibility flag, which deployed Workers cannot set.
