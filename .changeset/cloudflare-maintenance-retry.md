---
"@effect-agent/platform-cloudflare": patch
---

Persist bounded maintenance retry deadlines across failures and eviction, and prevent alarm repair from bypassing backoff. Allow no-progress retries to reach the configured cap independently of the wake scan interval.
