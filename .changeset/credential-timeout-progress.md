---
"@effect-agent/platform-cloudflare": patch
---

Preserve acknowledged credential writes, dispatch evidence, and browser cleanup status in credential timeout failures.

BEHAVIOR CHANGE: Call `session.fillCredential(request)` instead of the standalone `fillCredential(page, request)` helper.
