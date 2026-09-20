---
"@effect-agent/platform-cloudflare": patch
---

Allow ordinary protected browser filling for checkout and contact email fields. BEHAVIOR CHANGE: Treat email-only login forms marked solely `autocomplete="email"` as ordinary text without saved-login offers; retain offers for explicit `autocomplete="username"` and preserve password-form and card protections.
