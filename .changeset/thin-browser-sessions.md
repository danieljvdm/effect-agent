---
"effect-agent": minor
"@effect-agent/platform-cloudflare": minor
---

Replace protected browser passes with application-owned Cloudflare sessions, native Puppeteer actions, and authorized credential filling.

BEHAVIOR CHANGE: Migrate removed `protected-browser` APIs to `browser-session` and `browser-credentials`; ordinary page observations may expose filled values, and application owners must retain session references and close browsers on completion or expiry.
