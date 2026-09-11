---
"@effect-agent/platform-cloudflare": patch
---

Keep alarm updates independent of concurrent SQLite transactions, and retain the earliest requested recovery deadline atomically. Provide the owner's SqlClient when building standalone alarm or maintenance layers.
