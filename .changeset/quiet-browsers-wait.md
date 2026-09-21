---
"@effect-agent/platform-cloudflare": patch
---

Allow retained browser attachments up to ten seconds to acknowledge disconnection, preserving completed commands when the close handshake takes longer than one second. Continue fencing dispatch immediately and fail cleanup when closure remains unconfirmed.
