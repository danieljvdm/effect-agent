---
"@effect-agent/pr-review": patch
---

Retain host-validated findings from final submissions when another finding or resolution is rejected, and mark the review incomplete without resolutions. BEHAVIOR CHANGE: Return an empty incomplete outcome for invalid-only uncapped baseline submissions with no recorded findings instead of failing with `ReviewVerificationError`.
