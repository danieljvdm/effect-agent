---
"@effect-agent/pr-review": minor
---

Expose bounded review activity and candidate diagnostics, and add an experimental discovery-then-verification strategy while retaining the baseline default. Supply `Crypto.Crypto` for input-bound candidate IDs and `ReviewDiagnosticsSink` for finalization diagnostics, using `ReviewDiagnosticsSink.layerNoop` when only returned diagnostics are needed.
