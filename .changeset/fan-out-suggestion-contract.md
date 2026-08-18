---
"@effect-agent/pr-review": patch
---

Publish a fan-out finding's GitHub suggestion block only when the independent verifier settles it as "committable"; strip it from confirmed findings otherwise, and fail unit settlement when a carried suggestion is left unsettled. Also spell the finding shape and committable-suggestion rule out to discovery workers and annotate `ReviewFinding.suggestion` so the model-visible output schema carries the contract.
