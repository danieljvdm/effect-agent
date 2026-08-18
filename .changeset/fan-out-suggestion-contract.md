---
"@effect-agent/pr-review": patch
---

Publish a fan-out finding's GitHub suggestion block only when independent verification settles its text as committable replacement source. A confirmed finding whose suggestion is not settled as committable is published without the suggestion block, and a verification pass that leaves a carried suggestion unsettled fails the unit's settlement.
