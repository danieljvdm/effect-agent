---
"effect-agent": patch
---

Distinguish authorization check failures from policy denials and preserve original local causes with structured private diagnostics across durable settlements, Worker admissions, retained message deliveries, and programmatic Worker observation. Add reusable diagnostic codecs and bounded diagnostic context copies while keeping generated tool results and completion reports free of private causal detail.

BEHAVIOR CHANGE: Project Worker and validation errors into a safe failure schema before exposing them through custom tools with `failureMode: "return"`; their new causal fields are private diagnostics.
