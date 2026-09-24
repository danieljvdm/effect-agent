---
"effect-agent": patch
---

Preserve OpenAI implicit prompt-cache boundaries when `runStatus: "appended"` is enabled.
Keep discarded status and reference context out of later requests when native response-ID tracking is enabled.
