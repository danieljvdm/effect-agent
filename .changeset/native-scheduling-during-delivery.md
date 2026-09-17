---
"@effect-agent/platform-cloudflare": patch
---

Keep ready Thread work running while alarm-owned delivery finishes, preserving independent retry deadlines and one shared event budget. Treat `ThreadHostMaintenance`'s `dispatchClosed` signal as the end of new delivery waves; keep local admission listeners alive until their event Scope closes.
