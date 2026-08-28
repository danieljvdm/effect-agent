---
"@effect-agent/sandbox": patch
"@effect-agent/platform-cloudflare": patch
---

Add an explicit `Unrestricted` interactive browser policy for arbitrary-site browsing without URL/host or private-network containment guarantees, retaining session limits and host controls. Admit credential-free HTTP and HTTPS interactive navigation and URL observations without changing PageCapture contracts.
