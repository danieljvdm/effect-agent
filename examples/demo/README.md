# Phase 0 runtime demo

A deliberately small test bench for the Phase 0 Travel Planner. It uses TanStack Start, Effect
Atom, shadcn/ui on Base UI, Tailwind CSS, and AI Elements-style conversation primitives.

```sh
bun --filter @effect-agent/example-demo dev
```

The model is the deterministic `ScriptedModel` fixture, so the browser can exercise the real
agent runtime, tool handler, event stream, Schema decoding, and resource Layers without a provider
key or network access.
