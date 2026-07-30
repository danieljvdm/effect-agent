# Phase 0 runtime demo

A deliberately small test bench for the Phase 0 Travel Planner. It uses TanStack Start, Effect
Atom, shadcn/ui on Base UI, Tailwind CSS, and AI Elements-style conversation primitives.

```sh
bun --filter @effect-agent/example-demo dev
```

The deterministic `ScriptedModel` profile remains the default, so the browser can exercise the
real agent runtime, tool handler, event stream, Schema decoding, and resource Layers without a
provider key or network access.

The opt-in OpenAI profile runs the same Agent Definition through Effect AI's Responses API adapter
and `gpt-5.6-luna`. Put the credential in this workspace's untracked `.env` file:

```dotenv
OPENAI_API_KEY=...
```

The provider Layer and credential are resolved only by the server implementation of a shared
Effect RPC definition. A TanStack Start server route delegates the request to Effect RPC over
framed HTTP/NDJSON, so the generated browser client receives each Schema-encoded semantic
`RunEvent` as it occurs. Text, provider-returned reasoning, Tool lifecycle, and terminal events are
projected through the same Effect Atom action used by the direct in-browser scripted profile.

The ordinary test and build gates never make a live provider call. This remains an ephemeral
provider preview over the Phase 0 contract, not a deployable transport or a claim that the Phase 1
provider workstream is complete.
