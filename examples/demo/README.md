# General chat runtime bench

A deliberately restrained test bench for the Effect Agent runtime. It uses TanStack Start, Effect
Atom, shadcn/ui on Base UI, Tailwind CSS, and AI Elements-style conversation primitives.

```sh
bun --filter @effect-agent/example-demo dev
```

Both profiles accept the same Schema-owned `{ message }` input and produce a Schema-decoded
`{ answer }` output. Neither profile injects a hidden travel scenario or mandates a Tool Call.

The default offline profile binds the general chat Definition to `ScriptedModel`. Its submitted
message becomes the parameters of a real `search_fixture_knowledge` application Tool Call, whose
Effect handler returns explicitly marked deterministic catalog data. The profile also exposes a
real `calculate` application Tool. It exercises the runtime, handler Layers, semantic events, and
output decoding without a provider key or network access.

The opt-in live profile binds a separate provider-specific Definition to `gpt-5.6-luna`. The model
may answer directly, use the same exact arithmetic Tool, or invoke Effect AI's OpenAI-hosted web
search according to the Tool descriptions and the user's message. Put the credential in this
workspace's untracked `.env` file:

```dotenv
OPENAI_API_KEY=...
```

The provider Layer and credential are resolved only by the server implementation of a shared
Effect RPC definition. A TanStack Start server route delegates the request to Effect RPC over
framed HTTP/NDJSON, so the generated browser client receives each Schema-encoded semantic
`RunEvent` as it occurs. Text, provider-returned reasoning, Tool lifecycle, and terminal events are
projected through the same Effect Atom action used by the direct in-browser scripted profile.

The Tool panel renders parameters and results from semantic events. It explicitly distinguishes
framework-executed handlers from provider-hosted execution. URLs exposed in hosted search results
are provider output and are not independently verified by this bench.

The pinned Effect beta.102 OpenAI adapter emits an empty declaration for hosted search before its
final action result, while the exported helper currently expects that action during declaration.
The live profile keeps the upstream provider metadata and result schemas but narrows that
declaration schema to the empty object actually emitted. This compatibility projection should be
removed when the upstream adapter and helper agree.

The ordinary test and build gates never make a live provider call. This remains an ephemeral
provider preview and observability bench, not a deployable transport or a claim that the Phase 1
provider workstream is complete.
