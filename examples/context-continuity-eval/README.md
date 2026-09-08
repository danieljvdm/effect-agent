# Context continuity evaluation

A real OpenAI model maintains one evolving project across 13 user updates, at least 12 native
rollovers, and SQLite runtime recovery before and after a committed window boundary. The model
uses the public context-history and durable-notes tools. The host never supplies a summary or
restores notes into the prompt for it.

The scripted user and reference answers are deterministic. Receipt codes vary by seed, and a
delayed lookup must cite evidence covered by at least ten windows. Semantic checks compare the
agent's status to the user's decisions; operational checks verify canonical rollover, notes, and
retrieval evidence. No second model awards a subjective passing score.

This profile requests rollovers explicitly with a 16k estimated context limit. It does not measure
natural context-pressure behavior, full production-window saturation, Cloudflare CPU, hard process
kills, or the recovery scaling tracked in [#356](https://github.com/danieljvdm/effect-agent/issues/356).
The restart checkpoints close and reacquire the public Node/SQLite runtime in the same process.

Run `vp run context-continuity-eval --help` for configuration and limits. For a local live run:

```sh
EFFECT_AGENT_LIVE=1 vp run context-continuity-eval --model gpt-6-astra --output-dir /tmp/context-eval-1
```

Supply `OPENAI_API_KEY` through the environment or an existing `--env-file`. `--validate` checks the
scenario without inference and cannot pass the live gate. Each live run needs a new output directory.
Preserve failures: rerunning produces additional evidence rather than repairing the first result.

Artifacts contain synthetic conversation data, canonical records, exact outgoing model requests,
model settings, source/scenario identity, token usage, and a conservative cost estimate. Treat a
partial report, provider outage, exhausted budget, missing credential, or failed assertion as a
failed gate. Pricing is a checked-in estimate, not an invoice. There are no inference retries or
model fallbacks, and server-side conversation state and automatic truncation are disabled.
