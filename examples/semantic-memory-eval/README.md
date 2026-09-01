# Semantic memory evaluation

This leaf workspace runs a frozen, synthetic retrieval benchmark through the public semantic-memory APIs. It uses a local Transformers.js feature-extraction pipeline and never calls an LLM answerer or judge.

```sh
vp run semantic-memory-eval --output /tmp/kom18-evaluation.json --git-revision <sha>
vp run semantic-memory-eval --offline --output /tmp/kom18-evaluation-offline.json --git-revision <sha>
```

The first command may download the exact pinned fp32 model revision into `/tmp/effect-agent-semantic-model`. The offline command proves that the cached files are sufficient. `--cache`, `--output`, `--offline`, `--environment`, and `--git-revision` are explicit CLI inputs.

The corpus contains 20 documents and 16 queries. Its labels, the lexical overlap rule, semantic top-three limit, and 0.35 score threshold were fixed before embeddings were run. The evaluator first commits each source through the canonical SQLite Thread store, processes committed activity into the SQLite `MemoryWriter`, and indexes the initial version. It then applies real corrections and withdrawals, probes the stale index through authoritative filtering, refreshes the index, and runs three methods through the same `recallMemory` bounds.

- Explicit locator loading accepts one exact `memory://evaluation/<id>` locator. Natural-language input returns `NoMatch`.
- Lexical retrieval counts shared lowercased letter-or-number terms and breaks ties by source ID.
- Semantic retrieval uses `querySemanticMemory` with top three and a 0.35 minimum score.

All methods use at most three passages and a 4,096-byte/4,096-token rendered context envelope. Since the native provider does not report tokens, the report leaves native input tokens unknown and estimates context at one token per UTF-8 byte. Local inference has no API cash charge; CPU and economic cost are unmeasured.

Warm latency covers all 16 frozen queries. Cached-model cold samples use three newly loaded model instances over the already published index and include model load, query embedding, index search, authoritative reads, and bounded recall. They exclude the first model download and CLI import time. Commit-to-recall samples start after the canonical append acknowledgement. Extraction is deterministic and makes zero model calls; background source embedding is reported separately.

Transformers.js native inference has no abort signal. Once inference starts, the Effect adapter drains it before disposing the scoped pipeline, so a cooperative deadline can overshoot. Injected delay, failure, and timeout cohorts verify control flow and do not represent production latency.

The benchmark measures retrieval against frozen labels. It does not decide whether a proposal should override another proposal, treat assistant repetition as independent evidence, or infer an answer from the selected context.
