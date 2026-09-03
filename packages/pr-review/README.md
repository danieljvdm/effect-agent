# @effect-agent/pr-review

A read-only PR reviewer built on Effect AI. Reviews diffs and immutable source, then returns
findings, usage, and diagnostics.

Supply a model, authorized source through `ReviewRepository`, and `Crypto.Crypto` for candidate IDs.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd, costControl });
const program = reviewer
  .review(request)
  .pipe(
    Effect.provideService(ReviewRepository, repository),
    Effect.provideService(Crypto.Crypto, crypto),
    Effect.provide(ReviewDiagnosticsSink.layerNoop),
  );
```

`ReviewDiagnosticsSink` receives final diagnostics, including on cancellation; this example discards them.

The host owns credentials, source read limits, spending admission through `costControl`, and
publication. Incomplete or exhausted results, excluded paths, and pending patches mean unfinished
coverage. Recorded findings survive expected failures; defects and interruption propagate after
cleanup. Only explicit resolutions from a complete review can close prior blockers.

`baseline` is the default. Experimental `strategy: "verified"` adds a fresh verifier within the
same budget and limits. Supported findings publish unchanged; refuted findings stay in diagnostics;
unresolved findings remain visibly unverified and make the review incomplete. Only supported
blockers may request changes. The Action still uses baseline.

See the [API](src/review.ts), [Action setup](../../action/README.md), and
[evaluation guide](../../examples/pr-review-eval/README.md).

Review instructions partly adapted from [PR-Agent](https://github.com/The-PR-Agent/pr-agent).
See `NOTICE` for attribution.
