# @effect-agent/pr-review

A read-only PR reviewer built on Effect AI. Reviews diffs and immutable source, then returns
findings, usage, and diagnostics.

Supply a model, authorized source through `ReviewRepository`, and `Crypto.Crypto` for candidate IDs.

`ReviewRepository` hosts provide `readFile`, `findFiles`, and `findInFile` at the request's exact
base and head. `findFiles` searches path names; `findInFile` locates a case-sensitive literal in
one authorized file. Use `ReviewLineMatches.fromText` for the shared search bounds: literals are
1 to 200 characters without carriage returns or newlines, source is at most 2,000,000 UTF-8 bytes
and 1,000,000 lines, and results contain at most 20 distinct line numbers. Resume after the last
line when `truncated` is true. An empty result means no match from `startLine`; unavailable or
oversized source fails with `ReviewContextError`. Search locations contain no source text and
do not satisfy a source-evidence citation; follow relevant matches with `readFile`.

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

With `costControl`, a model-declared incomplete batch does not prevent later patch batches from
running within the same limits. The review stays incomplete and withholds all resolutions.
Discovery stops on failures, invalid submissions, exhausted budgets, or 24 accepted findings
before another batch. Unstarted patches remain pending; incomplete batches are not retried.

Reaching the five-minute review limit records the host stop reason `deadline` in stage diagnostics.
The Action explains this time limit separately from model-reported limitations; the review remains
incomplete.

An incomplete discovery submission may include an `incompleteReason` of up to 600 characters naming
unfinished work or missing evidence. Its stage diagnostic retains that model-reported explanation, including when
later batches complete. It is optional, unverified display text and must not enter telemetry.
Host stop reasons and completeness remain separate; an explanation never authorizes a retry or
closes a prior blocker. The Action displays up to four explanations and keeps the rest in diagnostics.

Final submissions retain findings that pass host validation, up to the existing 24-finding limit.
Rejected findings or an invalid resolution array make the review incomplete and withhold all
resolutions. With no recorded findings, a structurally valid submission containing only rejected
findings returns an empty incomplete outcome, including with an uncapped baseline; malformed native
output is still rejected without retrying.

`baseline` is the default. Experimental `strategy: "verified"` adds a fresh verifier within the
same budget and limits. Supported findings publish unchanged; refuted findings stay in diagnostics;
unresolved findings remain visibly unverified and make the review incomplete. Only supported
blockers may request changes. The Action still uses baseline.

Verifier evidence submissions accept `base` and `head` as revision selectors. An exact match to
either request revision takes precedence, including when that revision is literally named `base`
or `head`. Retained evidence uses the request's exact revisions. Selectors do not authorize source
access or satisfy evidence coverage: source must have been delivered in that verifier run, and diff
references must still belong to the selected side of the supplied patch.

See the [API](src/Review.ts), [Action setup](../../action/README.md), and
[evaluation guide](../../examples/pr-review-eval/README.md).

Review instructions partly adapted from [PR-Agent](https://github.com/The-PR-Agent/pr-agent).
See `NOTICE` for attribution.
