# @effect-agent/pr-review

A small, provider-neutral review agent.

Two independent investigations inspect every changed behavior: one traces head execution
forward, while the other compares changed input and collection boundaries at base and head and
sees the files in reverse order. They follow changed member classes through finite resources,
effects, completion, and unchanged callees. Each private hypothesis records its source location,
supported external trigger, head failure, and governing contract. Discovery may compare revisions
to find hypotheses but does not prove novelty or choose a final anchor, severity, wording, or repair.

A fresh verifier compares base and head for every hypothesis, rejects unrelated old bugs,
and checks the complete delta for missed defects. A changed source edge is not covered until its
distinct predecessor conditions, inputs, or member classes reach a terminal outcome. The verifier
alone decides what belongs in the published review and writes the findings and cause-level repairs. All three passes use
bounded source reads and path lookup through the host's immutable `ReviewRepository`.
Every pass remains responsible for inspecting all admitted changes.

A finding needs no majority vote. No repository code is executed. Every pass
finishes through a required native completion Tool with strict parameters.

The result contains verified findings, validated line anchors, aggregate model
usage, and optional host-priced cost. Incomplete verification fails instead of
returning a clean report. Incremental findings must be caused or exposed by the
new delta. Verification compares the operation's relevant inputs, state, configuration,
authority, and observable behavior at base and head. An unchanged helper can be affected by
new caller conditions. A defect is pre-existing only when the corresponding affected boundary,
operation, input, and conditions already reach it at base; an equal outcome from another cause is
insufficient.
Each repair must trace one concrete valid boundary input or member that it preserves. Blocking
severity requires material core failure or actual authority expansion, including work beyond the
scope delegated by the specific operation even when ambient credentials permit it.
Model output, aggregate usage, candidate counts, context, and retained response bytes remain bounded. Public
finding paths, titles, and bodies retain their 512, 200, and 2,000-character limits.

The reviewer does not resolve previous findings or declare a partial review safe
to merge. GitHub history, credentials, diff collection, and publication belong to
the host channel.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd });
const program = reviewer.review(request).pipe(Effect.provideService(ReviewRepository, repository));
```

`repository.readFile` and `repository.findFiles` return typed Effects. Hosts must
authorize the source sent to their model, enforce immutable revisions and read
bounds, and treat source content as untrusted data. The reviewer exposes this
dependency in its Effect requirements; it has no ambient filesystem or network
access and does not cache review answers.

`ReviewSource.fromText(request, text)` applies the shared line and character
bounds after the host authorizes and reads a file.
