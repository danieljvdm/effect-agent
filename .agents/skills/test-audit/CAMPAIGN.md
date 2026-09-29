# Test-pruning campaign

Use this mode only when whole-subsystem pruning is explicitly requested. The value bar,
retention bar, candidate evidence, and repository testing policy in [SKILL.md](SKILL.md)
apply throughout. Read-only discovery does not authorize cutover.

1. Pin the baseline revision and record each in-scope test file's result, with test/support
   and production line counts separate. Baseline failures may be real product defects.
2. Inventory every owned test and live/QA scenario exactly once, grouped by production owner
   rather than filename prefix. Include cases at shared boundaries.
3. Assign independent read-only lanes. Read complete declarations and parameter tables,
   production owners, callers, history, and CI routing. Mark each declaration or distinct row:
   `R` retain with its contract and credible failure; `F` retain the contract but repair a
   weak assertion; `C` consolidate into a named keeper; `D` delete with remaining proof or
   an explanation that no independent contract exists. Keep evidence in task artifacts.
4. Review redundant layers, not only individual tests. Name the keeper for each contract,
   assertions that must move, retired files, and test-only seams unlocked. Prefer the real
   transport boundary with a controlled network to a mock implementing collaborator behavior.
5. Apply authorized cutovers lane by lane. Serialize shared schemas, exports, and support
   through one integrator. Remove orphaned support and test-only seams; preserve required
   contracts and update existing runner routing when necessary. Each keeper must pass.
6. Have independent reviewers compare deleted proof with keepers. Resolve each reported gap
   with source evidence. Where a restored assertion's effectiveness is uncertain, use a
   deliberate, isolated owner mutation and confirm the keeper fails, then restore source
   exactly. Do not mutate a checkout while tests run, or build a new mutation framework.
7. Reproduce retained baseline defects at their owners. Keep unrelated defects as follow-ups.
   Repairs need failing control and passing candidate evidence on the same existing workflow.
8. Reconcile upstream changes before handoff: newly added contracts must still have a keeper.
   Review combined changes, run the subsystem proof and `vp run ready`, and repeat live proof
   only when changed contracts require it. Do not automatically discard upstream edits to a
   deleted file; inspect and preserve any distinct contract.

Report baseline/final counts, retired layers and keepers, preservation gaps and verification,
product defects and their evidence, plus the normal skill handoff. Missing baseline or
preservation proof prevents claiming a completed campaign.
