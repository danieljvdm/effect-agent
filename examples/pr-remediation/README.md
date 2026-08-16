# PR remediation loop

This private leaf example is the first executable proof of a conservative pull-request
review → remediation → re-review loop. It composes the public read-only
`@effect-agent/pr-review` reviewer with a separate, bounded implementation Agent and a
deterministic host coordinator.

The host accepts only an explicit `pr-remediate` trigger for a trusted same-repository pull
request. The first review is converted into an HMAC-authenticated handoff bound to the repository,
PR number, exact reviewed head, review/profile fingerprints, and the active blocking/important
findings. One implementation attempt is admitted for that head. A fresh explicit trigger is
required before any later head can be attempted.

The implementation Agent receives no GitHub credential, provider secret, shell, or publication
tool. Its complete authority is five Schema-defined tools: read/search allowlisted files, replace
an exact string in one allowlisted file, inspect its patch, and request a named host-configured
check. It runs in a scoped detached Git worktree. Suggestions in findings are untrusted evidence,
not patches to apply automatically.

After the Agent settles, host code independently collects the patch, rejects paths outside the
finding/support-file allowlist, verifies the patch digest and the report's finding accounting,
reruns every required named check, verifies checks did not mutate the patch, and atomically moves
the configured local branch only when the reviewed head is still current. The implementation
Agent never grades or publishes its own work. A new reviewer invocation then evaluates the
published head.

## Security posture

This is deliberately local, trusted, deployment-class E evidence. Its local child-process adapter
does not isolate execution of repository checks, and the example provides no GitHub workflow,
token-bearing process, network publisher, or production sandbox claim. Do not execute untrusted PR
code with credentials or provider secrets in this host. A production host must provide genuine
isolation for checks, authenticate the label actor and same-repository provenance, keep credentials
out of the worktree/check environment, and implement the same head-bound atomic publication seam.

The deterministic real-Git tests cover success, path escape rejection, host-check failure, a moved
head, interruption/worktree cleanup, one-attempt admission, and a fresh re-review.
