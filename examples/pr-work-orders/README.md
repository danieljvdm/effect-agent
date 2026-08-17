# PR work-order host

Private class E proof of a head-bound [work-order](../../docs/spec/pr-work-orders.md)
implementer. The host admits one explicit human dispatch, runs a jailed
implementation Agent, and either publishes one validated patch or settles
without publication.

The implementer receives no GitHub credential, provider secret, shell, or
publish tool. Comment `body` and `suggestion` are untrusted evidence.

This adapter is trusted-local and unisolated. It is not a GitHub workflow.
