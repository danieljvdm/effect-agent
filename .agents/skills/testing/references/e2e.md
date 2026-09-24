# E2E verification and artifacts

E2E describes the verification boundary, not a requirement to create test code.
Use existing acceptance commands or interact with the running product directly.
When no automated scenario exists, replayable steps plus observed output can
supply the proof. Apply [test selection](selection.md) before adding automation.

Exercise the requested workflow through its real entrypoint to an independently
checked outcome. Use the browser or native app for interaction behavior, and a
real CLI/API workflow when that is the product boundary. Keep the actual storage,
authorization, orchestration, and provider boundaries required by the proof.
Mocked business logic or a hand-built copy of the system does not establish E2E.

Prefer one representative scenario proving the complex feature. Exercise a
failure or recovery path only for a requirement or a concrete risk in scope;
do not expand the journey to make the evidence look thorough.
Choose the smallest existing consumer, CLI, adapter or deployed acceptance
entrypoint from `docs/TOOLCHAIN.md` and its owning README. Documentation changes
and test removal need static validation and the required repository gate;
they do not justify an unrelated product journey.

## Finish with a verifiable, repeatable artifact

Every E2E run, including a failed or interrupted run, must leave inspectable
evidence and concise replay instructions. A saved transcript, screenshot, or
resulting file with the necessary steps can be enough. Reuse existing receipts
and captures; do not write artifact generators, report schemas, or new fixture
machinery just to satisfy this requirement. Include only what the run needs:

- Exact command or replay steps, revision/build, environment, prerequisites,
  and fixture/seed identifiers plus setup and reset/cleanup instructions. Use
  a tested commit/build, or retain a sanitized patch and source fingerprint
  sufficient to reconstruct any uncommitted changes in the tested tree.
- The expected and observed result, and whether the required outcome passed,
  failed, or remains blocked.
- Inspectable evidence appropriate to the claim: a trace, response transcript,
  resulting file or persisted record, screenshot, or video. A screenshot alone
  cannot prove persistence or recovery, and a green summary alone is not proof.
- Enough retained inputs and instructions for another person or agent to rerun
  the scenario and check the same outcomes. Record fixture retirement or a
  recreation recipe; do not depend on an unrecoverable live session.

Keep credentials, cookies, private payloads, and capability URLs out of artifacts.
Use sanitized fixtures and preserve the evidence in the task's artifact directory
or approved artifact store. Link the receipt and evidence in the final handoff;
ephemeral console output or a claim that the flow passed is insufficient.

## Reuse evidence and stop deliberately

Reuse passing evidence until a relevant source, dependency, configuration, or
environment change invalidates it. After a fix, rerun the affected proof; do not
restart the whole live scenario unless earlier evidence is invalid. The final
`vp run ready` requirement still applies.

Distinguish an in-scope defect from an unrelated failure or unavailable
environment. Before retrying, state what changed or what new evidence the run
will collect. An unchanged failure does not justify another broad suite. Keep
blocked required acceptance explicit; never silently drop it, report it as
passed, or turn unrelated findings into new delivery requirements.
