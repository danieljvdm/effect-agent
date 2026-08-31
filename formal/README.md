# Formal models (TLA+ / PlusCal)

Bounded TLA+ models of the durable Submission protocol and the durable
attached-Subagent protocol, model-checked with TLC.

These checks are **not** part of `bun run ready` or the PR gate: they need a
JVM, which the Bun-only CI deliberately does not carry. Run them manually via
`bun run formal:check`, or on demand through the scheduled/dispatchable
GitHub Actions workflow (`.github/workflows/formal.yml`).

## Toolchain

- Java 17+ (any distribution; verified with Temurin 21).
- `tla2tools.jar` is the TLA+ tool bundle. Use version **1.8.0 or newer**. This repository was
  verified with TLC2 version 2.19 of 08 August 2024. Download it from:
  <https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar>

`bun run formal:check` locates `java` (honoring `JAVA_HOME`), downloads
`tla2tools.jar` to a cache when absent (override with `--tools <path>` or
`TLA2TOOLS_JAR`), runs TLC on every committed instance below, and asserts the
expected verdict of each. The negative controls must fail.

## Files

| File                             | What it is                                                                                                                                                      |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DurableSubmission.tla`          | PlusCal: one Thread lane, FIFO Submissions, workers, recovery, resolution dependency, abort, lease expiry, crash at every durable boundary                      |
| `DurableSubmission.cfg`          | safety instance with 2 Submissions, 2 workers, a fault budget of 2, and 8 invariants                                                                            |
| `DurableSubmissionLiveness.cfg`  | liveness instance (1 worker): `EventuallySettled` under the documented fairness + fault-budget assumptions                                                      |
| `DurableSubmissionNoFencing.cfg` | **negative control** with fencing disabled. TLC must report a `FencingSafety` violation                                                                         |
| `SubagentEstablishment.tla`      | PlusCal: S2 establishment ladder, `waitingForChild`, at-least-once child-settled wake, canonical-settlement join, reservation lifecycle, request-abort-and-join |
| `SubagentEstablishment.cfg`      | main instance under the current child-recovery discipline, with 6 invariants and 2 liveness properties                                                          |
| `SubagentEstablishmentRace.cfg`  | **negative control / plan §7(a)**. TLC must find the child-runs-before-lineage interleaving as a `ChildTurnRequiresLineage` violation                           |
| `SubagentEstablishmentFix.cfg`   | the `AwaitParentEstablishment` fix discipline. The race is eliminated, and all invariants and liveness properties hold                                          |

## Exact invocations

With `java` on `PATH` and `tla2tools.jar` in this directory:

```sh
cd formal

# parse only (SANY)
java -cp tla2tools.jar tla2sany.SANY DurableSubmission.tla SubagentEstablishment.tla

# expected: PASS (no error found)
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config DurableSubmission.cfg DurableSubmission.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config DurableSubmissionLiveness.cfg DurableSubmission.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config SubagentEstablishment.cfg SubagentEstablishment.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config SubagentEstablishmentFix.cfg SubagentEstablishment.tla

# expected: FAIL (the negative controls prove the invariants are load-bearing)
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config DurableSubmissionNoFencing.cfg DurableSubmission.tla
java -XX:+UseParallelGC -cp tla2tools.jar tlc2.TLC -deadlock -workers auto \
  -config SubagentEstablishmentRace.cfg SubagentEstablishment.tla
```

Notes:

- `-deadlock` disables TLC's deadlock reporting: a fully settled instance has
  no enabled action by design, and progress is asserted by the temporal
  properties instead.
- The full `DurableSubmission.cfg` safety sweep explores ~144M generated /
  ~38M distinct states (about a minute on a modern laptop with parallel
  workers). The other instances finish in seconds.
- If your sandbox restricts sockets: TLC's liveness checker opens a loopback
  RMI socket. If it restricts the system temp directory, pass
  `-Djava.io.tmpdir=<writable dir>` so TLC can unpack the standard modules.

## Editing the models

The PlusCal algorithm is the source of truth; the TLA+ after
`\* BEGIN TRANSLATION` is generated. After editing the algorithm block:

```sh
java -cp tla2tools.jar pcal.trans -nocfg DurableSubmission.tla
java -cp tla2tools.jar pcal.trans -nocfg SubagentEstablishment.tla
```

then re-run TLC on all six instances (`bun run formal:check`). Keep the
invariant/property definitions below the `\* END TRANSLATION` marker; the
translator rewrites only the block between the markers.
