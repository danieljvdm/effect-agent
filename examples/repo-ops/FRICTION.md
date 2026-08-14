# Authoring friction — evidence auditor (WP7 input)

Real observations from writing this agent against the public framework
surface. These feed the P7 API-simplification work package.

1. **Sandbox command tools need a lot of ceremony for "run one read-only
   command".** Every tool handler rebuilds a full `SandboxRequest` (runtime,
   environment, mounts, network, limits, secret handles, artifact rules) and
   re-implements stdout collection over the event stream, including the
   "exit code 1 is data, not failure" dance for `grep`. A small framework
   helper — `Sandbox.exec(command, args, { cwd, limits })` returning collected
   bounded stdout with a typed exit-code branch — would remove ~60 lines of
   boilerplate per agent without weakening the schema-first request surface.

2. **Nothing composes "validate a model-supplied path" fail-closed.** The
   normalization rules (no absolute paths, no `..`, no `\`, bounded length)
   had to be hand-written, and every handler must remember to call it before
   touching the workspace. security-operations §6 names the requirement;
   a shared, well-tested `RelativePathPolicy` capability would make the safe
   path the default one.

3. **The approval flow's split personality takes discovery.** `needsApproval`
   is declared on the Tool, the durable suspension happens automatically, but
   the RESUME requires knowing the `submissionId` + `toolCallId` pair and
   calling `resolveApproval` on the runtime — none of which is discoverable
   from the Tool definition. Finding that the canonical
   `ToolApprovalRequested` record carries the `toolCallId` an operator needs
   required reading the durable-approval test suite, not documentation. (The
   P7 admin `explain` operation should close most of this gap.)

4. **Durable Steps were the smoothest part of the whole surface.** Declaring
   `DurableStep` as a dependency and wrapping each document in
   `step.do(\`audit:\${document}\`, Schema, effect)` worked first try, and the
   step records show up in canonical history with obvious names. No change
   requested.

5. **Choosing an execution class forces honest thinking but gives no
   feedback.** Annotating `readonly` / `idempotent` / leaving `uncertain` is
   purely declarative; nothing checks, for example, that a `readonly` tool's
   handler acquired no write capability. Even a lint-level heuristic (flag
   `readonly` tools whose dependencies include a known mutating service)
   would catch honest mistakes.
