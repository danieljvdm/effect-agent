# Command design

Model the CLI's public surface as a typed contract. Prefer a small root command,
shared parent flags, focused subcommands, and Effect handlers.

```ts
import { Effect } from "effect";
import { Argument, Command, Flag } from "effect/unstable/cli";

const workspace = Flag.string("workspace").pipe(
  Flag.withAlias("w"),
  Flag.withDescription("Workspace to operate on"),
  Flag.withDefault("personal"),
);

const root = Command.make("acme").pipe(
  Command.withSharedFlags({
    workspace,
    verbose: Flag.boolean("verbose").pipe(
      Flag.withAlias("v"),
      Flag.withDescription("Print diagnostic output"),
    ),
  }),
  Command.withDescription("Operate Acme projects"),
);

const deploy = Command.make(
  "deploy",
  {
    environment: Argument.string("environment").pipe(
      Argument.withDescription("Target environment"),
    ),
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDescription("Show the deployment plan without applying it"),
    ),
    json: Flag.boolean("json").pipe(Flag.withDescription("Print machine-readable JSON")),
  },
  Effect.fn("deployCommand")(function* ({ dryRun, environment, json }) {
    const shared = yield* root;
    const deployments = yield* Deployments;
    const plan = yield* deployments.plan({ environment, workspace: shared.workspace });

    if (dryRun) return yield* renderPlan(plan, { json });

    const result = yield* deployments.apply(plan);
    return yield* renderDeployment(result, { json });
  }),
).pipe(
  Command.withDescription("Deploy a workspace"),
  Command.withExamples([
    {
      command: "acme --workspace team deploy production --dry-run",
      description: "Preview a production deployment",
    },
  ]),
);

export const command = root.pipe(Command.withSubcommands([deploy]));
```

## Input and help

- Use arguments for essential positional identity and flags for optional
  behavior. Prefer named flags when position would be ambiguous.
- Give public commands, arguments, and non-obvious flags descriptions. Add
  examples for quoting, shared flags, or surprising combinations.
- Put cross-command inputs in `Command.withSharedFlags`; read them by yielding
  the parent command instead of duplicating parsing or reading globals.
- Use `Flag.choice` for closed vocabularies and schemas for structured values
  loaded from JSON, files, or environment boundaries.
- Keep aliases additive and unsurprising. Never give two concepts the same
  short flag in one command path.

## Interactive and automated use

- Make every required value available non-interactively. A fallback `Prompt`
  may improve terminal use but must not be the only way to supply input.
- Prompt only after deterministic discovery cannot choose safely. Summarize the
  detected state and the exact mutation before requesting confirmation.
- Pair destructive execution with a genuine plan/apply split. `--dry-run` must
  run the real discovery and planning logic while skipping writes.
- Require explicit `--yes` or an equivalent authorization flag when a
  non-interactive destructive path cannot prompt.

## Output contract

- Treat human and machine output as separate renderers over the same result.
- In `--json` mode, emit one documented schema-encoded value to stdout. Send no
  headings, spinners, progress, or warnings to stdout.
- Use stderr for diagnostics and failures. Keep normal human output concise and
  stable enough for users to understand without reading source.
- Do not expose stack traces for expected failures. Preserve causes for logs and
  tests, then render an actionable message at the executable boundary.
