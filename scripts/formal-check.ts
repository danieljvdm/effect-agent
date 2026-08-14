import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Option, Path, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

const TLA2TOOLS_RELEASE_URL =
  "https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar";

const JAVA_INSTALL_GUIDANCE = [
  "TLC needs a Java 17+ runtime and none was found.",
  "Install one and re-run, for example:",
  "  - macOS:  brew install --cask temurin",
  "  - Linux:  apt-get install temurin-21-jre  (or your distribution's OpenJDK 17+)",
  "  - direct: https://adoptium.net/temurin/releases/",
  "Then ensure `java` is on PATH or JAVA_HOME points at the installation,",
  "or pass --java <path-to-java-binary> explicitly.",
].join("\n");

/** One committed bounded model-checking instance and its REQUIRED verdict. */
interface FormalInstance {
  readonly spec: string;
  readonly cfg: string;
  readonly expectation: "pass" | "fail";
  /** For expected failures: the invariant TLC must report as violated. */
  readonly expectedViolation?: string;
  readonly description: string;
}

const INSTANCES: ReadonlyArray<FormalInstance> = [
  {
    spec: "DurableSubmission.tla",
    cfg: "DurableSubmission.cfg",
    expectation: "pass",
    description: "durable Submission protocol safety (2 workers)",
  },
  {
    spec: "DurableSubmission.tla",
    cfg: "DurableSubmissionLiveness.cfg",
    expectation: "pass",
    description: "EventuallySettled under the documented fairness assumptions",
  },
  {
    spec: "DurableSubmission.tla",
    cfg: "DurableSubmissionNoFencing.cfg",
    expectation: "fail",
    expectedViolation: "FencingSafety",
    description: "negative control: disabling fencing must break FencingSafety",
  },
  {
    spec: "SubagentEstablishment.tla",
    cfg: "SubagentEstablishment.cfg",
    expectation: "pass",
    description: "S2 establishment/join protocol (current discipline)",
  },
  {
    spec: "SubagentEstablishment.tla",
    cfg: "SubagentEstablishmentRace.cfg",
    expectation: "fail",
    expectedViolation: "ChildTurnRequiresLineage",
    description: "negative control: the plan SS7(a) child-before-lineage race exists today",
  },
  {
    spec: "SubagentEstablishment.tla",
    cfg: "SubagentEstablishmentFix.cfg",
    expectation: "pass",
    description: "AwaitParentEstablishment discipline eliminates the race",
  },
];

class JavaNotFound extends Schema.TaggedError<JavaNotFound>()("JavaNotFound", {
  message: Schema.String,
}) {}

class ToolsUnavailable extends Schema.TaggedError<ToolsUnavailable>()("ToolsUnavailable", {
  message: Schema.String,
}) {}

class VerdictMismatch extends Schema.TaggedError<VerdictMismatch>()("VerdictMismatch", {
  cfg: Schema.String,
  expected: Schema.String,
  actual: Schema.String,
  detail: Schema.String,
}) {
  override get message() {
    return `${this.cfg}: expected ${this.expected} but got ${this.actual}\n${this.detail}`;
  }
}

const javaFlag = Flag.string("java").pipe(
  Flag.optional,
  Flag.withDescription("Path to the java binary (defaults to JAVA_HOME/bin/java, then PATH)."),
);

const toolsFlag = Flag.string("tools").pipe(
  Flag.optional,
  Flag.withDescription(
    "Path to tla2tools.jar (>= 1.8.0). Defaults to $TLA2TOOLS_JAR, then formal/tla2tools.jar, then a cached download.",
  ),
);

const parseOnly = Flag.boolean("parse-only").pipe(
  Flag.withDescription("Run the SANY parser on both specifications and skip model checking."),
);

const skipDownload = Flag.boolean("skip-download").pipe(
  Flag.withDescription("Never download tla2tools.jar; fail if no local copy exists."),
);

const resolveFormalDir = Effect.fn("formalCheck.resolveFormalDir")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  return path.resolve(path.dirname(scriptPath), "..", "formal");
});

/** Run a command, returning combined output and exit code without failing. */
const runCommand = Effect.fn("formalCheck.runCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make(command, args, { cwd, stderr: "pipe", stdout: "pipe" });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  return { exitCode, output: output.trim() };
});

/** Locate a working `java`: --java flag, then JAVA_HOME/bin/java, then PATH. */
const locateJava = Effect.fn("formalCheck.locateJava")(function* (
  flagValue: Option.Option<string>,
  formalDir: string,
) {
  const path = yield* Path.Path;
  const javaHome = yield* Config.string("JAVA_HOME").pipe(Config.option, Effect.orDie);
  const candidates: Array<string> = [];
  if (Option.isSome(flagValue)) candidates.push(flagValue.value);
  if (Option.isSome(javaHome)) candidates.push(path.join(javaHome.value, "bin", "java"));
  candidates.push("java");
  for (const candidate of candidates) {
    const probe = yield* runCommand(formalDir, candidate, ["-version"]).pipe(
      Effect.orElseSucceed(() => ({ exitCode: 1, output: "" })),
    );
    if (probe.exitCode === 0) {
      const version = probe.output.split("\n")[0] ?? "unknown";
      return { java: candidate, version };
    }
  }
  return yield* JavaNotFound.make({ message: JAVA_INSTALL_GUIDANCE });
});

/** Locate tla2tools.jar: --tools, $TLA2TOOLS_JAR, formal/, cache, download. */
const locateTools = Effect.fn("formalCheck.locateTools")(function* (
  flagValue: Option.Option<string>,
  formalDir: string,
  allowDownload: boolean,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const fromEnv = yield* Config.string("TLA2TOOLS_JAR").pipe(Config.option, Effect.orDie);
  const cachePath = path.resolve(
    formalDir,
    "..",
    "node_modules",
    ".cache",
    "effect-agent",
    "tla2tools.jar",
  );
  const candidates = [
    ...(Option.isSome(flagValue) ? [flagValue.value] : []),
    ...(Option.isSome(fromEnv) ? [fromEnv.value] : []),
    path.join(formalDir, "tla2tools.jar"),
    cachePath,
  ];
  for (const candidate of candidates) {
    if (yield* fs.exists(candidate).pipe(Effect.orElseSucceed(() => false))) {
      return candidate;
    }
  }
  if (!allowDownload) {
    return yield* ToolsUnavailable.make({
      message: `No tla2tools.jar found (checked: ${candidates.join(", ")}) and --skip-download was set. Download it from ${TLA2TOOLS_RELEASE_URL}.`,
    });
  }
  yield* Console.log(`Downloading tla2tools.jar (>= 1.8.0) to ${cachePath} ...`);
  const bytes = yield* Effect.tryPromise({
    try: async () => {
      const response = await fetch(TLA2TOOLS_RELEASE_URL, { redirect: "follow" });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return new Uint8Array(await response.arrayBuffer());
    },
    catch: (cause) =>
      ToolsUnavailable.make({
        message: `Downloading tla2tools.jar failed (${String(cause)}). Download it manually from ${TLA2TOOLS_RELEASE_URL} and pass --tools <path>.`,
      }),
  });
  yield* fs.makeDirectory(path.dirname(cachePath), { recursive: true }).pipe(Effect.orDie);
  yield* fs.writeFile(cachePath, bytes).pipe(Effect.orDie);
  return cachePath;
});

const summarize = (output: string): string => {
  const interesting = output
    .split("\n")
    .filter(
      (line) =>
        line.startsWith("Error:") ||
        line.includes("states generated") ||
        line.includes("Model checking completed") ||
        line.startsWith("Finished in"),
    );
  return interesting.slice(0, 6).join("\n  ");
};

const checkInstance = Effect.fn("formalCheck.checkInstance")(function* (
  java: string,
  jar: string,
  formalDir: string,
  instance: FormalInstance,
) {
  yield* Console.log(`\n== ${instance.cfg} — ${instance.description}`);
  const result = yield* runCommand(formalDir, java, [
    "-XX:+UseParallelGC",
    "-cp",
    jar,
    "tlc2.TLC",
    "-deadlock",
    "-workers",
    "auto",
    "-config",
    instance.cfg,
    instance.spec,
  ]);
  const passed = result.exitCode === 0 && result.output.includes("Model checking completed");
  if (instance.expectation === "pass") {
    if (!passed) {
      return yield* VerdictMismatch.make({
        cfg: instance.cfg,
        expected: "pass",
        actual: `exit ${result.exitCode}`,
        detail: summarize(result.output),
      });
    }
    yield* Console.log(`  PASS\n  ${summarize(result.output)}`);
    return;
  }
  const violated =
    instance.expectedViolation === undefined ||
    result.output.includes(`Invariant ${instance.expectedViolation} is violated`);
  if (passed || !violated) {
    return yield* VerdictMismatch.make({
      cfg: instance.cfg,
      expected: `violation of ${instance.expectedViolation ?? "an invariant"}`,
      actual: passed ? "pass" : `exit ${result.exitCode} without the expected violation`,
      detail: summarize(result.output),
    });
  }
  yield* Console.log(
    `  FAILED AS EXPECTED (${instance.expectedViolation} violated — the invariant is load-bearing)`,
  );
});

const formalCheck = Effect.fn("formalCheck")(function* (options: {
  readonly java: Option.Option<string>;
  readonly tools: Option.Option<string>;
  readonly parseOnly: boolean;
  readonly skipDownload: boolean;
}) {
  const formalDir = yield* resolveFormalDir();
  const located = yield* locateJava(options.java, formalDir);
  yield* Console.log(`java: ${located.java} (${located.version})`);
  const jar = yield* locateTools(options.tools, formalDir, !options.skipDownload);
  yield* Console.log(`tla2tools: ${jar}`);
  const specs = [...new Set(INSTANCES.map((instance) => instance.spec))];

  yield* Console.log(`\n== SANY parse: ${specs.join(", ")}`);
  const sany = yield* runCommand(formalDir, located.java, ["-cp", jar, "tla2sany.SANY", ...specs]);
  if (sany.exitCode !== 0 || sany.output.includes("Fatal errors")) {
    return yield* VerdictMismatch.make({
      cfg: "SANY",
      expected: "parse success",
      actual: `exit ${sany.exitCode}`,
      detail: sany.output.split("\n").slice(-12).join("\n  "),
    });
  }
  yield* Console.log("  parsed cleanly");
  if (options.parseOnly) {
    yield* Console.log("\n--parse-only: skipping TLC model checking.");
    return;
  }

  for (const instance of INSTANCES) {
    yield* checkInstance(located.java, jar, formalDir, instance);
  }
  yield* Console.log(
    "\nAll formal instances match their committed verdicts (see formal/EVIDENCE.md).",
  );
});

const command = CliCommand.make(
  "formal-check",
  { java: javaFlag, parseOnly, skipDownload, tools: toolsFlag },
  formalCheck,
).pipe(
  CliCommand.withDescription(
    "Parse (SANY) and model-check (TLC) the formal/ TLA+ specifications against every committed bounded instance, asserting the expected verdict of each — including that the negative-control instances FAIL. Needs Java 17+; not part of `bun run ready`.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
