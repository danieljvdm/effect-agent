import { Console, Effect, FileSystem, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { EvaluationReport } from "./contracts.ts";
import { runEvaluation } from "./evaluate.ts";

const cachePath = Flag.directory("cache").pipe(
  Flag.withDefault("/tmp/effect-agent-semantic-model"),
  Flag.withDescription("Transformers.js model cache directory."),
);

const output = Flag.file("output").pipe(
  Flag.withDefault("/tmp/kom18-evaluation.json"),
  Flag.withDescription("Destination for the single schema-encoded JSON report."),
);

const offline = Flag.boolean("offline").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Require the exact pinned model revision to exist in the local cache."),
);

const environment = Flag.string("environment").pipe(
  Flag.withDefault("unspecified"),
  Flag.withDescription("Reproducibility descriptor recorded verbatim in the report."),
);

const gitRevision = Flag.string("git-revision").pipe(
  Flag.withDefault("working-tree"),
  Flag.withDescription("Git revision descriptor recorded verbatim in the report."),
);

export const command = Command.make(
  "semantic-memory-eval",
  { cachePath, environment, gitRevision, offline, output },
  Effect.fn("SemanticMemoryEvaluation.command")(function* (options) {
    const report = yield* runEvaluation({
      cachePath: options.cachePath,
      environment: options.environment,
      gitRevision: options.gitRevision,
      offline: options.offline,
    });

    const json = yield* Schema.encodeEffect(Schema.fromJsonString(EvaluationReport))(report);
    const fs = yield* FileSystem.FileSystem;

    yield* fs.writeFileString(options.output, `${json}\n`);
    yield* Console.log(json);
  }),
).pipe(
  Command.withDescription(
    "Run the frozen local semantic-memory retrieval evaluation without an LLM answerer or judge.",
  ),
  Command.withExamples([
    {
      command: "semantic-memory-eval --output /tmp/kom18-evaluation.json --git-revision <sha>",
      description: "Populate the pinned model cache if needed and write the evaluation report.",
    },
    {
      command:
        "semantic-memory-eval --offline --output /tmp/kom18-evaluation-offline.json --git-revision <sha>",
      description: "Verify a second run using only cached model files.",
    },
  ]),
);
