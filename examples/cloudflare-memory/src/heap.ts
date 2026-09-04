import { Console, Crypto, Effect, Encoding, FileSystem, Path, Schema, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

import { HeapProbeError, NodeSample, WorkerdSample } from "./heap-contracts.ts";
import { measureWorkerd } from "./heap-workerd.ts";

const ModuleContribution = Schema.Struct({ path: Schema.String, bytes: Schema.Number });

const Bundle = Schema.Struct({
  name: Schema.String,
  file: Schema.String,
  bytes: Schema.Number,
  sha256: Schema.String,
  modules: Schema.Array(ModuleContribution),
  nodeSamples: Schema.Array(NodeSample),
});

const Report = Schema.Struct({
  version: Schema.Literal(1),
  revision: Schema.String,
  dirty: Schema.Boolean,
  wrangler: Schema.String,
  miniflare: Schema.String,
  effect: Schema.String,
  objectCount: Schema.Number,
  sampleCount: Schema.Number,
  bundles: Schema.Array(Bundle),
  workerdSamples: Schema.Array(WorkerdSample),
  notes: Schema.Array(Schema.String),
});

const Metafile = Schema.Struct({
  outputs: Schema.Record(
    Schema.String,
    Schema.Struct({
      bytes: Schema.Number,
      entryPoint: Schema.optionalKey(Schema.String),
      inputs: Schema.Record(Schema.String, Schema.Struct({ bytesInOutput: Schema.Number })),
    }),
  ),
});

const run = Effect.fn("heap.run")(function* (
  cwd: string,
  args: ReadonlyArray<string>,
  env?: Record<string, string>,
) {
  const child = yield* ChildProcess.make("vp", ["exec", ...args], {
    cwd,
    stdout: "pipe",
    stderr: "pipe",
    env,
    extendEnv: true,
  });

  const [stdout, stderr, code] = yield* Effect.all(
    [
      Stream.mkString(Stream.decodeText(child.stdout)),
      Stream.mkString(Stream.decodeText(child.stderr)),
      child.exitCode,
    ],
    { concurrency: 3 },
  );

  if (code !== 0)
    return yield* HeapProbeError.make({
      operation: `${args[0]} exited ${code}: ${stdout}${stderr}`,
    });

  return { stdout, stderr };
}, Effect.scoped);

const buildBundle = Effect.fn("heap.buildBundle")(function* (options: {
  example: string;
  output: string;
  name: string;
  config: string;
  entry?: string;
  samples: number;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const directory = path.join(options.output, options.name);

  yield* fs.makeDirectory(directory, { recursive: true });

  const built = yield* run(
    options.example,
    [
      "wrangler",
      "deploy",
      "--dry-run",
      ...(options.entry ? [options.entry] : []),
      "--config",
      options.config,
      "--outdir",
      directory,
      "--metafile",
      path.join(directory, "meta.json"),
    ],
    { WRANGLER_SEND_METRICS: "false", WRANGLER_LOG_PATH: path.join(directory, "wrangler.log") },
  );

  yield* fs.writeFileString(path.join(directory, "build.log"), built.stdout + built.stderr);

  const meta = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Metafile))(
    yield* fs.readFileString(path.join(directory, "meta.json")),
  );

  const output = Object.entries(meta.outputs).find(
    ([name, value]) => name.endsWith(".js") && value.entryPoint,
  );

  if (!output)
    return yield* HeapProbeError.make({ operation: `find emitted entry ${options.name}` });
  const file = path.join(directory, path.basename(output[0]));
  const bytes = yield* fs.readFile(file);

  const modules = Object.entries(output[1].inputs)
    .filter(([, value]) => value.bytesInOutput > 0)
    .map(([path, value]) => ({ path, bytes: value.bytesInOutput }))
    .sort((a, b) => b.bytes - a.bytes);

  const nodeSamples = yield* Effect.forEach(
    Array.from({ length: options.samples }, (_, i) => i),
    Effect.fn(function* (i) {
      const sample = yield* run(options.example, [
        "node",
        "--experimental-transform-types",
        "--expose-gc",
        "--experimental-vm-modules",
        path.join(options.example, "src", "heap-node-main.ts"),
        file,
      ]);

      yield* fs.writeFileString(path.join(directory, `node-${i + 1}.json`), sample.stdout);

      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(NodeSample))(sample.stdout);
    }),
  );

  return Bundle.make({
    name: options.name,
    file,
    bytes: bytes.byteLength,
    sha256: Encoding.encodeHex(yield* crypto.digest("SHA-256", bytes)),
    modules,
    nodeSamples,
  });
});

const version = Effect.fn("heap.version")(function* (filename: string) {
  const fs = yield* FileSystem.FileSystem;

  return (yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
  )(yield* fs.readFileString(filename))).version;
});

const git = Effect.fn("heap.git")(function* (root: string, args: ReadonlyArray<string>) {
  const child = yield* ChildProcess.make("git", args, {
    cwd: root,
    stdout: "pipe",
    stderr: "pipe",
  });

  const [text, code] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.stdout)),
    child.exitCode,
  ]);

  if (code !== 0) return yield* HeapProbeError.make({ operation: `git ${args.join(" ")}` });

  return text.trim();
}, Effect.scoped);

export const command = Command.make(
  "measure-heap",
  {
    output: Flag.string("out-dir").pipe(
      Flag.withDescription(
        "New or existing directory for exact bundles, samples, and report.json.",
      ),
    ),
    samples: Flag.integer("samples").pipe(
      Flag.withDefault(3),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }))),
      Flag.withDescription("Fresh processes and local workerd instances per cohort, 1–20."),
    ),
    objects: Flag.integer("objects").pipe(
      Flag.withDefault(4),
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 }))),
      Flag.withDescription("Initialized and concurrently active Thread Objects, 1–16."),
    ),
  },
  Effect.fn(function* ({ output, samples, objects }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const example = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    const root = path.resolve(example, "../..");

    output = path.resolve(output);
    yield* fs.makeDirectory(output, { recursive: true });
    // Remove only our result: a failed invocation must not leave an older success report.
    yield* fs.remove(path.join(output, "report.json"), { force: true });
    const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-heap-root-" });
    const source = yield* fs.readFileString(path.join(example, "src", "worker.ts"));

    const directImport =
      'import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";';

    if (!source.includes(directImport))
      return yield* HeapProbeError.make({
        operation: "memory Worker import changed; update the equivalent root consumer",
      });
    yield* fs.writeFileString(
      path.join(scratch, "worker.ts"),
      source.replace(
        directImport,
        'import { ThreadObject } from "@effect-agent/platform-cloudflare";',
      ),
    );
    yield* fs.copyFile(path.join(scratch, "worker.ts"), path.join(output, "root-consumer.ts"));
    yield* fs.copyFile(
      path.join(example, "src", "contracts.ts"),
      path.join(scratch, "contracts.ts"),
    );
    yield* fs.symlink(path.join(example, "node_modules"), path.join(scratch, "node_modules"));
    const common = { example, output, samples, config: path.join(example, "wrangler.jsonc") };

    yield* Console.error("Building and measuring exact Worker bundles...");
    const direct = yield* buildBundle({ ...common, name: "direct" });

    const rootImport = yield* buildBundle({
      ...common,
      name: "root",
      entry: path.join(scratch, "worker.ts"),
    });

    const runtime = yield* buildBundle({
      ...common,
      name: "runtime",
      config: path.join(example, "wrangler.heap.jsonc"),
    });

    yield* Console.error(`Sampling local workerd with ${objects} active Thread Objects...`);

    const workerdSamples = yield* Effect.forEach(
      Array.from({ length: samples }, (_, i) => i),
      Effect.fn(function* (i) {
        const sample = yield* measureWorkerd(runtime.file, objects);

        yield* fs.writeFileString(
          path.join(output, `workerd-${i + 1}.json`),
          yield* Schema.encodeEffect(Schema.fromJsonString(WorkerdSample))(sample),
        );

        return sample;
      }),
    );

    const report = Report.make({
      version: 1,
      revision: yield* git(root, ["rev-parse", "HEAD"]),
      dirty: (yield* git(root, ["status", "--porcelain"])).length > 0,
      wrangler: yield* version(path.join(example, "node_modules/wrangler/package.json")),
      miniflare: yield* version(path.join(example, "node_modules/miniflare/package.json")),
      effect: yield* version(path.join(root, "node_modules/effect/package.json")),
      objectCount: objects,
      sampleCount: samples,
      bundles: [direct, rootImport, runtime],
      workerdSamples,
      notes: [
        "Node VM results measure exact-bundle module evaluation relative to a preloaded Effect harness and empty realm. Cloudflare external modules are shimmed; no handlers execute in Node.",
        "Local workerd Runtime.getHeapUsage values are instantaneous JS heap snapshots for one Worker isolate, not peak usage, process RSS, total isolate memory, or a hosted capacity guarantee. Embedder/backing storage counters are reported separately when supported.",
        "Startup follows a /ready request with no Objects. Initialized follows one status RPC per Object. Active requires every Object to have started both tools, with lookup blocked at an explicit gate. Settled requires completed receipts and exactly two model calls and two tool calls per Object.",
        "Synthetic model, two registered tools, 4096-character input per Object, two 16384-character tool results per run. No provider, remote binding, network tool, or deployment is used.",
        "Local workerd GC is not forced. Node reports both post-evaluation and explicit post-GC deltas. Neither sampled value establishes peak memory.",
      ],
    });

    yield* fs.writeFileString(
      path.join(output, "report.json"),
      yield* Schema.encodeEffect(Schema.fromJsonString(Report))(report),
    );
    yield* Console.log(`Memory report: ${path.join(output, "report.json")}`);
  }),
).pipe(
  Command.withDescription(
    "Measure bundled startup heap and synthetic concurrent Thread Objects locally; never deploys.",
  ),
);
