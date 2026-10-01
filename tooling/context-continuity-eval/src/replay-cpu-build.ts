import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Crypto, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { Command, Flag } from "effect/cli";
import { Hex } from "effect/encoding";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { build } from "esbuild";

import { ReplayCpuBuild, ReplayCpuError } from "./replay-cpu-contracts.ts";

const Manifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  exports: Schema.Record(Schema.String, Schema.String),
});

const packages = ["effect-agent", "platform-cloudflare", "storage-cloudflare", "storage-sql"];

export const requireReplayCpu = (valid: boolean, message: string) =>
  valid ? Effect.void : Effect.fail(new ReplayCpuError({ message }));

export const sha256 = Effect.fnUntraced(function* (value: string | Uint8Array) {
  const crypto = yield* Crypto.Crypto;

  return Hex.encode(
    yield* crypto.digest(
      "SHA-256",
      typeof value === "string" ? new TextEncoder().encode(value) : value,
    ),
  );
});

/** Resolve public package exports to their production build, never a workspace source entry. */
export const buildReplayCpu = Effect.fn("ReplayCpu.build")(function* (
  sourceRoot: string,
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  sourceRoot = path.resolve(sourceRoot);
  directory = path.resolve(directory);
  const fixtureRoot = path.dirname(yield* path.fromFileUrl(new URL(import.meta.url)));

  const git = (...args: ReadonlyArray<string>) =>
    spawner
      .string(ChildProcess.make("git", args, { cwd: sourceRoot }))
      .pipe(Effect.map((s) => s.trim()));

  const revision = yield* git("rev-parse", "HEAD");

  yield* requireReplayCpu(
    (yield* git("status", "--porcelain")) === "",
    "Source checkout must be clean",
  );
  yield* requireReplayCpu(
    !(yield* fs.exists(directory)),
    "Build output already exists; preserve it",
  );
  yield* fs.makeDirectory(directory, { recursive: true });

  const buildLog = yield* spawner.string(
    ChildProcess.make("vp", ["run", "-F", "@effect-agent/platform-cloudflare...", "build"], {
      cwd: sourceRoot,
    }),
  );

  yield* fs.writeFileString(path.join(directory, "packages.log"), buildLog);
  const versions: Record<string, string> = {};
  const exports = new Map<string, string>();

  for (const name of packages) {
    const base = path.join(sourceRoot, "packages", name);

    const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(
      yield* fs.readFileString(path.join(base, "package.json")),
    );

    versions[manifest.name] = manifest.version;
    for (const [key, value] of Object.entries(manifest.exports)) {
      yield* requireReplayCpu(
        value.startsWith("./src/") && value.endsWith(".ts"),
        "Unsupported public export",
      );
      const built = path.join(base, "dist", value.slice(6, -3) + ".mjs");

      yield* requireReplayCpu(yield* fs.exists(built), `Missing production export: ${built}`);
      exports.set(manifest.name + (key === "." ? "" : key.slice(1)), built);
    }
  }
  for (const name of ["effect", "effect-cf"]) {
    const manifest = yield* Schema.decodeEffect(
      Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
    )(yield* fs.readFileString(path.join(sourceRoot, "node_modules", name, "package.json")));

    versions[name] = manifest.version;
  }
  const fixtureFiles = ["replay-cpu-worker.ts", "replay-cpu-contracts.ts"];

  const fixture = yield* Effect.forEach(fixtureFiles, (name) =>
    fs
      .readFileString(path.join(fixtureRoot, name))
      .pipe(Effect.map((source) => ({ name, source }))),
  );

  const fixtureSha256 = yield* sha256(JSON.stringify(fixture));
  const lockfileSha256 = yield* sha256(yield* fs.readFile(path.join(sourceRoot, "bun.lock")));

  const result = yield* Effect.tryPromise({
    try: () =>
      build({
        absWorkingDir: sourceRoot,
        entryPoints: [path.join(fixtureRoot, "replay-cpu-worker.ts")],
        outfile: path.join(directory, "worker.mjs"),
        bundle: true,
        minify: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        conditions: ["workerd", "worker", "browser"],
        external: ["cloudflare:*", "node:*"],
        sourcemap: true,
        metafile: true,
        plugins: [
          {
            name: "public-production-exports",
            setup(builder) {
              builder.onResolve({ filter: /^[^./]/ }, (args) => {
                if (args.path.startsWith("cloudflare:") || args.path.startsWith("node:"))
                  return { path: args.path, external: true };
                if (
                  args.path === "effect-agent" ||
                  args.path.startsWith("effect-agent/") ||
                  args.path.startsWith("@effect-agent/")
                ) {
                  const built = exports.get(args.path);

                  return built === undefined
                    ? { errors: [{ text: `Unstaged public framework export: ${args.path}` }] }
                    : { path: built };
                }
                if (args.importer.startsWith(`${fixtureRoot}/`))
                  return builder.resolve(args.path, {
                    resolveDir: path.join(sourceRoot, "tooling/context-continuity-eval"),
                    kind: args.kind,
                  });
              });
            },
          },
        ],
        define: {
          BENCH_REVISION: JSON.stringify(revision),
          BENCH_FIXTURE_SHA256: JSON.stringify(fixtureSha256),
          BENCH_PACKAGE_VERSIONS: JSON.stringify(JSON.stringify(versions)),
        },
      }),
    catch: () => new ReplayCpuError({ message: "Worker bundle failed; inspect build diagnostics" }),
  });

  const inputs = Object.keys(result.metafile.inputs);

  yield* requireReplayCpu(
    !inputs.some((input) =>
      /packages\/(?:effect-agent|platform-cloudflare|storage-cloudflare|storage-sql)\/src\//.test(
        input,
      ),
    ),
    "Framework workspace source entered the production bundle",
  );
  yield* requireReplayCpu(
    (yield* git("rev-parse", "HEAD")) === revision && (yield* git("status", "--porcelain")) === "",
    "Source changed while building",
  );
  for (const { name, source } of fixture)
    yield* requireReplayCpu(
      (yield* fs.readFileString(path.join(fixtureRoot, name))) === source,
      "Benchmark fixture changed while building",
    );

  const metadata = ReplayCpuBuild.make({
    revision,
    fixtureSha256,
    lockfileSha256,
    versions,
    inputFiles: inputs.length,
    bundleSha256: yield* sha256(yield* fs.readFile(path.join(directory, "worker.mjs"))),
  });

  yield* fs.writeFileString(path.join(directory, "build.json"), JSON.stringify(metadata, null, 2));
  yield* fs.writeFileString(
    path.join(directory, "metafile.json"),
    JSON.stringify(result.metafile, null, 2),
  );
  for (const { name, source } of fixture)
    yield* fs.writeFileString(path.join(directory, name), source);

  return metadata;
});

const command = Command.make(
  "perf:cloudflare:cpu:build",
  {
    sourceRoot: Flag.Directory("source-root").pipe(Flag.withDefault(".")),
    output: Flag.Directory("output-dir"),
  },
  ({ sourceRoot, output }) => buildReplayCpu(sourceRoot, output).pipe(Effect.flatMap(Console.log)),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1" }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    ),
  );
