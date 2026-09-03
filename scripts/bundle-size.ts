import { gzip } from "node:zlib";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Option, Path, Schema, Stream } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";
import { analyzeMetafile, build, version as esbuildVersion } from "esbuild";

import { PublishManifest, withPublishManifests } from "./release-publish.ts";

class BundleSizeError extends Schema.TaggedError<BundleSizeError>()("BundleSizeError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const Bytes = Schema.Struct({ raw: Schema.Int, gzip: Schema.Int });

const BundleSize = Schema.Struct({
  initial: Bytes,
  deferred: Bytes,
  total: Bytes,
  chunks: Schema.Array(
    Schema.Struct({ file: Schema.String, initial: Schema.Boolean, ...Bytes.fields }),
  ),
});

const BundleReport = Schema.Struct({
  esbuild: Schema.String,
  settings: Schema.Struct({
    format: Schema.Literal("esm"),
    target: Schema.Literal("es2022"),
    platform: Schema.Literal("browser"),
    minify: Schema.Literal(true),
    compression: Schema.Literal("gzip level 9 per chunk"),
  }),
  base: Schema.Struct({ revision: Schema.String, effect: Schema.String }),
  head: Schema.Struct({ revision: Schema.String, effect: Schema.String }),
  fixtures: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      base: Schema.NullOr(BundleSize),
      head: BundleSize,
      missingBaseExports: Schema.Array(Schema.String),
    }),
  ),
});

type BundleReport = typeof BundleReport.Type;

// The same consumer source is bundled against both checkouts. Direct entry points
// added by this PR have no historical baseline; never report those as a saving.
const fixtures = [
  { name: "agent-root", requires: ["effect-agent"] },
  { name: "agent-module", requires: ["effect-agent/Agent"] },
  { name: "runtime-root", requires: ["effect-agent"] },
  { name: "runtime-module", requires: ["effect-agent/AgentRuntime"] },
  { name: "lazy-root", requires: ["effect-agent"] },
  { name: "lazy-module", requires: ["effect-agent/Agent", "effect-agent/AgentRuntime"] },
];

// Effect has no compression platform service. Keep Node's zlib at this typed
// adapter boundary; compress each emitted chunk separately, as HTTP would.
const gzipBytes = (bytes: Uint8Array) =>
  Effect.callback<number, BundleSizeError>((resume) => {
    gzip(bytes, { level: 9 }, (cause, compressed) =>
      resume(
        cause
          ? Effect.fail(new BundleSizeError({ message: "Could not gzip bundle chunk", cause }))
          : Effect.succeed(compressed.byteLength),
      ),
    );
  });

/** Measure the actual emitted graph, including shared static dependencies of lazy entries. */
const measureBundle = Effect.fn("bundleSize.measureBundle")(function* (options: {
  readonly root: string;
  readonly entry: string;
  readonly dependencies: string;
  readonly output: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  // esbuild canonicalizes symlinks, including macOS /var -> /private/var.
  const root = yield* fs.realPath(options.root);
  const sourceEntry = yield* fs.realPath(options.entry);

  const result = yield* Effect.tryPromise({
    try: () =>
      build({
        absWorkingDir: root,
        entryPoints: { entry: sourceEntry },
        outdir: "output",
        outExtension: { ".js": ".mjs" },
        nodePaths: [options.dependencies],
        bundle: true,
        treeShaking: true,
        minify: true,
        splitting: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        define: { "process.env.NODE_ENV": '"production"' },
        legalComments: "none",
        sourcemap: false,
        metafile: true,
        write: false,
        logLevel: "silent",
      }),
    catch: (cause) =>
      new BundleSizeError({
        message: `Could not bundle ${options.entry}: ${String(cause)}`,
        cause,
      }),
  });

  const outputs = result.metafile.outputs;

  const entry = Object.keys(outputs).find(
    (file) => path.resolve(root, outputs[file]?.entryPoint ?? "") === sourceEntry,
  );

  if (entry === undefined) {
    return yield* new BundleSizeError({ message: `No output entry for ${options.entry}` });
  }

  const initial = new Set<string>();
  const pending = [entry];

  while (pending.length > 0) {
    const file = pending.pop();

    if (file === undefined || initial.has(file)) continue;
    initial.add(file);
    for (const imported of outputs[file]?.imports ?? []) {
      if (imported.external) {
        return yield* new BundleSizeError({
          message: `Unbundled dependency ${imported.path} in ${options.entry}`,
        });
      }
      if (imported.kind !== "dynamic-import") pending.push(imported.path);
    }
  }

  // Also reject unresolved/external imports in deferred output.
  for (const output of Object.values(outputs)) {
    if (output.imports.some((imported) => imported.external)) {
      return yield* new BundleSizeError({ message: `External import in ${options.entry}` });
    }
  }

  yield* fs.makeDirectory(options.output, { recursive: true });

  const chunks = yield* Effect.forEach(
    result.outputFiles,
    Effect.fn(function* (output) {
      const file = path.relative(root, output.path).replaceAll("\\", "/");

      yield* fs.writeFile(path.join(options.output, path.basename(file)), output.contents);

      return {
        file: path.basename(file),
        initial: initial.has(file),
        raw: output.contents.byteLength,
        gzip: yield* gzipBytes(output.contents),
      };
    }),
  );

  const sum = (selected: typeof chunks) => ({
    raw: selected.reduce((total, chunk) => total + chunk.raw, 0),
    gzip: selected.reduce((total, chunk) => total + chunk.gzip, 0),
  });

  yield* fs.writeFileString(
    path.join(options.output, "meta.json"),
    JSON.stringify(result.metafile, null, 2),
  );

  const analysis = yield* Effect.tryPromise({
    try: () => analyzeMetafile(result.metafile, { verbose: true }),
    catch: (cause) => new BundleSizeError({ message: "Could not analyze bundle", cause }),
  });

  yield* fs.writeFileString(path.join(options.output, "modules.txt"), analysis);

  return {
    initial: sum(chunks.filter((chunk) => chunk.initial)),
    deferred: sum(chunks.filter((chunk) => !chunk.initial)),
    total: sum(chunks),
    chunks,
  } satisfies typeof BundleSize.Type;
});

const revision = Effect.fn("bundleSize.revision")(function* (root: string) {
  const child = yield* ChildProcess.make("git", ["rev-parse", "HEAD"], {
    cwd: root,
    stdout: "pipe",
    stderr: "ignore",
  });

  const [output, code] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.stdout)),
    child.exitCode,
  ]);

  return code === 0 ? output.trim() : "unversioned checkout";
}, Effect.scoped);

// Size alone cannot prove that tree shaking preserves required initialization.
// Execute a real consumer against the same published files used by the report.
const verifyRuntime = Effect.fn("bundleSize.verifyRuntime")(
  function* (options: {
    readonly root: string;
    readonly dependencies: string;
    readonly output: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const root = yield* fs.realPath(options.root);

    const result = yield* Effect.tryPromise({
      try: () =>
        build({
          absWorkingDir: root,
          entryPoints: { entry: path.join(root, "fixtures", "runtime-smoke.ts") },
          outdir: options.output,
          outExtension: { ".js": ".mjs" },
          nodePaths: [options.dependencies],
          bundle: true,
          treeShaking: true,
          minify: true,
          splitting: true,
          format: "esm",
          platform: "node",
          target: "es2022",
          define: { "process.env.NODE_ENV": '"production"', "import.meta.main": "true" },
          legalComments: "none",
          write: false,
          logLevel: "silent",
        }),
      catch: (cause) =>
        new BundleSizeError({
          message: `Could not bundle runtime smoke check: ${String(cause)}`,
          cause,
        }),
    });

    yield* fs.makeDirectory(options.output, { recursive: true });
    for (const file of result.outputFiles) {
      yield* fs.writeFile(file.path, file.contents);
    }

    const child = yield* ChildProcess.make("node", [path.join(options.output, "entry.mjs")], {
      cwd: root,
      stdout: "pipe",
      stderr: "pipe",
    });

    const [stdout, stderr, code] = yield* Effect.all(
      [
        Stream.mkString(Stream.decodeText(child.stdout)),
        Stream.mkString(Stream.decodeText(child.stderr)),
        child.exitCode,
      ],
      { concurrency: 3 },
    );

    yield* fs.writeFileString(path.join(options.output, "result.txt"), stdout + stderr);
    if (code !== 0) {
      return yield* new BundleSizeError({
        message: `Bundled runtime smoke check exited ${code}: ${stdout}${stderr}`,
      });
    }
  },
  Effect.scoped,
  Effect.timeout("30 seconds"),
);

const measureCheckout = Effect.fn("bundleSize.measureCheckout")(function* (
  root: string,
  fixtureDirectory: string,
  output: string,
  allowMissing: boolean,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const stage = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-bundle-" });
  const available = new Set<string>();
  const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(PublishManifest));

  yield* fs.copyFile(path.join(root, "package.json"), path.join(stage, "package.json"));
  yield* fs.copy(fixtureDirectory, path.join(stage, "fixtures"));

  for (const directory of (yield* fs.readDirectory(path.join(root, "packages"))).sort()) {
    if (directory.startsWith(".")) continue;
    const source = path.join(root, "packages", directory);

    const manifest = yield* decodeManifest(
      yield* fs.readFileString(path.join(source, "package.json")),
    );

    if (manifest.private === true) continue;
    const destination = path.join(stage, "packages", directory);

    yield* fs.makeDirectory(destination, { recursive: true });
    yield* fs.copyFile(path.join(source, "package.json"), path.join(destination, "package.json"));
    // Copy only built files. Source imports cannot accidentally resolve here.
    yield* fs.copy(path.join(source, "dist"), path.join(destination, "dist"));
    const link = path.join(stage, "node_modules", manifest.name);

    yield* fs.makeDirectory(path.dirname(link), { recursive: true });
    yield* fs.symlink(destination, link);
    for (const key of Object.keys(manifest.exports ?? {})) {
      available.add(key === "." ? manifest.name : manifest.name + key.slice(1));
    }
  }

  const results = yield* withPublishManifests(stage, () =>
    Effect.forEach(
      fixtures,
      Effect.fn(function* (fixture) {
        const missing = fixture.requires.filter((required) => !available.has(required));

        if (missing.length > 0) {
          if (allowMissing) return { name: fixture.name, size: null, missing };

          return yield* new BundleSizeError({
            message: `Missing public exports: ${missing.join(", ")}`,
          });
        }

        const size = yield* measureBundle({
          root: stage,
          entry: path.join(stage, "fixtures", fixture.name + ".ts"),
          dependencies: path.join(root, "node_modules"),
          output: path.join(output, fixture.name),
        });

        return { name: fixture.name, size, missing };
      }),
    ).pipe(
      Effect.tap(() =>
        allowMissing
          ? Effect.void
          : verifyRuntime({
              root: stage,
              dependencies: path.join(root, "node_modules"),
              output: path.join(output, "runtime-smoke"),
            }),
      ),
    ),
  );

  const effect = yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(Schema.Struct({ version: Schema.String })),
  )(yield* fs.readFileString(path.join(root, "node_modules", "effect", "package.json")));

  return { results, environment: { revision: yield* revision(root), effect: effect.version } };
});

const kb = (bytes: number) => `${(bytes / 1000).toFixed(2)} kB`;

const renderBundleReport = (report: BundleReport) => {
  const lines = [
    "| Fixture | Part | Base gzip | PR gzip | Change | PR minified |",
    "| --- | --- | ---: | ---: | ---: | ---: |",
  ];

  for (const fixture of report.fixtures) {
    for (const part of ["initial", "deferred", "total"] as const) {
      if (
        part === "deferred" &&
        fixture.head.deferred.raw === 0 &&
        (fixture.base?.deferred.raw ?? 0) === 0
      )
        continue;
      if (
        part === "total" &&
        fixture.head.deferred.raw === 0 &&
        (fixture.base?.deferred.raw ?? 0) === 0
      )
        continue;
      const before = fixture.base?.[part].gzip;
      const after = fixture.head[part].gzip;
      const delta = before === undefined ? undefined : after - before;
      const sign = delta !== undefined && delta > 0 ? "+" : "";

      const change =
        delta === undefined || before === undefined
          ? "new export"
          : `${sign}${kb(delta)}${before === 0 ? "" : ` / ${sign}${((delta / before) * 100).toFixed(2)}%`}`;

      lines.push(
        `| ${fixture.name} | ${part} | ${before === undefined ? "n/a" : kb(before)} | ${kb(after)} | ${change} | ${kb(fixture.head[part].raw)} |`,
      );
    }
  }

  return lines.join("\n") + "\n";
};

export const compareBundles = Effect.fn("bundleSize.compareBundles")(
  function* (options: {
    readonly root: string;
    readonly base: string;
    readonly output: string;
    readonly baseRevision?: string;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const fixtureDirectory = path.join(options.root, "scripts", "bundle");

    // Only replace analyzer-owned output. Failed or changed baselines must not
    // leave an older success report or obsolete hashed chunks in the artifacts.
    for (const generated of ["base", "head", "report.json", "report.md"]) {
      yield* fs.remove(path.join(options.output, generated), { recursive: true, force: true });
    }

    const base = yield* measureCheckout(
      options.base,
      fixtureDirectory,
      path.join(options.output, "base"),
      true,
    );

    const head = yield* measureCheckout(
      options.root,
      fixtureDirectory,
      path.join(options.output, "head"),
      false,
    );

    const comparisons: BundleReport["fixtures"][number][] = [];

    for (const current of head.results) {
      const previous = base.results.find((fixture) => fixture.name === current.name);

      if (current.size === null || previous === undefined) {
        return yield* new BundleSizeError({
          message: `Incomplete measurement for ${current.name}`,
        });
      }
      comparisons.push({
        name: current.name,
        head: current.size,
        base: previous.size,
        missingBaseExports: previous.missing,
      });
    }

    const report: BundleReport = {
      esbuild: esbuildVersion,
      settings: {
        format: "esm",
        target: "es2022",
        platform: "browser",
        minify: true,
        compression: "gzip level 9 per chunk",
      },
      base: { ...base.environment, revision: options.baseRevision ?? base.environment.revision },
      head: head.environment,
      fixtures: comparisons,
    };

    yield* fs.writeFileString(
      path.join(options.output, "report.json"),
      yield* Schema.encodeEffect(Schema.fromJsonString(BundleReport))(report),
    );
    yield* fs.writeFileString(path.join(options.output, "report.md"), renderBundleReport(report));

    return report;
  },
  Effect.scoped,
  Effect.mapError((cause) =>
    cause._tag === "BundleSizeError"
      ? cause
      : new BundleSizeError({
          message: `Bundle comparison failed. Install dependencies and build packages in both checkouts. ${cause.message}`,
          cause,
        }),
  ),
);

export const command = Command.make(
  "bundle-size",
  {
    base: Flag.string("base-dir").pipe(
      Flag.withDescription("Base checkout with installed dependencies and built packages."),
    ),
    baseRevision: Flag.string("base-revision").pipe(
      Flag.withDescription("Revision label for a base snapshot created without Git metadata."),
      Flag.optional,
    ),
    output: Flag.string("out-dir").pipe(
      Flag.withDefault(".bundle-report"),
      Flag.withDescription("Write the comparison, emitted chunks, and module analysis here."),
    ),
  },
  Effect.fn(function* ({ base, baseRevision, output }) {
    const path = yield* Path.Path;

    const root = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    const report = yield* compareBundles({
      root,
      base: path.resolve(base),
      output: path.resolve(output),
      ...(Option.isSome(baseRevision) ? { baseRevision: baseRevision.value } : {}),
    });

    yield* Console.log(renderBundleReport(report));
  }),
).pipe(
  Command.withDescription(
    "Compare consumer bundles against another built checkout. Includes all dependencies, including Effect.",
  ),
);

if (import.meta.main) {
  NodeRuntime.runMain(
    Command.run(command, { version: "1.0.0" }).pipe(
      Effect.tapErrorTag("BundleSizeError", (error) => Console.error(error.message)),
      Effect.scoped,
      Effect.provide(NodeServices.layer),
    ),
    { disableErrorReporting: true },
  );
}
