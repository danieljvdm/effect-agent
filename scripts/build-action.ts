import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Crypto, Effect, Encoding, FileSystem, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Bundle both JavaScript Action entrypoints into committed dist files. A
// node-runtime Action must run directly from an immutable checkout without an
// install step. `--check` rebuilds to scratch and rejects stale bundles using
// stable input and artifact digests; Bun may reorder equivalent bundle output.
// ---------------------------------------------------------------------------

const bundles = [
  {
    entry: "packages/pr-review-action/src/action-entry.ts",
    bundle: "action/dist/index.mjs",
    manifest: "action/dist/index.manifest.json",
    scratch: "node_modules/.tmp/action-dist-check/index.mjs",
    metafile: "node_modules/.tmp/action-dist-check/pr-review.json",
  },
  {
    entry: "examples/pr-work-order-ingress/src/action-entry.ts",
    bundle: "work-order-action/dist/index.mjs",
    manifest: "work-order-action/dist/index.manifest.json",
    scratch: "node_modules/.tmp/action-dist-check/index.mjs",
    metafile: "node_modules/.tmp/action-dist-check/pr-work-order.json",
  },
] as const;

const buildInputs = ["scripts/build-action.ts", "package.json", "bun.lock"] as const;

const Sha256 = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const BunMetafile = Schema.Struct({
  inputs: Schema.Record(Schema.String, Schema.Unknown),
});
const BundleManifest = Schema.Struct({
  version: Schema.Literal(1),
  inputsSha256: Sha256,
  bundleSha256: Sha256,
});

const decodeMetafile = Schema.decodeUnknownEffect(Schema.fromJsonString(BunMetafile));
const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(BundleManifest));
const encodeManifest = Schema.encodeEffect(Schema.fromJsonString(BundleManifest));

class CommandError extends Schema.TaggedError<CommandError>()("CommandError", {
  command: Schema.String,
  exitCode: Schema.Int,
  output: Schema.String,
}) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

class StaleBundleError extends Schema.TaggedError<StaleBundleError>()("StaleBundleError", {
  bundle: Schema.String,
}) {
  override get message() {
    return `${this.bundle} is stale: rebuild it with \`vp run action:build\` and commit the result.`;
  }
}

const runCommand = Effect.fn("runCommand")(function* (
  command: string,
  args: ReadonlyArray<string>,
) {
  const formatted = [command, ...args].join(" ");
  const child = yield* ChildProcess.make(command, args, { stderr: "pipe", stdout: "pipe" });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();
  if (exitCode !== 0) {
    return yield* CommandError.make({ command: formatted, exitCode, output: trimmed });
  }
  return trimmed;
});

const sha256 = Effect.fn("sha256")(function* (bytes: Uint8Array) {
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", bytes);
  return yield* Schema.decodeUnknownEffect(Sha256)(Encoding.encodeHex(digest));
});

const fingerprintInputs = Effect.fn("fingerprintInputs")(function* (metafilePath: string) {
  const fs = yield* FileSystem.FileSystem;
  const metafile = yield* decodeMetafile(yield* fs.readFileString(metafilePath));
  const inputs = [...Object.keys(metafile.inputs), ...buildInputs]
    .map((source) => ({ source, key: source.replaceAll("\\", "/") }))
    .sort((left, right) => (left.key < right.key ? -1 : left.key > right.key ? 1 : 0));
  const records: Array<string> = [];
  for (const input of inputs) {
    const digest = yield* sha256(yield* fs.readFile(input.source));
    records.push(`${input.key}\0${digest}\n`);
  }
  return yield* sha256(new TextEncoder().encode(records.join("")));
});

const bundleTo = Effect.fn("bundleTo")(function* (
  entry: string,
  outfile: string,
  metafile: string,
) {
  yield* runCommand("bun", [
    "build",
    entry,
    "--target=node",
    "--format=esm",
    `--outfile=${outfile}`,
    `--metafile=${metafile}`,
  ]);
});

const checkFlag = Flag.boolean("check").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Rebuild to a scratch path and fail if the committed bundle is stale."),
);

const command = CliCommand.make("build-action", { check: checkFlag }, ({ check }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    yield* fs.makeDirectory("node_modules/.tmp/action-dist-check", { recursive: true });
    if (!check) {
      for (const item of bundles) {
        yield* bundleTo(item.entry, item.bundle, item.metafile);
        const manifest = BundleManifest.make({
          version: 1,
          inputsSha256: yield* fingerprintInputs(item.metafile),
          bundleSha256: yield* sha256(yield* fs.readFile(item.bundle)),
        });
        yield* fs.writeFileString(item.manifest, `${yield* encodeManifest(manifest)}\n`);
        const stat = yield* fs.stat(item.bundle);
        yield* Console.log(`Bundled ${item.entry} -> ${item.bundle} (${stat.size} bytes).`);
      }
      return;
    }
    for (const item of bundles) {
      yield* bundleTo(item.entry, item.scratch, item.metafile);
      const manifest = yield* decodeManifest(yield* fs.readFileString(item.manifest));
      const inputsSha256 = yield* fingerprintInputs(item.metafile);
      const bundleSha256 = yield* sha256(yield* fs.readFile(item.bundle));
      if (manifest.inputsSha256 !== inputsSha256 || manifest.bundleSha256 !== bundleSha256) {
        return yield* StaleBundleError.make({ bundle: item.bundle });
      }
      yield* Console.log(`${item.bundle} is up to date.`);
    }
  }),
).pipe(
  CliCommand.withDescription(
    "Bundle the committed PR-review and PR-work-order GitHub Action entrypoints.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program);
