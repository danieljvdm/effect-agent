import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";

class UnsafeStorageTarget extends Schema.TaggedError<UnsafeStorageTarget>()("UnsafeStorageTarget", {
  message: Schema.String,
  target: Schema.String,
}) {}

const database = Flag.file("database").pipe(
  Flag.withDescription(
    "SQLite database file to reset; it must be an explicit .db, .sqlite, or .sqlite3 file inside this repository.",
  ),
);

const confirmPrivateDevelopment = Flag.boolean("confirm-private-development").pipe(
  Flag.withDefault(false),
  Flag.withDescription(
    "Confirm that this is disposable private-development data and no stored-data migration is expected.",
  ),
);

const dryRun = Flag.boolean("dry-run").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Print the files that would be removed without changing them."),
);

const resolveRepositoryRoot = Effect.fn("resetDevelopmentStorage.resolveRepositoryRoot")(
  function* () {
    const path = yield* Path.Path;
    const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
    return path.resolve(path.dirname(scriptPath), "..");
  },
);

const validateTarget = Effect.fn("resetDevelopmentStorage.validateTarget")(function* (
  requestedTarget: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const repositoryRoot = yield* resolveRepositoryRoot();
  const canonicalRoot = yield* fs.realPath(repositoryRoot);
  const target = path.resolve(repositoryRoot, requestedTarget);
  const relativeTarget = path.relative(canonicalRoot, target);
  const extension = path.extname(target).toLowerCase();

  if (
    relativeTarget.length === 0 ||
    relativeTarget === ".." ||
    relativeTarget.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relativeTarget)
  ) {
    return yield* UnsafeStorageTarget.make({
      target,
      message: "The database target must be a file inside this repository.",
    });
  }

  if (extension !== ".db" && extension !== ".sqlite" && extension !== ".sqlite3") {
    return yield* UnsafeStorageTarget.make({
      target,
      message: "The database target must end in .db, .sqlite, or .sqlite3.",
    });
  }

  const parent = path.dirname(target);
  if (!(yield* fs.exists(parent))) {
    return yield* UnsafeStorageTarget.make({
      target,
      message: "The database parent directory does not exist; refusing an ambiguous target.",
    });
  }

  const canonicalParent = yield* fs.realPath(parent);
  const relativeParent = path.relative(canonicalRoot, canonicalParent);
  if (
    relativeParent === ".." ||
    relativeParent.startsWith(`..${path.sep}`) ||
    path.isAbsolute(relativeParent)
  ) {
    return yield* UnsafeStorageTarget.make({
      target,
      message: "The database parent resolves outside this repository.",
    });
  }

  if (yield* fs.exists(target)) {
    const info = yield* fs.stat(target);
    if (info.type !== "File") {
      return yield* UnsafeStorageTarget.make({
        target,
        message: "The database target exists but is not a regular file.",
      });
    }
  }

  return target;
});

const resetStorage = Effect.fn("resetDevelopmentStorage")(function* (options: {
  readonly confirmPrivateDevelopment: boolean;
  readonly database: string;
  readonly dryRun: boolean;
}) {
  if (!options.confirmPrivateDevelopment) {
    return yield* UnsafeStorageTarget.make({
      target: options.database,
      message:
        "Pass --confirm-private-development to acknowledge that this command permanently deletes incompatible private-development data.",
    });
  }

  const fs = yield* FileSystem.FileSystem;
  const target = yield* validateTarget(options.database);
  const files = [target, `${target}-wal`, `${target}-shm`];
  const existing = yield* Effect.filter(files, (file) => fs.exists(file));

  if (existing.length === 0) {
    yield* Console.log(`No development SQLite files exist for ${target}.`);
    return;
  }

  if (options.dryRun) {
    yield* Console.log("Would remove private-development SQLite files:");
    yield* Effect.forEach(existing, (file) => Console.log(`- ${file}`), {
      discard: true,
    });
    return;
  }

  yield* Effect.forEach(existing, (file) => fs.remove(file), {
    discard: true,
  });
  yield* Console.log(`Removed ${existing.length} private-development SQLite file(s).`);
});

const command = CliCommand.make(
  "reset-development-storage",
  {
    confirmPrivateDevelopment,
    database,
    dryRun,
  },
  resetStorage,
).pipe(
  CliCommand.withDescription(
    "Delete an incompatible private-development SQLite database. This is not a migration command.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
