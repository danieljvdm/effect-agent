import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { build as buildWithEsbuild, type Metafile } from "esbuild";

const DependencyMap = Schema.Record(Schema.String, Schema.String);

const PackageManifest = Schema.StructWithRest(
  Schema.Struct({
    name: Schema.NonEmptyString,
    private: Schema.optionalKey(Schema.Boolean),
    exports: Schema.Record(Schema.String, Schema.String),
    dependencies: Schema.optionalKey(DependencyMap),
    optionalDependencies: Schema.optionalKey(DependencyMap),
    peerDependencies: Schema.optionalKey(DependencyMap),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(PackageManifest));

class PackagePurityInfrastructureError extends Schema.TaggedError<PackagePurityInfrastructureError>()(
  "PackagePurityInfrastructureError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Package purity check could not ${this.operation}: ${String(this.cause)}`;
  }
}

class PackagePurityViolation extends Schema.TaggedError<PackagePurityViolation>()(
  "PackagePurityViolation",
  {
    details: Schema.Array(Schema.String),
  },
) {
  override get message() {
    return [
      "Published production entrypoints reached test-only code:",
      ...this.details.map((detail) => `\n${detail}`),
      "\nMove the dependency behind an explicit testing subpath and keep the production entrypoint graph pure.",
    ].join("\n");
  }
}

interface ProductionEntryPoint {
  readonly displayName: string;
  readonly sourcePath: string;
}

interface PurityViolation {
  readonly reason: string;
  readonly target: string;
}

interface EntryPointAudit {
  readonly entryPoint: ProductionEntryPoint;
  readonly bytes: number;
  readonly moduleCount: number;
  readonly metafile: Metafile;
  readonly violations: ReadonlyArray<PurityViolation>;
}

const TEST_ONLY_PACKAGE = "@effect-agent/testing";

const bannedRuntimeDependencies = new Set([TEST_ONLY_PACKAGE, "@effect/vitest", "vitest"]);

const testOnlyExternalReason = (specifier: string): string | undefined => {
  if (specifier === "effect/testing" || specifier.startsWith("effect/testing/")) {
    return "imports Effect's test-only runtime";
  }
  if (specifier === "@effect/vitest" || specifier.startsWith("@effect/vitest/")) {
    return "imports @effect/vitest";
  }
  if (specifier === "vite-plus/test" || specifier === "vitest" || specifier.startsWith("vitest/")) {
    return "imports a test runner";
  }
  if (specifier === TEST_ONLY_PACKAGE || specifier.startsWith(`${TEST_ONLY_PACKAGE}/`)) {
    return "imports @effect-agent/testing";
  }

  return undefined;
};

const testOnlyModuleReason = (sourcePath: string): string | undefined => {
  const normalized = sourcePath.replaceAll("\\", "/");

  if (normalized.includes("/packages/testing/")) {
    return "bundles the @effect-agent/testing package";
  }
  if (/(^|\/)(?:test|tests|testing|__tests__|fixtures?)(?:\/|\.|$)/i.test(normalized)) {
    return "bundles a test or fixture module";
  }
  if (/(^|\/)[^/]*(?:conformance|certification)[^/]*\.[cm]?[jt]sx?$/i.test(normalized)) {
    return "bundles an adapter conformance or certification module";
  }
  if (/\.(?:test|spec)\.[cm]?[jt]sx?$/i.test(normalized)) {
    return "bundles a test module";
  }

  return undefined;
};

const isWorkspaceImport = (specifier: string): boolean =>
  specifier === "effect-agent" ||
  specifier.startsWith("effect-agent/") ||
  specifier.startsWith("@effect-agent/");

const displayNameFor = (packageName: string, exportPath: string): string =>
  exportPath === "." ? packageName : `${packageName}${exportPath.slice(1)}`;

const isTestingExport = (exportPath: string): boolean =>
  exportPath === "./testing" || exportPath.startsWith("./testing/");

const readProductionEntryPoints = Effect.fn("packagePurity.readProductionEntryPoints")(function* (
  repositoryRoot: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const packagesRoot = path.join(repositoryRoot, "packages");

  const packageDirectories = yield* fs.readDirectory(packagesRoot).pipe(
    Effect.mapError(
      (cause) =>
        new PackagePurityInfrastructureError({
          operation: `list ${packagesRoot}`,
          cause,
        }),
    ),
  );

  const entries: Array<ProductionEntryPoint> = [];
  const testingModules = new Set<string>();
  const manifestViolations: Array<string> = [];

  for (const directory of packageDirectories.sort()) {
    const relativeDirectory = `packages/${directory}`;
    const manifestPath = path.join(repositoryRoot, relativeDirectory, "package.json");

    const manifestText = yield* fs.readFileString(manifestPath).pipe(
      Effect.mapError(
        (cause) =>
          new PackagePurityInfrastructureError({
            operation: `read ${relativeDirectory}/package.json`,
            cause,
          }),
      ),
    );

    const manifest = yield* decodeManifest(manifestText).pipe(
      Effect.mapError(
        (cause) =>
          new PackagePurityInfrastructureError({
            operation: `decode ${relativeDirectory}/package.json`,
            cause,
          }),
      ),
    );

    if (manifest.private === true || manifest.name === TEST_ONLY_PACKAGE) continue;

    for (const dependency of [
      ...Object.keys(manifest.dependencies ?? {}),
      ...Object.keys(manifest.optionalDependencies ?? {}),
      ...Object.keys(manifest.peerDependencies ?? {}),
    ]) {
      if (bannedRuntimeDependencies.has(dependency)) {
        manifestViolations.push(
          `${manifest.name} declares test-only runtime dependency ${dependency} in ${relativeDirectory}/package.json`,
        );
      }
    }

    for (const [exportPath, sourcePath] of Object.entries(manifest.exports).sort(
      ([left], [right]) => left.localeCompare(right),
    )) {
      if (isTestingExport(exportPath)) {
        testingModules.add(path.join(relativeDirectory, sourcePath).replaceAll("\\", "/"));
        continue;
      }
      entries.push({
        displayName: displayNameFor(manifest.name, exportPath),
        sourcePath: path.join(relativeDirectory, sourcePath),
      });
    }
  }

  if (manifestViolations.length > 0) {
    return yield* new PackagePurityViolation({ details: manifestViolations });
  }

  return { entries, testingModules };
});

const auditEntryPoint = Effect.fn("packagePurity.auditEntryPoint")(function* (
  repositoryRoot: string,
  entryPoint: ProductionEntryPoint,
  testingModules: ReadonlySet<string>,
) {
  const result = yield* Effect.tryPromise({
    try: () =>
      buildWithEsbuild({
        absWorkingDir: repositoryRoot,
        bundle: true,
        entryPoints: [entryPoint.sourcePath],
        format: "esm",
        logLevel: "silent",
        metafile: true,
        platform: "neutral",
        plugins: [
          {
            name: "externalize-non-workspace-packages",
            setup(build) {
              build.onResolve({ filter: /^[^./]/ }, (args) =>
                isWorkspaceImport(args.path) ? undefined : { external: true, path: args.path },
              );
            },
          },
        ],
        sourcemap: false,
        treeShaking: false,
        write: false,
      }),
    catch: (cause) =>
      new PackagePurityInfrastructureError({
        operation: `bundle ${entryPoint.displayName} from ${entryPoint.sourcePath}`,
        cause,
      }),
  });

  if (result.metafile === undefined) {
    return yield* new PackagePurityInfrastructureError({
      operation: `inspect ${entryPoint.displayName}'s bundle metadata`,
      cause: "esbuild returned no metafile",
    });
  }

  const violations: Array<PurityViolation> = [];

  for (const [sourcePath, input] of Object.entries(result.metafile.inputs)) {
    const moduleReason = testingModules.has(sourcePath.replaceAll("\\", "/"))
      ? "bundles a declared testing entry point"
      : testOnlyModuleReason(sourcePath);

    if (moduleReason !== undefined) violations.push({ reason: moduleReason, target: sourcePath });
    for (const imported of input.imports) {
      if (!imported.external) continue;
      const externalReason = testOnlyExternalReason(imported.path);

      if (externalReason !== undefined) {
        violations.push({ reason: externalReason, target: `external:${imported.path}` });
      }
    }
  }

  return {
    entryPoint,
    bytes: (result.outputFiles ?? []).reduce(
      (total, output) => total + output.contents.byteLength,
      0,
    ),
    moduleCount: Object.keys(result.metafile.inputs).length,
    metafile: result.metafile,
    violations,
  } satisfies EntryPointAudit;
});

const dependencyPath = (audit: EntryPointAudit, target: string): ReadonlyArray<string> => {
  const entryInput = Object.values(audit.metafile.outputs).find(
    (output) => output.entryPoint !== undefined,
  )?.entryPoint;

  if (entryInput === undefined)
    return [audit.entryPoint.sourcePath, target.replace("external:", "")];

  const pending = [entryInput];
  const parent = new Map<string, string | undefined>([[entryInput, undefined]]);

  while (pending.length > 0) {
    const current = pending.shift();

    if (current === undefined || current === target) break;
    const input = audit.metafile.inputs[current];

    if (input === undefined) continue;
    for (const imported of input.imports) {
      const next = imported.external ? `external:${imported.path}` : imported.path;

      if (parent.has(next)) continue;
      parent.set(next, current);
      pending.push(next);
    }
  }

  if (!parent.has(target)) return [entryInput, target.replace("external:", "")];
  const path: Array<string> = [];
  let current: string | undefined = target;

  while (current !== undefined) {
    path.push(current.replace("external:", ""));
    current = parent.get(current);
  }

  return path.reverse();
};

const formatViolation = (audit: EntryPointAudit, violation: PurityViolation): string => {
  const path = dependencyPath(audit, violation.target);

  return [
    `${audit.entryPoint.displayName} (${audit.entryPoint.sourcePath}) ${violation.reason}.`,
    "Dependency path:",
    ...path.map((part, index) => `  ${index === 0 ? "" : "→ "}${part}`),
  ].join("\n");
};

export const verifyPackagePurity = Effect.fn("verifyPackagePurity")(function* (
  repositoryRoot: string,
) {
  const { entries, testingModules } = yield* readProductionEntryPoints(repositoryRoot);

  const audits = yield* Effect.forEach(
    entries,
    (entryPoint) => auditEntryPoint(repositoryRoot, entryPoint, testingModules),
    { concurrency: 4 },
  );

  const violationDetails = audits.flatMap((audit) =>
    audit.violations.map((violation) => formatViolation(audit, violation)),
  );

  if (violationDetails.length > 0) {
    return yield* new PackagePurityViolation({ details: violationDetails });
  }

  const moduleCount = audits.reduce((total, audit) => total + audit.moduleCount, 0);
  const bytes = audits.reduce((total, audit) => total + audit.bytes, 0);

  yield* Console.log(
    `Package purity check passed: ${audits.length} production entrypoints, ${moduleCount} bundled modules, ${bytes} bytes; no test-only dependency path found.`,
  );
});

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));

  yield* verifyPackagePurity(path.resolve(path.dirname(scriptPath), ".."));
}).pipe(
  Effect.tapError((error) => Console.error(error.message)),
  Effect.provide(NodeServices.layer),
);

if (import.meta.main) NodeRuntime.runMain(program, { disableErrorReporting: true });
