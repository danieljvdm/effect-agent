import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import ts from "typescript-twoslash";

const Dependencies = Schema.Record(Schema.String, Schema.String);

const Manifest = Schema.Struct({
  name: Schema.String,
  private: Schema.optionalKey(Schema.Boolean),
  exports: Dependencies,
  dependencies: Schema.optionalKey(Dependencies),
  optionalDependencies: Schema.optionalKey(Dependencies),
  peerDependencies: Schema.optionalKey(Dependencies),
  devDependencies: Schema.optionalKey(Dependencies),
});

class PackageExportsError extends Schema.TaggedError<PackageExportsError>()("PackageExportsError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const propertyName = (node: ts.PropertyName): string | undefined =>
  ts.isIdentifier(node) || ts.isStringLiteral(node) ? node.text : undefined;

const walk = (node: ts.Node, visit: (node: ts.Node) => void): void => {
  visit(node);
  ts.forEachChild(node, (child) => walk(child, visit));
};

/** Manifests own the public module list; config and source must agree without evaluating code. */
export const verifyPackageExports = Effect.fn("verifyPackageExports")(
  function* (root: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const problems: Array<string> = [];
    const report = (file: string, message: string) => problems.push(`${file}: ${message}`);

    const read = Effect.fn("packageExports.read")(function* (file: string) {
      return yield* fs
        .readFileString(path.join(root, file))
        .pipe(
          Effect.mapError(
            (cause) => new PackageExportsError({ message: `Cannot read ${file}`, cause }),
          ),
        );
    });

    const parse = Effect.fn("packageExports.parse")(function* (file: string) {
      return ts.createSourceFile(
        file,
        yield* read(file),
        ts.ScriptTarget.Latest,
        true,
        file.endsWith(".json")
          ? ts.ScriptKind.JSON
          : file.endsWith(".tsx")
            ? ts.ScriptKind.TSX
            : ts.ScriptKind.TS,
      );
    });

    const packages = yield* Effect.forEach(
      (yield* fs.readDirectory(path.join(root, "packages")))
        .filter((name) => !name.startsWith("."))
        .sort(),
      Effect.fn(function* (directory) {
        const file = `packages/${directory}/package.json`;
        const source = yield* read(file);

        const manifest = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Manifest))(
          source,
        ).pipe(
          Effect.mapError(
            (cause) => new PackageExportsError({ message: `Invalid ${file}`, cause }),
          ),
        );

        // JSON decoding discards duplicate keys, so inspect the original JSON syntax as well.
        walk(
          ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.JSON),
          (node) => {
            if (!ts.isObjectLiteralExpression(node)) return;
            const keys = new Set<string>();

            for (const property of node.properties) {
              if (!ts.isPropertyAssignment(property)) continue;
              const key = propertyName(property.name);

              if (key === undefined) continue;
              if (keys.has(key)) report(file, `Duplicate key ${key}`);
              keys.add(key);
            }
          },
        );

        return { directory, file, manifest };
      }),
    );

    const byName = new Map(packages.map((pkg) => [pkg.manifest.name, pkg]));
    let entries = 0;

    for (const pkg of packages) {
      const base = `packages/${pkg.directory}`;
      const { manifest } = pkg;

      if (!manifest.private) {
        const filenames = new Set(yield* fs.readDirectory(path.join(root, base, "src")));
        const targets = Object.values(manifest.exports);

        for (const filename of filenames) {
          if (filename.endsWith(".ts") && !targets.includes(`./src/${filename}`)) {
            report(
              pkg.file,
              `Unpublished source root module ./src/${filename} must move under internal/`,
            );
          }
        }
        if (manifest.exports["."] !== "./src/index.ts")
          report(pkg.file, "Root must target ./src/index.ts");
        if (new Set(targets).size !== targets.length)
          report(pkg.file, "Export targets must be unique");
        for (const [key, target] of Object.entries(manifest.exports)) {
          entries++;
          if (
            key !== "." &&
            (!/^\.\/[A-Za-z][A-Za-z0-9-]*(?:\/[A-Za-z][A-Za-z0-9-]*)*$/.test(key) ||
              key.split("/").some((part) => part === "internal" || part === "index"))
          ) {
            report(
              pkg.file,
              `${key} must be an explicit public module or group path, excluding internal and index paths`,
            );
          }
          // The release publisher currently maps flat source entries to flat dist artifacts.
          if (!/^\.\/src\/[A-Za-z][A-Za-z0-9_-]*\.ts$/.test(target))
            report(
              pkg.file,
              `${target} must be a flat ./src/Module.ts entry supported by the publisher`,
            );
          if (!filenames.has(target.slice("./src/".length)))
            report(pkg.file, `${target} is missing or has different filesystem casing`);
        }
        const index = yield* parse(`${base}/src/index.ts`);
        const namespaces = new Set<string>();

        for (const statement of index.statements) {
          if (ts.isEmptyStatement(statement)) continue;
          if (
            !ts.isExportDeclaration(statement) ||
            !statement.exportClause ||
            !statement.moduleSpecifier ||
            !ts.isStringLiteral(statement.moduleSpecifier)
          ) {
            report(
              index.fileName,
              "Root must contain only namespace or explicit named re-export declarations",
            );
            continue;
          }
          if (ts.isNamedExports(statement.exportClause)) {
            if (
              statement.exportClause.elements.length === 0 ||
              statement.exportClause.elements.some((element) => element.name.text === "default")
            )
              report(
                index.fileName,
                "Root named re-exports must be nonempty and cannot export a default",
              );
            continue;
          }
          const name = statement.exportClause.name.text;

          if (namespaces.has(name)) report(index.fileName, `Duplicate namespace ${name}`);
          namespaces.add(name);
          if (
            !/^[A-Z][A-Za-z0-9]*$/.test(name) ||
            statement.moduleSpecifier.text !== `./${name}.ts` ||
            !targets.includes(`./src/${name}.ts`)
          ) {
            report(
              index.fileName,
              `${name} must reference the published same-name module ./${name}.ts`,
            );
          }
        }
        const config = yield* parse(`${base}/vite.config.ts`);
        const packed: Array<string> = [];

        walk(config, (node) => {
          if (
            !ts.isPropertyAssignment(node) ||
            propertyName(node.name) !== "pack" ||
            !ts.isObjectLiteralExpression(node.initializer)
          )
            return;
          for (const property of node.initializer.properties) {
            if (!ts.isPropertyAssignment(property) || propertyName(property.name) !== "entry")
              continue;
            if (!ts.isArrayLiteralExpression(property.initializer)) {
              report(config.fileName, "Pack entries must be an explicit array of source paths");
              continue;
            }
            for (const entry of property.initializer.elements) {
              if (ts.isStringLiteral(entry)) packed.push(`./${entry.text.replace(/^\.\//, "")}`);
              else report(config.fileName, "Pack entries must be literal source paths");
            }
          }
        });
        if (new Set(packed).size !== packed.length)
          report(config.fileName, "Pack entries must be unique");
        for (const target of targets)
          if (!packed.includes(target)) report(config.fileName, `Missing pack entry ${target}`);
        for (const target of packed)
          if (!targets.includes(target))
            report(config.fileName, `Unpublished pack entry ${target}`);
      }
      const pending = ["src", "test"];

      while (pending.length > 0) {
        const relative = pending.pop();

        if (relative === undefined) break;
        const directory = path.join(root, base, relative);

        if (!(yield* fs.exists(directory))) continue;
        for (const filename of yield* fs.readDirectory(directory)) {
          const file = `${base}/${relative}/${filename}`;

          if ((yield* fs.stat(path.join(root, file))).type === "Directory") {
            pending.push(`${relative}/${filename}`);
            continue;
          }
          if (!/\.[cm]?tsx?$/.test(filename)) continue;
          const source = yield* parse(file);

          const testOnly =
            /(?:^|\/)(?:test|tests|__tests__|fixtures)(?:\/|$)|\.(?:test|spec)\./.test(file) ||
            Object.entries(manifest.exports).some(
              ([key, target]) =>
                (key === "./testing" || key.startsWith("./testing/")) &&
                target === `./${relative}/${filename}`,
            );

          const dependencies = {
            ...manifest.dependencies,
            ...manifest.optionalDependencies,
            ...manifest.peerDependencies,
          };

          const imports: Array<{ specifier: string; typeOnly: boolean }> = [];

          walk(source, (node) => {
            if (ts.isImportDeclaration(node) && ts.isStringLiteral(node.moduleSpecifier)) {
              const clause = node.importClause;
              const bindings = clause?.namedBindings;

              const typeOnly =
                clause?.isTypeOnly === true ||
                (clause?.name === undefined &&
                  bindings !== undefined &&
                  ts.isNamedImports(bindings) &&
                  bindings.elements.length > 0 &&
                  bindings.elements.every((element) => element.isTypeOnly));

              imports.push({ specifier: node.moduleSpecifier.text, typeOnly });
            }
            if (
              ts.isExportDeclaration(node) &&
              node.moduleSpecifier &&
              ts.isStringLiteral(node.moduleSpecifier)
            ) {
              const typeOnly =
                node.isTypeOnly ||
                (node.exportClause !== undefined &&
                  ts.isNamedExports(node.exportClause) &&
                  node.exportClause.elements.length > 0 &&
                  node.exportClause.elements.every((element) => element.isTypeOnly));

              imports.push({ specifier: node.moduleSpecifier.text, typeOnly });
            }
            if (
              ts.isImportTypeNode(node) &&
              ts.isLiteralTypeNode(node.argument) &&
              ts.isStringLiteral(node.argument.literal)
            )
              imports.push({ specifier: node.argument.literal.text, typeOnly: true });
            if (
              ts.isCallExpression(node) &&
              node.expression.kind === ts.SyntaxKind.ImportKeyword &&
              node.arguments[0] &&
              ts.isStringLiteral(node.arguments[0])
            )
              imports.push({ specifier: node.arguments[0].text, typeOnly: false });
          });
          for (const { specifier, typeOnly } of imports) {
            if (specifier.startsWith(".")) {
              const resolved = path
                .relative(root, path.resolve(root, path.dirname(file), specifier))
                .replaceAll("\\", "/");

              if (resolved.startsWith("packages/") && !resolved.startsWith(`${base}/`))
                report(file, `Import ${specifier} must use the owning package's public module`);
              continue;
            }

            const name = specifier.startsWith("@")
              ? specifier.split("/").slice(0, 2).join("/")
              : specifier.split("/")[0];

            const owner = name === undefined ? undefined : byName.get(name);

            if (!owner) {
              if (
                specifier.startsWith("@effect-agent/") ||
                specifier === "effect-agent" ||
                specifier.startsWith("effect-agent/")
              )
                report(file, `${specifier} references an unknown workspace package`);
              continue;
            }

            const key =
              specifier === owner.manifest.name
                ? "."
                : `.${specifier.slice(owner.manifest.name.length)}`;

            if (owner.manifest.exports[key] === undefined)
              report(file, `${specifier} is not a public export of ${owner.manifest.name}`);
            if (
              owner !== pkg &&
              dependencies[owner.manifest.name] === undefined &&
              (!(testOnly || typeOnly) ||
                manifest.devDependencies?.[owner.manifest.name] === undefined)
            )
              report(
                file,
                `${specifier} needs a declared ${testOnly || typeOnly ? "development or runtime" : "runtime"} dependency`,
              );
          }
        }
      }
    }
    if (problems.length > 0)
      return yield* new PackageExportsError({ message: problems.join("\n") });
    yield* Console.log(
      `Package exports check passed: ${packages.filter((pkg) => !pkg.manifest.private).length} public packages, ${entries} entries.`,
    );
  },
  Effect.mapError((cause) =>
    cause._tag === "PackageExportsError"
      ? cause
      : new PackageExportsError({
          message: `Could not inspect package exports: ${cause.message}`,
          cause,
        }),
  ),
);

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const file = yield* path.fromFileUrl(new URL(import.meta.url));

  yield* verifyPackageExports(path.resolve(path.dirname(file), ".."));
}).pipe(
  Effect.tapError((error) => Console.error(error.message)),
  Effect.provide(NodeServices.layer),
);

if (import.meta.main) NodeRuntime.runMain(program, { disableErrorReporting: true });
