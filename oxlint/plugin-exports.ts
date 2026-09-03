import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];
type Visitor = ReturnType<NonNullable<Rule["create"]>>;
type Node = Parameters<NonNullable<Visitor[string]>>[0];
type Program = Extract<Node, { type: "Program" }>;

const frameworkPackage = (source: string) =>
  /^(effect-agent|@effect-agent\/[^/]+)(?:\/(.*))?$/.exec(source);

const packageDirectory = (filename: string) =>
  /(?:^|\/)packages\/([^/]+)\/src\//.exec(filename.replaceAll("\\", "/"))?.[1];

const isPublicModule = (subpath: string) => /^(?:testing\/)?[A-Z][A-Za-z0-9]*$/.test(subpath);

const literalSource = (node: Node): string | undefined => {
  if (node.type === "Literal" && typeof node.value === "string") return node.value;
  if (node.type === "TemplateLiteral" && node.expressions.length === 0)
    return node.quasis[0]?.value.cooked ?? undefined;

  return undefined;
};

// Track direct aliases as forwarding too: changing export syntax does not change API ownership.
const forwardedSource = (program: Program) => {
  const imports = new Map<string, string>();
  const aliases = new Map<string, Node>();

  const rememberBinding = (binding: Node, value: Node) => {
    if (binding.type === "Identifier") aliases.set(binding.name, value);
    else if (binding.type === "ObjectPattern") {
      for (const property of binding.properties) {
        rememberBinding(
          property.type === "RestElement" ? property.argument : property.value,
          value,
        );
      }
    } else if (binding.type === "ArrayPattern") {
      for (const element of binding.elements) {
        if (element !== null) rememberBinding(element, value);
      }
    } else if (binding.type === "RestElement") rememberBinding(binding.argument, value);
  };

  for (const statement of program.body) {
    const node = statement.type === "ExportNamedDeclaration" ? statement.declaration : statement;

    if (node?.type === "ImportDeclaration") {
      for (const specifier of node.specifiers) {
        imports.set(specifier.local.name, node.source.value);
      }
    } else if (node?.type === "VariableDeclaration") {
      for (const declaration of node.declarations) {
        if (declaration.init !== null) rememberBinding(declaration.id, declaration.init);
      }
    } else if (node?.type === "TSTypeAliasDeclaration") {
      aliases.set(node.id.name, node.typeAnnotation);
    }
  }

  const sourceOf = (node: Node | null, seen = new Set<string>()): string | undefined => {
    if (node === null) return undefined;
    if (node.type === "Identifier") {
      const source = imports.get(node.name);

      if (source !== undefined) return source;
      if (seen.has(node.name)) return undefined;
      seen.add(node.name);

      return sourceOf(aliases.get(node.name) ?? null, seen);
    }

    if (node.type === "TSImportType")
      return node.typeArguments === null ? node.source.value : undefined;
    if (node.type === "ImportExpression") return literalSource(node.source);
    if (node.type === "MemberExpression") return sourceOf(node.object, seen);
    if (node.type === "TSQualifiedName") return sourceOf(node.left, seen);
    if (node.type === "TSTypeReference")
      return node.typeArguments === null ? sourceOf(node.typeName, seen) : undefined;
    if (node.type === "TSTypeQuery")
      return node.typeArguments === null ? sourceOf(node.exprName, seen) : undefined;
    if (node.type === "TSIndexedAccessType") return sourceOf(node.objectType, seen);
    if (node.type === "TSParenthesizedType") return sourceOf(node.typeAnnotation, seen);
    if (node.type === "AwaitExpression") return sourceOf(node.argument, seen);
    if (
      node.type === "ParenthesizedExpression" ||
      node.type === "ChainExpression" ||
      node.type === "TSAsExpression" ||
      node.type === "TSTypeAssertion" ||
      node.type === "TSSatisfiesExpression" ||
      node.type === "TSNonNullExpression" ||
      node.type === "TSInstantiationExpression"
    )
      return sourceOf(node.expression, seen);

    return undefined;
  };

  return sourceOf;
};

const noInternalBarrel = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      barrel:
        "Keep re-export-only modules at approved public entry points. Import the owning module directly or move these exports into the public entry point.",
    },
  },
  create(context) {
    return {
      Program(program) {
        const sourceOf = forwardedSource(program);

        const isForwarding = (node: Node): boolean => {
          if (
            node.type === "EmptyStatement" ||
            node.type === "ImportDeclaration" ||
            node.type === "ExportAllDeclaration"
          )
            return true;
          if (node.type === "ExportNamedDeclaration")
            return node.declaration === null || isForwarding(node.declaration);
          if (node.type === "ExportDefaultDeclaration")
            return sourceOf(node.declaration) !== undefined;
          if (node.type === "VariableDeclaration")
            return node.declarations.every(
              (declaration) => sourceOf(declaration.init) !== undefined,
            );
          if (node.type === "TSTypeAliasDeclaration")
            return sourceOf(node.typeAnnotation) !== undefined;

          return (
            node.type === "ExpressionStatement" &&
            node.directive !== undefined &&
            node.directive !== null
          );
        };

        const hasExports = program.body.some(
          (node) =>
            node.type === "ExportAllDeclaration" ||
            node.type === "ExportDefaultDeclaration" ||
            (node.type === "ExportNamedDeclaration" &&
              (node.specifiers.length > 0 || node.declaration !== null)),
        );

        if (hasExports && program.body.every(isForwarding)) {
          context.report({ node: program, messageId: "barrel" });
        }
      },
    };
  },
} satisfies Rule;

const noPackageReexports = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      ownership:
        "Import this API from {{source}}. Only the umbrella may forward another framework package's bindings.",
    },
  },
  create(context) {
    return {
      Program(program) {
        const sourceOf = forwardedSource(program);

        const check = (node: Node, source = sourceOf(node)) => {
          if (source !== undefined && frameworkPackage(source) !== null) {
            context.report({ node, messageId: "ownership", data: { source } });
          }
        };

        for (const node of program.body) {
          if (node.type === "ExportDefaultDeclaration") check(node.declaration);
          if (node.type !== "ExportAllDeclaration" && node.type !== "ExportNamedDeclaration")
            continue;
          if (node.source !== null) check(node, node.source.value);
          else if (node.type === "ExportNamedDeclaration") {
            for (const specifier of node.specifiers) check(specifier.local);
            if (node.declaration?.type === "VariableDeclaration") {
              for (const declaration of node.declaration.declarations) {
                check(declaration, sourceOf(declaration.init));
              }
            } else if (node.declaration?.type === "TSTypeAliasDeclaration") {
              check(node.declaration, sourceOf(node.declaration.typeAnnotation));
            }
          }
        }
      },
    };
  },
} satisfies Rule;

const namespaceOnlyEntrypoint = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      namespace:
        "Package roots may only export same-name local module namespaces: export * as Module from './Module.ts'.",
    },
  },
  create(context) {
    return {
      Program(program) {
        for (const node of program.body) {
          if (node.type === "EmptyStatement") continue;
          if (
            node.type === "ExportAllDeclaration" &&
            node.exportKind !== "type" &&
            node.exported?.type === "Identifier" &&
            /^[A-Z][A-Za-z0-9]*$/.test(node.exported.name) &&
            node.source.value === `./${node.exported.name}.ts`
          )
            continue;

          context.report({ node, messageId: "namespace" });
        }
      },
    };
  },
} satisfies Rule;

const canonicalUmbrellaModule = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      canonical:
        "An umbrella module must contain only export * from the same-name core, engine, or capabilities public module.",
    },
  },
  create(context) {
    const moduleName = /(?:^|\/)([A-Z][A-Za-z0-9]*)\.ts$/.exec(
      context.filename.replaceAll("\\", "/"),
    )?.[1];

    return {
      Program(program) {
        const statements = program.body.filter((node) => node.type !== "EmptyStatement");
        const node = statements[0];

        if (
          moduleName !== undefined &&
          statements.length === 1 &&
          node?.type === "ExportAllDeclaration" &&
          node.exported === null &&
          node.exportKind !== "type" &&
          ["core", "engine", "capabilities"].some(
            (owner) => node.source.value === `@effect-agent/${owner}/${moduleName}`,
          )
        )
          return;

        context.report({ node: program, messageId: "canonical" });
      },
    };
  },
} satisfies Rule;

const noWildcardReexports = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      wildcard:
        "Select public exports explicitly. Bare export * can expose private helpers; only the umbrella's same-name module bridges may forward a whole module.",
    },
  },
  create(context) {
    return {
      ExportAllDeclaration(node) {
        if (node.exported === null) context.report({ node, messageId: "wildcard" });
      },
    };
  },
} satisfies Rule;

const requireDirectModuleImport = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      direct:
        "Import a direct public Module subpath from another framework package, or a relative implementation file from this package. Package roots, aggregate subpaths, and index barrels are not implementation dependencies.",
    },
  },
  create(context) {
    const directory = packageDirectory(context.filename);

    if (directory === undefined) return {};

    const ownPackage = directory === "effect-agent" ? "effect-agent" : `@effect-agent/${directory}`;

    const isIndirect = (source: string) => {
      const framework = frameworkPackage(source);

      if (framework !== null) {
        return framework[1] === ownPackage || !isPublicModule(framework[2] ?? "");
      }

      return (
        source === "." ||
        source === ".." ||
        /^(?:\.\.?\/)+(?:index(?:\.[cm]?[jt]sx?)?)?$/.test(source) ||
        /^\.\.?\/.*\/index(?:\.[cm]?[jt]sx?)?$/.test(source)
      );
    };

    const check = (node: Node, source: string | undefined) => {
      if (source !== undefined && isIndirect(source)) context.report({ node, messageId: "direct" });
    };

    return {
      ImportDeclaration(node) {
        check(node, node.source.value);
      },
      TSImportType(node) {
        check(node, node.source.value);
      },
      ImportExpression(node) {
        check(node, literalSource(node.source));
      },
      ExportAllDeclaration(node) {
        check(node, node.source.value);
      },
      ExportNamedDeclaration(node) {
        if (node.source !== null) check(node, node.source.value);
      },
      TSExternalModuleReference(node) {
        check(node, literalSource(node.expression));
      },
    };
  },
} satisfies Rule;

export default {
  meta: { name: "effect-agent-exports" },
  rules: {
    "no-internal-barrel": noInternalBarrel,
    "no-package-reexports": noPackageReexports,
    "no-wildcard-reexports": noWildcardReexports,
    "namespace-only-entrypoint": namespaceOnlyEntrypoint,
    "canonical-umbrella-module": canonicalUmbrellaModule,
    "require-direct-module-import": requireDirectModuleImport,
  },
};
