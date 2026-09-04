import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];
type Visitor = ReturnType<NonNullable<Rule["create"]>>;
type Node = Parameters<NonNullable<Visitor[string]>>[0];

const packageDirectory = (filename: string) =>
  /(?:^|\/)packages\/([^/]+)\/src\//.exec(filename.replaceAll("\\", "/"))?.[1];

const literalSource = (node: Node): string | undefined => {
  if (node.type === "Literal" && typeof node.value === "string") return node.value;
  if (node.type === "TemplateLiteral" && node.expressions.length === 0)
    return node.quasis[0]?.value.cooked ?? undefined;

  return undefined;
};

const noInternalBarrel = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      barrel:
        "Keep internal implementations in their owning modules. Import them directly instead of adding an internal re-export-only module.",
    },
  },
  create(context) {
    return {
      Program(program) {
        const importedNames = new Set(
          program.body.flatMap((node) =>
            node.type === "ImportDeclaration"
              ? node.specifiers.map((specifier) => specifier.local.name)
              : [],
          ),
        );

        // Recognize forwarding syntax without tracing aliases through local declarations.
        const isForwarding = (node: Node): boolean => {
          if (
            node.type === "EmptyStatement" ||
            node.type === "ImportDeclaration" ||
            node.type === "ExportAllDeclaration"
          )
            return true;
          if (node.type === "ExportNamedDeclaration") return node.declaration === null;
          if (node.type === "ExportDefaultDeclaration")
            return (
              node.declaration.type === "Identifier" && importedNames.has(node.declaration.name)
            );

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

const publicEntrypoint = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      entrypoint:
        "Public indexes may only re-export named bindings or same-name local module namespaces, without default exports.",
    },
  },
  create(context) {
    return {
      Program(program) {
        for (const node of program.body) {
          if (node.type === "EmptyStatement") continue;
          if (
            node.type === "ExportAllDeclaration" &&
            node.exported?.type === "Identifier" &&
            /^[A-Z][A-Za-z0-9]*$/.test(node.exported.name) &&
            node.source.value === `./${node.exported.name}.ts`
          )
            continue;
          if (
            node.type === "ExportNamedDeclaration" &&
            node.declaration === null &&
            node.source !== null &&
            node.specifiers.length > 0 &&
            node.specifiers.every(
              (specifier) =>
                (specifier.exported.type === "Identifier"
                  ? specifier.exported.name
                  : specifier.exported.value) !== "default",
            )
          )
            continue;

          context.report({ node, messageId: "entrypoint" });
        }
      },
    };
  },
} satisfies Rule;

const noSelfBarrelImport = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      self: "Import this package's implementation files by relative path, without routing through its published name or an index barrel.",
    },
  },
  create(context) {
    const directory = packageDirectory(context.filename);

    if (directory === undefined) return {};

    const ownPackage = directory === "effect-agent" ? "effect-agent" : `@effect-agent/${directory}`;

    const isIndirect = (source: string) =>
      source === ownPackage ||
      source.startsWith(`${ownPackage}/`) ||
      source === "." ||
      source === ".." ||
      /^(?:\.\.?\/)+(?:index(?:\.[cm]?[jt]sx?)?)?$/.test(source) ||
      /^\.\.?\/.*\/index(?:\.[cm]?[jt]sx?)?$/.test(source);

    const check = (node: Node, source: string | undefined) => {
      if (source !== undefined && isIndirect(source)) context.report({ node, messageId: "self" });
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
    "public-entrypoint": publicEntrypoint,
    "no-self-barrel-import": noSelfBarrelImport,
  },
};
