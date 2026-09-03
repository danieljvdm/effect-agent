import type { RuleTester } from "oxlint/plugins-dev";

type Rule = Parameters<RuleTester["run"]>[1];

const isFrameworkPackage = (source: string) =>
  source === "effect-agent" ||
  source.startsWith("effect-agent/") ||
  source.startsWith("@effect-agent/");

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
        const statements = program.body.filter((node) => node.type !== "EmptyStatement");

        const hasExports = statements.some(
          (node) =>
            node.type === "ExportAllDeclaration" ||
            (node.type === "ExportNamedDeclaration" && node.specifiers.length > 0),
        );

        if (
          hasExports &&
          statements.every(
            (node) =>
              node.type === "ImportDeclaration" ||
              node.type === "ExportAllDeclaration" ||
              (node.type === "ExportNamedDeclaration" && node.declaration === null),
          )
        ) {
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
        "Import this API from {{source}}. Only the umbrella should re-export another constituent package's bindings.",
    },
  },
  create(context) {
    return {
      Program(program) {
        const imports = new Map<string, string>();

        for (const node of program.body) {
          if (node.type !== "ImportDeclaration" || !isFrameworkPackage(node.source.value)) continue;
          for (const specifier of node.specifiers) {
            imports.set(specifier.local.name, node.source.value);
          }
        }

        for (const node of program.body) {
          if (node.type !== "ExportAllDeclaration" && node.type !== "ExportNamedDeclaration")
            continue;
          if (node.source !== null) {
            if (isFrameworkPackage(node.source.value)) {
              context.report({ node, messageId: "ownership", data: { source: node.source.value } });
            }
          } else if (node.type === "ExportNamedDeclaration") {
            for (const specifier of node.specifiers) {
              const name =
                specifier.local.type === "Identifier"
                  ? specifier.local.name
                  : specifier.local.value;

              const source = imports.get(name);

              if (source !== undefined) {
                context.report({ node: specifier, messageId: "ownership", data: { source } });
              }
            }
          }
        }
      },
    };
  },
} satisfies Rule;

const noEntrypointImplementation = {
  meta: {
    type: "problem",
    schema: [],
    messages: {
      implementation:
        "Keep package index.ts files as export lists. Define this API in its owning module and re-export it here.",
    },
  },
  create(context) {
    return {
      Program(program) {
        const implementation = program.body.find(
          (node) =>
            node.type !== "EmptyStatement" &&
            node.type !== "ImportDeclaration" &&
            node.type !== "ExportAllDeclaration" &&
            !(node.type === "ExportNamedDeclaration" && node.declaration === null),
        );

        if (implementation !== undefined) {
          context.report({ node: implementation, messageId: "implementation" });
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
      direct:
        "Import a sibling module directly instead of routing package implementation through a barrel.",
    },
  },
  create(context) {
    const packageDirectory = /(?:^|\/)packages\/([^/]+)\/src\//.exec(
      context.filename.replaceAll("\\", "/"),
    )?.[1];

    const packageName =
      packageDirectory === "effect-agent" ? "effect-agent" : `@effect-agent/${packageDirectory}`;

    const isBarrel = (source: string) =>
      source === packageName ||
      source.startsWith(`${packageName}/`) ||
      source === "." ||
      source === ".." ||
      /^(?:\.\.?\/)+(?:index(?:\.[cm]?[jt]sx?)?)?$/.test(source) ||
      /^\.\.?\/.*\/index(?:\.[cm]?[jt]sx?)?$/.test(source);

    return {
      ImportDeclaration(node) {
        if (isBarrel(node.source.value)) context.report({ node, messageId: "direct" });
      },
      TSImportType(node) {
        if (isBarrel(node.source.value)) context.report({ node, messageId: "direct" });
      },
      ImportExpression(node) {
        if (
          node.source.type === "Literal" &&
          typeof node.source.value === "string" &&
          isBarrel(node.source.value)
        ) {
          context.report({ node, messageId: "direct" });
        }
      },
      ExportAllDeclaration(node) {
        if (isBarrel(node.source.value)) context.report({ node, messageId: "direct" });
      },
      ExportNamedDeclaration(node) {
        if (node.source !== null && isBarrel(node.source.value))
          context.report({ node, messageId: "direct" });
      },
    };
  },
} satisfies Rule;

export default {
  meta: { name: "effect-agent-exports" },
  rules: {
    "no-internal-barrel": noInternalBarrel,
    "no-entrypoint-implementation": noEntrypointImplementation,
    "no-package-reexports": noPackageReexports,
    "no-self-barrel-import": noSelfBarrelImport,
  },
};
