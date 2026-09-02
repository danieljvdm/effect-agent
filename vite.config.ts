import type { OxfmtConfig } from "oxfmt";
import type { OxlintConfig } from "oxlint";
import { defineConfig } from "vite-plus";

// Tool-owned paths that should not be linted or formatted. Skill copies remain
// tracked project inputs, while symlinked harness targets and generated Git
// hook internals may duplicate or contain third-party source. Keep these
// exclusions in tool configuration rather than `.gitignore`.
const toolIgnorePatterns = [".agents/**", ".claude/**", ".opencode/**", ".vite-hooks/_/**"];

// Canonical formatting defaults for this project. Oxfmt does not support
// config inheritance, so this object is spread into the `fmt` block below.
const recommendedOxfmtConfig = {
  arrowParens: "always",
  endOfLine: "lf",
  ignorePatterns: toolIgnorePatterns,
  printWidth: 100,
  semi: true,
  singleQuote: false,
  sortImports: true,
  sortPackageJson: true,
  tabWidth: 2,
  trailingComma: "all",
  useTabs: false,
} satisfies OxfmtConfig;

// High-signal Oxlint defaults for this project, composed through `lint.extends`
// below so project-local plugins, rules, and overrides layer on top without
// losing this nested configuration.
const recommendedOxlintConfig = {
  ignorePatterns: toolIgnorePatterns,
  options: {
    typeAware: true,
  },
  jsPlugins: [
    // Oxlint's `extends` composition requires a package name or absolute path
    // for a JS plugin specifier — a relative path is only accepted at the
    // top-level `jsPlugins`, so these are resolved against this file.
    { name: "stylistic", specifier: new URL("./oxlint/plugin-style.js", import.meta.url).pathname },
  ],
  plugins: ["import", "react", "vitest"],
  rules: {
    eqeqeq: "error",
    "import/default": "off",
    "import/namespace": "off",
    "import/no-cycle": "error",
    "import/no-duplicates": ["error", { preferInline: true }],
    "import/no-self-import": "error",
    "react/exhaustive-deps": "error",
    "react/rules-of-hooks": "error",
    // Keep the severity with its options: a severity-only override discards this JS rule's options.
    "stylistic/padding-line-between-statements": [
      "error",
      { blankLine: "always", prev: ["const", "let", "var", "multiline-export"], next: "*" },
      {
        blankLine: "always",
        prev: "*",
        next: ["multiline-const", "multiline-let", "multiline-var", "multiline-export"],
      },
      {
        blankLine: "any",
        prev: ["singleline-const", "singleline-let", "singleline-var"],
        next: ["singleline-const", "singleline-let", "singleline-var"],
      },
      { blankLine: "always", prev: "*", next: "return" },
    ],
    "typescript/consistent-type-imports": [
      "error",
      { fixStyle: "inline-type-imports", prefer: "type-imports" },
    ],
    "typescript/no-floating-promises": "off",
    "typescript/no-explicit-any": "error",
    "typescript/no-misused-spread": "off",
    "typescript/no-non-null-assertion": "error",
    "typescript/require-array-sort-compare": "off",
    "typescript/restrict-template-expressions": "off",
    "typescript/switch-exhaustiveness-check": "error",
    "unicorn/prefer-node-protocol": "error",
    "vitest/no-focused-tests": "error",
    "vitest/no-identical-title": "error",
    "vitest/no-standalone-expect": "off",
    "vitest/valid-expect": "error",
  },
  overrides: [
    {
      files: ["**/*.test.{ts,tsx}", "**/*.spec.{ts,tsx}"],
      rules: {
        "typescript/no-non-null-assertion": "off",
      },
    },
  ],
} satisfies OxlintConfig;

const generatedPaths = [
  ".agents/**",
  ".claude/**",
  // Local and CI output from scripts/build-action.ts.
  "action/dist/**",
  "examples/demo/src/routeTree.gen.ts",
];

export default defineConfig({
  staged: {
    "*.{js,cjs,mjs,ts,tsx}": "vp check --fix",
  },
  fmt: {
    ...recommendedOxfmtConfig,
    ignorePatterns: generatedPaths,
  },
  lint: {
    extends: [recommendedOxlintConfig],
    ignorePatterns: generatedPaths,
    options: {
      typeAware: true,
      typeCheck: true,
    },
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: {
      "import/no-duplicates": "warn",
      "react-hooks/exhaustive-deps": "warn",
      "typescript/consistent-type-imports": "warn",
      "typescript/no-non-null-assertion": "warn",
      "typescript/switch-exhaustiveness-check": "warn",
      "vitest/expect-expect": "warn",
      "vitest/no-conditional-expect": "warn",
      "vitest/require-to-throw-message": "warn",
      "vitest/valid-expect": "warn",
      "vitest/valid-title": "warn",
      "vite-plus/prefer-vite-plus-imports": "error",
    },
  },
  pack: {
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
  test: {
    // Vite Task caches successful suites. Vitest's mutable results.json
    // otherwise becomes a task input and prevents reuse on fresh runners.
    cache: false,
    silent: "passed-only",
  },
  run: {
    cache: {
      scripts: true,
    },
    tasks: {
      "pr-review-eval": {
        cache: false,
        command: "bun --cwd examples/pr-review-eval src/main.ts",
      },
      "semantic-memory-eval": {
        cache: false,
        command: "node --experimental-transform-types examples/semantic-memory-eval/src/main.ts",
      },
    },
  },
});
