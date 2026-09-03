import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vite-plus/test";

import plugin from "../../../oxlint/plugin-exports.ts";

RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester();

tester.run("no-internal-barrel", plugin.rules["no-internal-barrel"], {
  valid: [
    "export const own = 1;",
    'import { input } from "./input.ts"; export const own = input + 1;',
    'export { helper } from "./helper.ts"; export function own() {}',
    'import "./setup.ts"; export {};',
  ],
  invalid: [
    { code: 'export * from "./module.ts";', errors: [{ messageId: "barrel" }] },
    { code: 'export * as Module from "./module.ts";', errors: [{ messageId: "barrel" }] },
    { code: 'export { value } from "./module.ts";', errors: [{ messageId: "barrel" }] },
    {
      code: 'import { value as local } from "./module.ts"; export { local as value };',
      errors: [{ messageId: "barrel" }],
    },
    {
      filename: "forward.ts",
      code: 'export type { Value } from "./module.ts";',
      errors: [{ messageId: "barrel" }],
    },
  ],
});

tester.run("no-package-reexports", plugin.rules["no-package-reexports"], {
  valid: [
    'import { Agent } from "@effect-agent/core"; export const make = () => Agent.make;',
    'export { own } from "./own.ts";',
    'export { Option } from "effect";',
  ],
  invalid: [
    { code: 'export * from "@effect-agent/core";', errors: [{ messageId: "ownership" }] },
    { code: 'export { Memory } from "@effect-agent/core";', errors: [{ messageId: "ownership" }] },
    {
      code: 'import { Memory as local } from "@effect-agent/core"; export { local as Memory };',
      errors: [{ messageId: "ownership" }],
    },
    {
      filename: "forward.ts",
      code: 'import type { RunOptions } from "@effect-agent/engine"; export type { RunOptions };',
      errors: [{ messageId: "ownership" }],
    },
    { code: 'export * as Framework from "effect-agent";', errors: [{ messageId: "ownership" }] },
  ],
});

const filename = "/repo/packages/engine/src/module.ts";

tester.run("no-entrypoint-implementation", plugin.rules["no-entrypoint-implementation"], {
  valid: ['export * from "./module.ts";', 'import { own } from "./own.ts"; export { own };'],
  invalid: [
    { code: "export const own = 1;", errors: [{ messageId: "implementation" }] },
    { code: "export function own() {}", errors: [{ messageId: "implementation" }] },
    {
      code: 'export * from "./module.ts"; initialize();',
      errors: [{ messageId: "implementation" }],
    },
  ],
});

tester.run("no-self-barrel-import", plugin.rules["no-self-barrel-import"], {
  valid: [
    { filename, code: 'import { own } from "./own.ts";' },
    { filename, code: 'import { Agent } from "@effect-agent/core";' },
    { filename, code: 'import * as Effect from "effect/Effect";' },
  ],
  invalid: [
    ...[
      ".",
      "..",
      "./index.ts",
      "../index.js",
      "./nested/index",
      "@effect-agent/engine",
      "@effect-agent/engine/history",
    ].map((source) => ({
      filename,
      code: `import { value } from "${source}";`,
      errors: [{ messageId: "direct" }],
    })),
    { filename, code: 'export * from "./index.ts";', errors: [{ messageId: "direct" }] },
    { filename, code: 'export { value } from "./index.ts";', errors: [{ messageId: "direct" }] },
    {
      filename,
      code: 'const load = () => import("./index.ts");',
      errors: [{ messageId: "direct" }],
    },
    {
      filename,
      code: 'type Value = import("./index.ts").Value;',
      errors: [{ messageId: "direct" }],
    },
    {
      filename: "/repo/packages/effect-agent/src/module.ts",
      code: 'import { Agent } from "effect-agent";',
      errors: [{ messageId: "direct" }],
    },
  ],
});
