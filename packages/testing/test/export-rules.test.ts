import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vite-plus/test";

import plugin from "../../../oxlint/plugin-exports.ts";

RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester();
const filename = "/repo/packages/engine/src/Module.ts";
const root = "/repo/packages/core/src/index.ts";
const umbrella = "/repo/packages/effect-agent/src/index.ts";

tester.run("no-internal-barrel", plugin.rules["no-internal-barrel"], {
  valid: [
    "export const own = 1;",
    'import { input } from "./Input.ts"; export const own = input + 1;',
    'export { helper } from "./Helper.ts"; export function own() {}',
    'export { helper } from "./Helper.ts"; initialize();',
    'import "./Setup.ts"; export {};',
    'import { Value } from "./Value.ts"; export type Own = { value: Value };',
    'import type { Value } from "./Value.ts"; export type Own<T> = Value<{ own: T }>;',
    'import { value } from "./Value.ts"; export default () => value;',
    "const first = second; const second = first; export { first };",
  ].map((code) => ({ filename, code })),
  invalid: [
    'export * from "./Module.ts";',
    'export * as Module from "./Module.ts";',
    'export { value } from "./Module.ts";',
    'import { value as local } from "./Module.ts"; export { local as value };',
    'import * as Module from "./Module.ts"; export default Module;',
    'import { value } from "./Module.ts"; const alias = value; export { alias };',
    'import * as Module from "./Module.ts"; export const value = Module.value;',
    'import type { Value } from "./Module.ts"; export type Alias = Value;',
    'export type Value = import("./Module.ts").Value;',
    '"use strict"; export * from "./Module.ts";',
  ].map((code) => ({ filename, code, errors: [{ messageId: "barrel" }] })),
});

tester.run("no-package-reexports", plugin.rules["no-package-reexports"], {
  valid: [
    'import { make } from "@effect-agent/core/Agent"; export const own = () => make();',
    'import type { Agent } from "@effect-agent/core/Agent"; export type Own = { agent: Agent };',
    'import type { Failure } from "@effect-agent/engine/AgentRuntime"; export type Own<R> = Failure<{ runtime: R }>;',
    'export type Own<R> = import("@effect-agent/engine/AgentRuntime").Failure<{ runtime: R }>;',
    'export { own } from "./Own.ts";',
    'export { Option } from "effect";',
    'import { value } from "@another/package"; export { value };',
    'import { Agent } from "@effect-agent/core/Agent"; export default function own() { return Agent; }',
  ].map((code) => ({ filename, code })),
  invalid: [
    ...[
      'export * from "@effect-agent/core";',
      'export { Agent } from "@effect-agent/core/Agent";',
      'export type { Agent } from "@effect-agent/core/Agent";',
      'export * as Agent from "@effect-agent/core/Agent";',
      'export * as Framework from "effect-agent";',
      'import { Agent as local } from "@effect-agent/core/Agent"; export { local as Agent };',
      'import type { RunOptions } from "@effect-agent/engine/AgentRuntime"; export type { RunOptions };',
      'import * as Agent from "@effect-agent/core/Agent"; export default Agent;',
      'import * as Agent from "@effect-agent/core/Agent"; export const make = Agent.make;',
      'import * as Agent from "@effect-agent/core/Agent"; const { make } = Agent; export { make };',
      'import { Agent } from "@effect-agent/core/Agent"; const local = Agent; export { local };',
      'import type { Agent } from "@effect-agent/core/Agent"; export type Alias = Agent;',
      'import * as Agent from "@effect-agent/core/Agent"; export type Alias = Agent.Agent;',
      'export type Agent = import("@effect-agent/core/Agent").Agent;',
      'export const Agent = await import("@effect-agent/core/Agent");',
      'import * as Agent from "@effect-agent/core/Agent"; export const make = Agent.make satisfies Function;',
    ].map((code) => ({ filename, code, errors: [{ messageId: "ownership" }] })),
    {
      filename: "/repo/packages/storage-memory/src/Storage.ts",
      code: 'export { Thread } from "@effect-agent/thread/Thread";',
      errors: [{ messageId: "ownership" }],
    },
  ],
});

tester.run("namespace-only-entrypoint", plugin.rules["namespace-only-entrypoint"], {
  valid: [
    { filename: root, code: 'export * as Agent from "./Agent.ts";' },
    {
      filename: root,
      code: '// Public modules\nexport * as Agent from "./Agent.ts"; export * as HTTP2 from "./HTTP2.ts";',
    },
    { filename: umbrella, code: 'export * as Agent from "./Agent.ts";' },
    {
      filename: umbrella,
      code: 'export * as AgentRuntime from "./AgentRuntime.ts";',
    },
    {
      filename: "C:\\repo\\packages\\effect-agent\\src\\index.ts",
      code: 'export * as Agent from "./Agent.ts";',
    },
  ],
  invalid: [
    ...[
      'export * from "./Agent.ts";',
      'export { Agent } from "./Agent.ts";',
      'export type { Agent } from "./Agent.ts";',
      'export * as agent from "./agent.ts";',
      'export * as Agent_Model from "./Agent_Model.ts";',
      'export * as Alias from "./Agent.ts";',
      'export * as Agent from "./agent.ts";',
      'export * as Agent from "./internal/Agent.ts";',
      'export * as Agent from "../Agent.ts";',
      'export * as Agent from "./Agent/index.ts";',
      'export * as Agent from "@effect-agent/core/Agent";',
      'export type * as Agent from "./Agent.ts";',
      "export const Agent = 1;",
      "export function make() {}",
      "export interface Agent {}",
      "export default {};",
      "initialize();",
      'import "./Setup.ts";',
    ].map((code) => ({ filename: root, code, errors: [{ messageId: "namespace" }] })),
    {
      filename: root,
      code: 'import * as Agent from "./Agent.ts"; export { Agent };',
      errors: [{ messageId: "namespace" }, { messageId: "namespace" }],
    },
    {
      filename: root,
      code: 'export * as Agent from "./Agent.ts"; initialize();',
      errors: [{ messageId: "namespace" }],
    },
    ...[
      'export * as Agent from "@effect-agent/core/Agent";',
      'export * as AgentRuntime from "@effect-agent/engine/AgentRuntime";',
      'export * as Core from "@effect-agent/core";',
      'export * as Alias from "@effect-agent/core/Agent";',
      'export * as Agent from "@effect-agent/core/internal/Agent";',
      'export * as Agent from "effect-agent/Agent";',
      'export * as Agent from "@effect-agent/thread/Agent";',
      'export * from "@effect-agent/core/Agent";',
    ].map((code) => ({ filename: umbrella, code, errors: [{ messageId: "namespace" }] })),
  ],
});

tester.run("canonical-umbrella-module", plugin.rules["canonical-umbrella-module"], {
  valid: [
    {
      filename: "/repo/packages/effect-agent/src/Agent.ts",
      code: 'export * from "@effect-agent/core/Agent";',
    },
    {
      filename: "/repo/packages/effect-agent/src/AgentRuntime.ts",
      code: 'export * from "@effect-agent/engine/AgentRuntime";',
    },
    {
      filename: "/repo/packages/effect-agent/src/Subagent.ts",
      code: 'export * from "@effect-agent/capabilities/Subagent";',
    },
  ],
  invalid: [
    "",
    'export * from "@effect-agent/core";',
    'export * from "@effect-agent/core/Other";',
    'export * from "@effect-agent/thread/Agent";',
    'export * from "effect-agent/Agent";',
    'export * from "./Agent.ts";',
    'export * as Agent from "@effect-agent/core/Agent";',
    'export { Agent } from "@effect-agent/core/Agent";',
    'export type * from "@effect-agent/core/Agent";',
    'export * from "@effect-agent/core/Agent"; export const own = 1;',
    'export * from "@effect-agent/core/Agent"; export * from "@effect-agent/core/Other";',
    'import * as Agent from "@effect-agent/core/Agent"; export { Agent };',
  ].map((code) => ({
    filename: "/repo/packages/effect-agent/src/Agent.ts",
    code,
    errors: [{ messageId: "canonical" }],
  })),
});

tester.run("no-wildcard-reexports", plugin.rules["no-wildcard-reexports"], {
  valid: [
    'export * as Agent from "./Agent.ts";',
    'export { Agent } from "./internal/agent.ts";',
    'export type { Agent } from "./internal/agent.ts";',
    'export { helper } from "./Helper.ts"; export const own = 1;',
  ].map((code) => ({ filename, code })),
  invalid: [
    'export * from "./Helper.ts";',
    'export type * from "./Helper.ts";',
    'export const own = 1; export * from "./Helper.ts";',
    'export * from "@effect-agent/core/Agent";',
  ].map((code) => ({ filename, code, errors: [{ messageId: "wildcard" }] })),
});

tester.run("require-direct-module-import", plugin.rules["require-direct-module-import"], {
  valid: [
    { filename, code: 'import { own } from "./Own.ts";' },
    { filename, code: 'import type { Own } from "../internal/Own.ts";' },
    { filename, code: 'import { Agent } from "@effect-agent/core/Agent";' },
    { filename, code: 'import * as Agent from "@effect-agent/core/Agent";' },
    { filename, code: 'import type { Agent } from "@effect-agent/core/Agent";' },
    {
      filename,
      code: 'import { Journal } from "@effect-agent/thread/testing/Journal";',
    },
    { filename, code: 'import { Agent } from "effect-agent/Agent";' },
    { filename, code: 'import * as Effect from "effect/Effect";' },
    { filename, code: 'export { Agent } from "@effect-agent/core/Agent";' },
    { filename, code: 'export * as Agent from "@effect-agent/core/Agent";' },
    { filename, code: 'const load = () => import("@effect-agent/core/Agent");' },
    { filename, code: 'type Agent = import("@effect-agent/core/Agent").Agent;' },
    { filename, code: "const load = (source: string) => import(source);" },
    {
      filename: "/repo/examples/demo/src/Consumer.ts",
      code: 'import { Agent } from "@effect-agent/core";',
    },
    {
      filename: "/repo/packages/engine/test/Consumer.test.ts",
      code: 'import { AgentRuntime } from "@effect-agent/engine";',
    },
  ],
  invalid: [
    ...[
      ".",
      "..",
      "./index.ts",
      "../index.js",
      "./nested/index",
      "../../index",
      "@effect-agent/engine",
      "@effect-agent/engine/AgentRuntime",
      "@effect-agent/engine/testing/Journal",
      "@effect-agent/core",
      "@effect-agent/thread/history",
      "@effect-agent/thread/testing",
      "@effect-agent/thread/testing/journal",
      "@effect-agent/core/internal/Agent",
      "@effect-agent/core/src/Agent",
      "@effect-agent/core/Agent.ts",
      "effect-agent",
      "effect-agent/engine",
    ].map((source) => ({
      filename,
      code: `import { value } from "${source}";`,
      errors: [{ messageId: "direct" }],
    })),
    ...[
      'export * from "@effect-agent/core";',
      'export { Agent } from "@effect-agent/core";',
      'export type { Agent } from "@effect-agent/core";',
      'export * as Core from "@effect-agent/core";',
      'export * from "./index.ts";',
      'const load = () => import("@effect-agent/core");',
      "const load = () => import(`@effect-agent/core`);",
      'const load = () => import("./index.ts");',
      'type Agent = import("@effect-agent/core").Agent;',
      'type Own = import("./index.ts").Own;',
      'import Core = require("@effect-agent/core");',
    ].map((code) => ({ filename, code, errors: [{ messageId: "direct" }] })),
    {
      filename: "/repo/packages/effect-agent/src/Agent.ts",
      code: 'import { Agent } from "effect-agent/Agent";',
      errors: [{ messageId: "direct" }],
    },
    {
      filename: "C:\\repo\\packages\\engine\\src\\Module.ts",
      code: 'import { AgentRuntime } from "@effect-agent/engine/AgentRuntime";',
      errors: [{ messageId: "direct" }],
    },
  ],
});
