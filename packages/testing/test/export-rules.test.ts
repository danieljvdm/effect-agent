import { RuleTester } from "oxlint/plugins-dev";
import { describe, it } from "vite-plus/test";

import plugin from "../../../oxlint/plugin-exports.ts";

RuleTester.describe = describe;
RuleTester.it = it;

const tester = new RuleTester();
const filename = "/repo/packages/engine/src/Module.ts";
const internal = "/repo/packages/engine/src/internal/Module.ts";
const root = "/repo/packages/core/src/index.ts";
const group = "/repo/packages/core/src/unstable/httpapi/index.ts";

tester.run("no-internal-barrel", plugin.rules["no-internal-barrel"], {
  valid: [
    "export const own = 1;",
    'import { input } from "./Input.ts"; export const own = input + 1;',
    'export { helper } from "./Helper.ts"; export function own() {}',
    'export { helper } from "./Helper.ts"; initialize();',
    'import "./Setup.ts"; export {};',
    'import type { Value } from "./Value.ts"; export type Own = { value: Value };',
    'import { value } from "./Value.ts"; export default () => value;',
  ].map((code) => ({ filename: internal, code })),
  invalid: [
    'export * from "./Module.ts";',
    'export * as Module from "./Module.ts";',
    'export { value } from "./Module.ts";',
    'export type { Value } from "./Module.ts";',
    'import { value as local } from "./Module.ts"; export { local as value };',
    'import * as Module from "./Module.ts"; export default Module;',
    '"use strict"; export * from "./Module.ts";',
  ].map((code) => ({ filename: internal, code, errors: [{ messageId: "barrel" }] })),
});

tester.run("public-entrypoint", plugin.rules["public-entrypoint"], {
  valid: [
    { filename: root, code: 'export * as Agent from "./Agent.ts";' },
    {
      filename: root,
      code: 'export { pipe, flow } from "./Function.ts"; export * as Function from "./Function.ts";',
    },
    { filename: root, code: 'export type { Agent } from "./Agent.ts";' },
    { filename: root, code: 'export { type Agent, make as create } from "./Agent.ts";' },
    {
      filename: "/repo/packages/effect-agent/src/index.ts",
      code: 'export { Agent } from "@effect-agent/core/Agent";',
    },
    { filename: root, code: 'export type * as Agent from "./Agent.ts";' },
    {
      filename: group,
      code: 'export * as HttpApi from "./HttpApi.ts"; export * as HttpApiClient from "./HttpApiClient.ts";',
    },
    {
      filename: "C:\\repo\\packages\\effect-agent\\src\\index.ts",
      code: 'export * as Agent from "./Agent.ts";',
    },
  ],
  invalid: [
    ...[
      'export * from "./Agent.ts";',
      'export type * from "./Agent.ts";',
      'export * as agent from "./agent.ts";',
      'export * as Alias from "./Agent.ts";',
      'export * as Agent from "./internal/Agent.ts";',
      'export * as Agent from "@effect-agent/core/Agent";',
      'export { Agent as default } from "./Agent.ts";',
      'export { Agent as "default" } from "./Agent.ts";',
      "export const Agent = 1;",
      "export function make() {}",
      "export interface Agent {}",
      "export default {};",
      "initialize();",
      'import "./Setup.ts";',
    ].map((code) => ({ filename: root, code, errors: [{ messageId: "entrypoint" }] })),
    {
      filename: root,
      code: 'import * as Agent from "./Agent.ts"; export { Agent };',
      errors: [{ messageId: "entrypoint" }, { messageId: "entrypoint" }],
    },
    {
      filename: group,
      code: 'export * from "./HttpApi.ts";',
      errors: [{ messageId: "entrypoint" }],
    },
  ],
});

tester.run("no-self-barrel-import", plugin.rules["no-self-barrel-import"], {
  valid: [
    ...[
      'import { own } from "./Own.ts";',
      'import type { Own } from "../internal/Own.ts";',
      'import { Agent } from "@effect-agent/core";',
      'import * as Agent from "@effect-agent/core/Agent";',
      'import { Journal } from "@effect-agent/thread/testing";',
      'import { Journal } from "@effect-agent/thread/testing/Journal";',
      'import { History } from "@effect-agent/thread/history";',
      'import { Agent } from "effect-agent";',
      'import { Effect } from "effect";',
      'import { HttpApi } from "effect/unstable/httpapi";',
      'export * from "@effect-agent/core/Agent";',
      'export { Agent } from "@effect-agent/core";',
      'export * from "./Own.ts";',
      'const load = () => import("@effect-agent/core");',
      'type Agent = import("@effect-agent/core/Agent").Agent;',
      "const load = (source: string) => import(source);",
    ].map((code) => ({ filename, code })),
    {
      filename: "/repo/examples/demo/src/Consumer.ts",
      code: 'import { Agent } from "@effect-agent/core";',
    },
    {
      filename: "/repo/packages/engine/test/Consumer.test.ts",
      code: 'import { AgentRuntime } from "@effect-agent/engine";',
    },
    {
      filename: "/repo/packages/platform-node/src/NodeStream.ts",
      code: 'export * from "@effect/platform-node-shared/NodeStream";',
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
    ].map((source) => ({
      filename,
      code: `import { value } from "${source}";`,
      errors: [{ messageId: "self" }],
    })),
    ...[
      'export * from "@effect-agent/engine";',
      'export { AgentRuntime } from "@effect-agent/engine/AgentRuntime";',
      'export type { AgentRuntime } from "@effect-agent/engine/AgentRuntime";',
      'export * as Engine from "@effect-agent/engine";',
      'const load = () => import("@effect-agent/engine");',
      "const load = () => import(`@effect-agent/engine`);",
      'const load = () => import("./index.ts");',
      'type AgentRuntime = import("@effect-agent/engine").AgentRuntime;',
      'type Own = import("./index.ts").Own;',
      'import Engine = require("@effect-agent/engine");',
    ].map((code) => ({ filename, code, errors: [{ messageId: "self" }] })),
    {
      filename: "/repo/packages/effect-agent/src/Agent.ts",
      code: 'import { Agent } from "effect-agent/Agent";',
      errors: [{ messageId: "self" }],
    },
    {
      filename: "C:\\repo\\packages\\engine\\src\\Module.ts",
      code: 'import { AgentRuntime } from "@effect-agent/engine/AgentRuntime";',
      errors: [{ messageId: "self" }],
    },
  ],
});
