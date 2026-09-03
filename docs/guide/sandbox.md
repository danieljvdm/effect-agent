---
title: Sandbox execution
description: Run trusted local processes, choose Code Mode, or capture and browse web pages.
---

# Sandbox execution

Run a trusted command while streaming its output, give generated JavaScript a bounded host API,
or capture a rendered page. The sandbox package defines a separate contract for each job:

| Need                                                                      | Use                                             | Result                              |
| ------------------------------------------------------------------------- | ----------------------------------------------- | ----------------------------------- |
| Run a known command with arguments, working directory, and bounded output | `Sandbox`                                       | A stream of process events          |
| Run generated JavaScript that can call an allowlisted host API            | `CodeExecutor` through [Code Mode](./code-mode) | One schema-bounded result           |
| Render or extract one web page                                            | `PageCapture`                                   | One bounded capture result          |
| Capture a rendered page as PNG                                            | `PageScreenshot`                                | Bounded image bytes                 |
| Crawl one HTTPS host                                                      | `PageCrawl`                                     | A scoped stream of Markdown records |
| Navigate, read, click, or fill a live page                                | `InteractiveBrowser`                            | A scoped browser handle             |

`Sandbox` is for command-shaped work. It keeps the executable, arguments, current directory,
environment names, network request, and resource limits in a Schema-defined request. The host can
call it directly or wrap it in an application Tool. Process execution has no durable recovery;
a process that may have changed an external system has an uncertain outcome after a crash.

`CodeExecutor` is a different port for one generated JavaScript async function. The only host
authority it receives is the scoped `CodeExecutionHost` callback. [Code Mode](./code-mode) adds
the agent-facing Tool, validation, broker, and accounting. Do not use a command process when the
work needs that narrow callback boundary.

Browser services also have their own contracts because rendering and navigation are egress
operations. Use [browser services](./browser) for Cloudflare adapter setup, network policy, and
browser session lifetime.

## Install a local process runner

In your application, install the local adapter:

```sh
bun add @effect-agent/sandbox-local@beta
```

Requires `effect@^4.0.0-rc.112`. For the example below, also install
`@effect-agent/sandbox@beta` and `@effect/platform-node@4.0.0-rc.112`.
Keep framework packages at the [same release](./getting-started#installation-and-compatibility).

`@effect-agent/sandbox-local` runs a child process on the current machine. It is useful for local
development and trusted automation. Every event identifies it as `unisolated`.

## Run a trusted local process

The local adapter accepts only the `unisolated-process` runtime with the `local-process` identity.
The request must name every environment variable the child may receive. It starts with an empty
environment, then copies only those names. The `layer` export supplies the Node process services.

```ts twoslash
// @types: node
import { NodeRuntime } from "@effect/platform-node";
import { NetworkDisabled, Sandbox, SandboxRequest } from "@effect-agent/sandbox/Sandbox";
import { layer as LocalSandboxLive } from "@effect-agent/sandbox-local/LocalSandbox";
import { Console, Duration, Effect, Stream } from "effect";

const request = SandboxRequest.make({
  runtime: { kind: "unisolated-process", identity: "local-process" },
  command: process.execPath,
  args: ["-e", "console.log(process.env.LANG ?? 'LANG is not set')"],
  cwd: process.cwd(),
  environment: { allow: ["LANG"] },
  mounts: [],
  network: NetworkDisabled.make({}),
  limits: {
    maxOutputBytes: 16 * 1024,
    maxWallTime: Duration.seconds(10),
  },
  secretHandles: [],
  artifactRules: [],
});

const program = Effect.gen(function* () {
  const sandbox = yield* Sandbox;
  yield* sandbox.execute(request).pipe(
    Stream.runForEach((event) => {
      switch (event._tag) {
        case "SandboxStarted":
          return Console.log(`started ${event.implementation.identity}`);
        case "SandboxOutput":
          return event.stream === "stdout" ? Console.log(event.text) : Console.error(event.text);
        case "SandboxExited":
          return Console.log(`exited ${event.exitCode}`);
      }
    }),
  );
}).pipe(Effect.provide(LocalSandboxLive), Effect.scoped);

NodeRuntime.runMain(program);
```

Save this as `sandbox.ts` and run it with Node.js:

```sh
node --experimental-transform-types sandbox.ts
```

`Sandbox.execute` emits `SandboxStarted`, zero or more `SandboxOutput` events, then
`SandboxExited` when the process reaches an exit status. Stdout and stderr events may interleave.
The local adapter counts their combined bytes against `maxOutputBytes`.

A zero exit code completes the stream. A nonzero exit still emits `SandboxExited` first, then
fails with `SandboxExitError`. `NodeRuntime.runMain` reports the failure and exits unsuccessfully.
A spawn failure has no started or exited event. `SandboxOutputLimitError` and `SandboxTimeoutError`
also remain in the typed error channel. Use `Effect.catchTag` when your application has a recovery
action for one of these failures.

Interrupting the consumer preserves Effect interruption. The adapter does not turn it into an exit
record or a `SandboxError`. Keep the call in a Scope, as above, so stream finalization owns child
process cleanup.

## Limits and local adapter boundaries

The local adapter enforces its wall-clock limit and combined stdout and stderr byte limit. It does
not enforce process isolation, mount access, CPU limits, memory limits, secret-handle resolution,
or artifact collection. Requests that ask for those features fail as
`SandboxUnsupportedRequestError` before the process starts.

`NetworkDisabled` is required by the local adapter because it rejects allowlists. It is only a
request marker there. The adapter does not configure the operating system or a network namespace,
so it does not prevent the child process from opening a connection. Do not run untrusted commands,
model-generated commands, or sensitive credentials through this adapter.

The local adapter also rejects any runtime identity other than `local-process`. It passes no
environment values unless they appear in `environment.allow`, and it never resolves
`secretHandles` into environment variables. Put secrets behind a real isolated runtime or an
application-owned host API.

## Use the other services deliberately

`CodeExecutor` accepts only JavaScript source that evaluates to one async function. It has bounded
source, logs, result, wall-clock time, CPU time when supported, host calls, and host-call payloads.
An implementation must enforce each requested limit or return a typed unsupported error. Use Code
Mode when an agent should write that program and call explicitly allowed application Tools.

`PageCapture` returns one rendered content, Markdown, links, structured extraction, or selector
scrape result. Its output is untrusted input. `PageCrawl` returns a scoped stream of rendered
Markdown from one exact HTTPS host. `InteractiveBrowser` returns a scoped handle that cannot be
persisted or transferred. Its actions can have uncertain outcomes, especially after a click or
fill request.

The Cloudflare adapters provide page capture, crawl, screenshots, and interactive browser passes.
They do not make local command execution isolated. Keep browser and process capabilities behind
the host policy that authenticates callers and authorizes the resources they may reach.
