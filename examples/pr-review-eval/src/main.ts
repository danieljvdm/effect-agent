import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ConfigProvider, Console, Effect, FileSystem, Path } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./command.ts";
import { CURRENT_RUNNER_VERSION } from "./contracts.ts";

const localConfig = ConfigProvider.layerAdd(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const envFile = yield* path.fromFileUrl(new URL("../../../.env.local", import.meta.url));

    return (yield* fs.exists(envFile))
      ? yield* ConfigProvider.fromDotEnv({ path: envFile })
      : ConfigProvider.fromUnknown({});
  }),
);

const program = Command.run(command, { version: CURRENT_RUNNER_VERSION }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(localConfig),
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
