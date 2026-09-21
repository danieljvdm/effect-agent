import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, Schema } from "effect";

import { localPreview } from "./runtime.ts";

Effect.gen(function* () {
  const port = yield* Config.schema(
    Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 65535 })),
    "PREVIEW_PORT",
  ).pipe(Config.withDefault(4173));

  const { origin } = yield* localPreview(port);

  yield* Console.log(`Local travel preview: ${origin}
Accept the local HTTPS certificate, then create an email account.
Verification emails are saved locally; their file paths appear in this terminal.
Use sk-preview-local for the offline planner and send: complete travel cards fixture
This preview uses test data, makes no provider calls, and resets when stopped.`);

  return yield* Effect.never;
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer), NodeRuntime.runMain);
