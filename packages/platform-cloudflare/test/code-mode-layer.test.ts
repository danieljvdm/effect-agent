import { CodeMode } from "@effect-agent/capabilities";
import { ToolExecutionClass } from "@effect-agent/engine";
import { Context, Effect, Layer, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import { CloudflareCodeMode } from "../src/index.ts";

class Database extends Context.Service<Database, { readonly count: number }>()("test/Database") {}
class SetupError extends Schema.TaggedError<SetupError>()("SetupError", {}) {}

const Read = Tool.make("read_count", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Number,
}).annotate(ToolExecutionClass, "readonly");

const Other = Tool.make("read_other", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.String,
}).annotate(ToolExecutionClass, "readonly");

const definition = CodeMode.make("run_code", {
  description: "Read counts",
  tools: { first: { read: Read }, second: { read: Other } },
});

const loader: WorkerLoader = {
  get: () => {
    throw new Error("Unexpected worker get");
  },
  load: () => {
    throw new Error("Unexpected worker load");
  },
};

describe("Cloudflare Code Mode assembly", () => {
  it("preserves missing handlers, application dependencies, and handler setup failures", async () => {
    const handlers = Toolkit.make(Read).toLayer(
      Effect.gen(function* () {
        const database = yield* Database;

        if (database.count < 0) return yield* SetupError.make({});

        return { read_count: () => Effect.succeed(database.count) };
      }),
    );

    const partial = CloudflareCodeMode.layer(definition, { loader, handlers });

    expectTypeOf<Layer.Services<typeof partial>>().toEqualTypeOf<
      Database | Tool.Handler<"read_other">
    >();
    expectTypeOf<Layer.Error<typeof partial>>().toEqualTypeOf<SetupError>();
    expectTypeOf<Layer.Success<typeof partial>>().toEqualTypeOf<Tool.Handler<"run_code">>();

    const complete = CloudflareCodeMode.layer(definition, {
      loader,
      handlers: Layer.merge(
        handlers,
        Toolkit.make(Other).toLayer({ read_other: () => Effect.succeed("other") }),
      ),
    });

    expectTypeOf<Layer.Services<typeof complete>>().toEqualTypeOf<Database>();
    const build = Layer.build(complete.pipe(Layer.provide(Layer.succeed(Database, { count: -1 }))));
    const error = await Effect.runPromise(build.pipe(Effect.scoped, Effect.flip));

    expect(error).toBeInstanceOf(SetupError);
  });
});
