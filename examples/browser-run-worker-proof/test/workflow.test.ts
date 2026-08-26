import { assert, describe, it } from "@effect/vitest";
import { Effect, Option, Ref } from "effect";

import { BrowserRunWorkerProofResult } from "../src/contract.ts";
import {
  temporaryWorker,
  type WorkerDeploymentOperations,
  type WorkerProofError,
} from "../src/workflow.ts";

describe("Browser Run Worker proof deployment resource", () => {
  it.effect("deletes the successfully deployed Worker when its Scope exits", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const operations: WorkerDeploymentOperations = {
        nameExists: () => Effect.succeed(false),
        deploy: (name) => Ref.update(events, (current) => [...current, `deploy:${name}`]),
        invoke: () =>
          Effect.succeed(
            BrowserRunWorkerProofResult.make({
              sourceUrl: "https://example.com/",
              action: "markdown",
              fact: "Example Domain",
              screenshot: {
                mediaType: "image/png",
                pngSignatureValid: true,
              },
            }),
          ),
        delete: (name) => Ref.update(events, (current) => [...current, `delete:${name}`]),
      };
      const deletionFailure = yield* Ref.make<Option.Option<WorkerProofError>>(Option.none());
      const name = "effect-agent-browser-proof-0123456789abcdef0123456789abcdef";

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* temporaryWorker(operations, name, deletionFailure);
          assert.deepStrictEqual(yield* Ref.get(events), [`deploy:${name}`]);
        }),
      );

      assert.deepStrictEqual(yield* Ref.get(events), [`deploy:${name}`, `delete:${name}`]);
      assert.isTrue(Option.isNone(yield* Ref.get(deletionFailure)));
    }),
  );
});
