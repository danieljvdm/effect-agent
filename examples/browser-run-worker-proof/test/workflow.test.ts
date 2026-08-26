import { NodeCrypto } from "@effect/platform-node";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Option, Ref } from "effect";

import { BrowserRunWorkerProofResult } from "../src/contract.ts";
import {
  runWorkerProofWith,
  temporaryWorker,
  type WorkerDeploymentOperations,
  type WorkerProofError,
} from "../src/workflow.ts";

const proofResult = () =>
  BrowserRunWorkerProofResult.make({
    sourceUrl: "https://example.com/",
    action: "markdown",
    fact: "Example Domain",
    scrape: {
      selectors: ["h1", "a"],
      headingFact: "Example Domain",
    },
    screenshot: {
      mediaType: "image/png",
      pngSignatureValid: true,
    },
    interactive: {
      finalUrl: "https://example.com/",
      readFact: "Example Domain",
    },
  });

describe("Browser Run Worker proof deployment resource", () => {
  it.effect("deletes the successfully deployed Worker when its Scope exits", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const operations: WorkerDeploymentOperations = {
        nameExists: () => Effect.succeed(false),
        deploy: (name) => Ref.update(events, (current) => [...current, `deploy:${name}`]),
        invoke: () => Effect.succeed(proofResult()),
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

  it.effect("returns only validated interactive proof metadata and still deletes the Worker", () =>
    Effect.gen(function* () {
      const events = yield* Ref.make<ReadonlyArray<string>>([]);
      const operations: WorkerDeploymentOperations = {
        nameExists: () => Effect.succeed(false),
        deploy: (name) => Ref.update(events, (current) => [...current, `deploy:${name}`]),
        invoke: (name) =>
          Ref.update(events, (current) => [...current, `invoke:${name}`]).pipe(
            Effect.as(proofResult()),
          ),
        delete: (name) => Ref.update(events, (current) => [...current, `delete:${name}`]),
      };

      const proof = yield* runWorkerProofWith(operations).pipe(Effect.provide(NodeCrypto.layer));

      assert.strictEqual(proof.result.interactive.finalUrl, "https://example.com/");
      assert.strictEqual(proof.result.interactive.readFact, "Example Domain");
      assert.deepStrictEqual(proof.result.scrape.selectors, ["h1", "a"]);
      assert.strictEqual(proof.result.scrape.headingFact, "Example Domain");
      assert.deepStrictEqual(Object.keys(proof.result.interactive).sort(), [
        "finalUrl",
        "readFact",
      ]);
      assert.deepStrictEqual(yield* Ref.get(events), [
        `deploy:${proof.name}`,
        `invoke:${proof.name}`,
        `delete:${proof.name}`,
      ]);
    }),
  );
});
