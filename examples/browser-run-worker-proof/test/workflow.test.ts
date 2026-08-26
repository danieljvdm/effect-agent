import { NodeCrypto } from "@effect/platform-node";
import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Option, Ref } from "effect";
import { TestClock } from "effect/testing";

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
      screenshot: { mediaType: "image/png", pngSignatureValid: true },
      scrolled: true,
      liveViewCreated: true,
      handoffActive: true,
      closed: true,
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
        "closed",
        "finalUrl",
        "handoffActive",
        "liveViewCreated",
        "readFact",
        "screenshot",
        "scrolled",
      ]);
      assert.deepStrictEqual(yield* Ref.get(events), [
        `deploy:${proof.name}`,
        `invoke:${proof.name}`,
        `delete:${proof.name}`,
      ]);
    }),
  );

  it.effect("times out one unresolved invocation and deletes its Worker without retrying", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const events = yield* Ref.make<ReadonlyArray<string>>([]);
        const invoked = yield* Deferred.make<void>();
        const operations: WorkerDeploymentOperations = {
          nameExists: () => Effect.succeed(false),
          deploy: (name) => Ref.update(events, (current) => [...current, `deploy:${name}`]),
          invoke: (name) =>
            Ref.update(events, (current) => [...current, `invoke:${name}`]).pipe(
              Effect.andThen(Deferred.succeed(invoked, undefined)),
              Effect.andThen(Effect.never),
            ),
          delete: (name) => Ref.update(events, (current) => [...current, `delete:${name}`]),
        };

        const fiber = yield* runWorkerProofWith(operations).pipe(
          Effect.provide(NodeCrypto.layer),
          Effect.forkChild,
        );
        yield* Deferred.await(invoked);
        yield* TestClock.adjust("150 seconds");
        const error = yield* Fiber.join(fiber).pipe(Effect.flip);

        assert.strictEqual(error.reason, "invocation-timeout");
        const observed = yield* Ref.get(events);
        const deployment = observed.find((event) => event.startsWith("deploy:")) ?? "";
        assert.deepStrictEqual(observed, [
          deployment,
          deployment.replace("deploy:", "invoke:"),
          deployment.replace("deploy:", "delete:"),
        ]);
      }),
    ),
  );
});
