import { BrowserRunHandoffRequest } from "@effect-agent/platform-cloudflare/interactive-browser";
import {
  BrowserRunProtectedCheckpoint,
  BrowserRunProtectedHost,
  browserRunProtectedHostLayer,
} from "@effect-agent/platform-cloudflare/protected-browser";
import { BrowserCrypto } from "@effect/platform-browser";
import { expect, expectTypeOf, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema, type Scope } from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import {
  BrowserCredentialAccess,
  CredentialObservationGrant,
  CredentialOfferMetadata,
  CredentialTarget,
  LoginCredential,
  ProtectedBrowserControl,
  ProtectedBrowserError,
} from "effect-agent/protected-browser";
import { TestClock } from "effect/testing";

import { BrowserRunSessionLifecycle } from "../src/internal/browser-session-lifecycle.ts";
import {
  BrowserRunProtectedBinding,
  type ProtectedProviderIdentity,
} from "../src/protected-browser/binding.ts";
import {
  ProtectedBrowserDispatch,
  ProtectedTransportError,
} from "../src/protected-browser/policy.ts";

const policy = InteractiveBrowserPolicy.make({
  network: { _tag: "Unrestricted" },
  maxActions: 30,
  maxElapsedMillis: 120_000,
  maxReturnedBytes: 16_384,
});

const takeover = BrowserRunHandoffRequest.make({
  instructions: "Complete sign-in",
  timeout: 60_000,
});

const target = CredentialTarget.make({
  topOrigin: "https://shop.test",
  frameOrigin: "https://shop.test",
  recipientOrigin: "https://shop.test",
  document: "00000000-0000-4000-8000-000000000001",
  frame: "00000000-0000-4000-8000-000000000002",
  form: "00000000-0000-4000-8000-000000000003",
});

const fixture = () => {
  const identity: ProtectedProviderIdentity = {
    sessionId: Redacted.make("00000000-0000-4000-8000-000000000004"),
    contextId: Redacted.make("exact-context"),
    targetId: Redacted.make("exact-page"),
  };

  let attachment = 0;
  let closed = false;
  let active = false;
  let allowObservation = true;
  let uncertainHandoff = false;
  const commands: Array<string> = [];
  const restored: Array<ProtectedProviderIdentity> = [];
  const observations: Array<Parameters<BrowserCredentialAccess["Service"]["observation"]>[0]> = [];
  const filled: Array<string> = [];

  const access = BrowserCredentialAccess.of({
    caller: Effect.succeed(Redacted.make("authorized-attempt")),
    list: () =>
      Effect.succeed([
        {
          key: Redacted.make("host-only-vault-key"),
          metadata: CredentialOfferMetadata.make({ label: "Account" }),
        },
      ]),
    authorize: () => Effect.void,
    authorizeAction: () => Effect.void,
    resolve: () =>
      Effect.succeed(
        LoginCredential.make({
          username: Redacted.make("private-user"),
          password: Redacted.make("private-password"),
        }),
      ),
    observation: (request) =>
      Effect.sync(() => {
        observations.push(request);

        return allowObservation
          ? CredentialObservationGrant.make({
              decision: "trust-recipient-no-credential-echo",
              origins: ["https://shop.test"],
            })
          : "deny";
      }),
  });

  const binding = Layer.succeed(BrowserRunProtectedBinding, {
    open: (_policy, previous) =>
      Effect.gen(function* () {
        if (closed)
          return yield* ProtectedBrowserError.make({
            reason: "closed",
            dispatch: "not-dispatched",
            milestone: "none",
            observation: "closed",
            cleanup: "confirmed",
          });
        if (previous !== undefined) restored.push(previous);
        attachment++;
        let detached = false;
        let invalid = false;
        let controls: Array<ProtectedBrowserControl> = [];

        const close = Effect.sync(() => {
          closed = true;
          invalid = true;

          return "confirmed" as const;
        });

        yield* Effect.addFinalizer(() => (detached ? Effect.void : close));

        const get = (ref: string) =>
          Effect.suspend(() => {
            const control = controls.find((entry) => entry.ref === ref);

            return invalid || control === undefined
              ? Effect.fail(ProtectedTransportError.make({ reason: "stale-reference" }))
              : Effect.succeed(control);
          });

        return {
          identity,
          driver: {
            restrictObservation: () => Effect.void,
            context: Effect.succeed({
              document: target.document,
              topOrigin: target.topOrigin,
              frameOrigins: [target.frameOrigin],
            }),
            discover: Effect.sync(() => {
              controls = [
                ProtectedBrowserControl.make({
                  ref: crypto.randomUUID(),
                  target,
                  role: "password",
                  label: "Password",
                }),
                ProtectedBrowserControl.make({
                  ref: crypto.randomUUID(),
                  target,
                  role: "button",
                  label: "Continue",
                }),
              ];

              return {
                document: target.document,
                topOrigin: target.topOrigin,
                frameOrigins: [target.frameOrigin],
                text: "Account",
                controls,
                truncated: false,
              };
            }),
            target: get,
            navigate: () => Effect.void,
            click: (ref) => get(ref).pipe(Effect.asVoid),
            fill: (ref, _role, value) =>
              Effect.gen(function* () {
                yield* get(ref);
                yield* (yield* ProtectedBrowserDispatch).mark;
                filled.push(Redacted.value(value));
              }),
            resetReferences: () => {
              controls = [];
            },
            invalidate: () => {
              invalid = true;
              controls = [];
            },
            close,
          },
          command: (method) =>
            Effect.suspend(() => {
              commands.push(method);
              if (method === "Cloudflare.handoff") {
                active = true;

                return uncertainHandoff
                  ? Effect.fail(
                      ProtectedBrowserError.make({
                        reason: "provider",
                        dispatch: "not-dispatched",
                        milestone: "none",
                        observation: "protected",
                        cleanup: "not-requested",
                      }),
                    )
                  : Effect.succeed({ handoffId: "exact-handoff" });
              }

              return Effect.succeed({ active, handoffId: "exact-handoff", durationMs: 1 });
            }),
          detach: Effect.sync(() => {
            detached = true;
            invalid = true;
          }),
        };
      }),
  });

  const layer = browserRunProtectedHostLayer().pipe(
    Layer.provide(binding),
    Layer.provide(
      Layer.succeed(BrowserRunSessionLifecycle, {
        close: () =>
          Effect.sync(() => {
            closed = true;
          }),
      }),
    ),
    Layer.provide(BrowserCrypto.layer),
    Layer.provideMerge(Layer.succeed(BrowserCredentialAccess, access)),
  );

  return {
    layer,
    filled,
    observations,
    commands,
    restored,
    closed: () => closed,
    attachments: () => attachment,
    complete: () => {
      active = false;
    },
    deny: () => {
      allowObservation = false;
    },
    loseReply: () => {
      uncertainHandoff = true;
    },
  };
};

it.effect(
  "suspends, transfers and resumes the exact protected page without exposing credentials or reviving refs",
  () => {
    const f = fixture();

    return Effect.gen(function* () {
      const host = yield* BrowserRunProtectedHost;

      const first = yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* host.open(policy);

          yield* session.handle.navigate({ url: "https://shop.test" });
          const observation = yield* session.handle.observe;
          const password = observation.controls[0]!;

          const offers = yield* session.handle.listCredentialOffers({
            kind: "login",
            target: password.ref,
          });

          yield* session.handle.useCredential({
            offer: offers[0]!.ref,
            fields: [{ ref: password.ref, role: "password" }],
          });
          const checkpoint = yield* session.suspend;

          expect(checkpoint.protected.humanExposure).toBe(false);
          expect(checkpoint.handoffId).toBeUndefined();
          expect((yield* session.handle.observe.pipe(Effect.flip)).reason).toBe("busy");
          const encoded = yield* Schema.encodeEffect(BrowserRunProtectedCheckpoint)(checkpoint);

          expect(JSON.stringify(encoded)).not.toContain("private-password");
          expect(JSON.stringify(encoded)).not.toContain("host-only-vault-key");
          yield* session.detach;
          expect(yield* session.handle.close).toBe("not-requested");
          expect(f.closed()).toBe(false);

          return { encoded, ref: observation.controls[1]!.ref };
        }),
      );

      expect(f.closed()).toBe(false);
      const checkpoint = yield* Schema.decodeEffect(BrowserRunProtectedCheckpoint)(first.encoded);

      const human = yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* host.resume(checkpoint);
          const receipt = yield* session.handoff(takeover);

          expect(receipt.protected.humanExposure).toBe(true);
          expect(receipt.protected.humanOrigins).toEqual(["https://shop.test"]);
          expect(receipt.protected.exposures).toEqual([target]);
          expect((yield* session.returnControl.pipe(Effect.flip)).reason).toBe("busy");
          yield* session.detach;

          return receipt;
        }),
      );

      expect(f.closed()).toBe(false);
      f.complete();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const session = yield* host.resume(human);

          yield* session.returnControl;
          expect(
            (yield* session.handle.navigate({ url: "https://shop.test" }).pipe(Effect.flip)).reason,
          ).toBe("stale-reference");
          const observation = yield* session.handle.observe;

          expect(observation.observation).toBe("approved-after-exposure");
          expect((yield* session.handle.click({ ref: first.ref }).pipe(Effect.flip)).reason).toBe(
            "stale-reference",
          );
          yield* session.handle.click({ ref: observation.controls[1]!.ref });
        }),
      );
      expect(f.closed()).toBe(true);
      expect(f.filled).toEqual(["private-password"]);
      expect(f.commands.filter((command) => command === "Cloudflare.handoff")).toHaveLength(1);
      expect(f.restored.map((entry) => Redacted.value(entry.targetId))).toEqual([
        "exact-page",
        "exact-page",
      ]);
      expect(f.observations.at(-1)).toMatchObject({
        humanExposure: true,
        humanOrigins: ["https://shop.test"],
        exposures: [target],
      });
    }).pipe(Effect.provide(f.layer));
  },
);

it.effect("preserves exhausted action budgets and elapsed deadlines across host suspension", () => {
  const f = fixture();

  return Effect.gen(function* () {
    const host = yield* BrowserRunProtectedHost;

    expect(
      (yield* host
        .open({ ...policy, network: { _tag: "ExactHosts", allowedHosts: ["shop.test"] } })
        .pipe(Effect.scoped, Effect.flip)).reason,
    ).toBe("unsupported");
    expect(f.attachments()).toBe(0);

    const checkpoint = yield* Effect.scoped(
      Effect.gen(function* () {
        const session = yield* host.open({ ...policy, maxActions: 1 });

        yield* session.handle.observe;
        const checkpoint = yield* session.handoff(takeover);

        yield* session.detach;

        return checkpoint;
      }),
    );

    f.complete();
    yield* Effect.scoped(
      Effect.gen(function* () {
        const session = yield* host.resume(checkpoint);

        yield* session.returnControl;
        expect((yield* session.handle.observe.pipe(Effect.flip)).reason).toBe("limit");
        yield* session.detach.pipe(Effect.flip); // Return consumed the suspended controller.
      }),
    );
    yield* TestClock.adjust("2 minutes");
    expect((yield* host.resume(checkpoint).pipe(Effect.scoped, Effect.flip)).reason).toBe(
      "timeout",
    );
    expect(f.attachments()).toBe(2);
  }).pipe(Effect.provide(f.layer));
});

it.effect(
  "keeps the Return observation gate closed when the fresh observation exceeds its byte budget",
  () => {
    // Regression: https://github.com/danieljvdm/effect-agent/pull/479#pullrequestreview-5203030938
    const f = fixture();

    return Effect.gen(function* () {
      const session = yield* (yield* BrowserRunProtectedHost).open({
        ...policy,
        maxReturnedBytes: 1,
      });

      yield* session.handoff(takeover);
      f.complete();
      yield* session.returnControl;
      expect((yield* session.handle.observe.pipe(Effect.flip)).reason).toBe("limit");
      expect(
        yield* session.handle
          .navigate({ url: "https://shop.test" })
          .pipe(Effect.match({ onFailure: (error) => error.reason, onSuccess: () => "navigated" })),
      ).toBe("stale-reference");
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect(
  "refuses cumulative human origins beyond the checkpoint bound before handoff dispatch",
  () => {
    const f = fixture();

    return Effect.gen(function* () {
      const host = yield* BrowserRunProtectedHost;
      const initial = yield* host.open(policy);
      const checkpoint = yield* initial.suspend;

      yield* initial.detach;

      const resumed = yield* host.resume({
        ...checkpoint,
        protected: {
          ...checkpoint.protected,
          humanExposure: true,
          humanOrigins: Array.from({ length: 16 }, (_, i) => `https://prior${i}.test`),
        },
      });

      expect((yield* resumed.handoff(takeover).pipe(Effect.flip)).reason).toBe("needs-attention");
      expect(f.commands).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect(
  "requires explicit observation trust after human entry even without a vault exposure",
  () => {
    const f = fixture();

    return Effect.gen(function* () {
      const session = yield* (yield* BrowserRunProtectedHost).open(policy);

      yield* session.handoff(takeover);
      f.complete();
      f.deny();
      expect((yield* session.returnControl.pipe(Effect.flip)).reason).toBe("observation-blocked");
      expect((yield* session.handle.observe.pipe(Effect.flip)).reason).toBe("busy");
      expect(f.observations.at(-1)).toMatchObject({ humanExposure: true, exposures: [] });
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it.effect(
  "closes an uncertain handoff instead of retrying or releasing a resumable receipt",
  () => {
    const f = fixture();

    return Effect.gen(function* () {
      const session = yield* (yield* BrowserRunProtectedHost).open(policy);

      f.loseReply();
      expect(yield* session.handoff(takeover).pipe(Effect.flip)).toMatchObject({
        reason: "outcome-unknown",
        dispatch: "possibly-dispatched",
        observation: "closed",
        cleanup: "confirmed",
      });
      expect(f.closed()).toBe(true);
      expect((yield* session.detach.pipe(Effect.flip)).reason).toBe("denied");
      expect(f.commands).toEqual(["Cloudflare.handoff"]);
    }).pipe(Effect.scoped, Effect.provide(f.layer));
  },
);

it("keeps caller authority and scope visible at the public host boundary", () => {
  expectTypeOf<
    Effect.Services<ReturnType<BrowserRunProtectedHost["Service"]["resume"]>>
  >().toEqualTypeOf<Scope.Scope | BrowserCredentialAccess>();
  expectTypeOf<
    Effect.Error<ReturnType<BrowserRunProtectedHost["Service"]["resume"]>>
  >().toEqualTypeOf<ProtectedBrowserError>();
});
