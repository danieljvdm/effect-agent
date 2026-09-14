import {
  Clock,
  Context,
  Crypto,
  Effect,
  Layer,
  Redacted,
  Schema,
  Semaphore,
  type Scope,
} from "effect";
import { InteractiveBrowserPolicy } from "effect-agent/interactive-browser";
import {
  type BrowserCredentialAccess,
  ProtectedBrowserCheckpoint,
  ProtectedBrowserError,
  type ProtectedBrowserHandle,
  type ProtectedCleanup,
} from "effect-agent/protected-browser";

import {
  BrowserRunHandoffRequest,
  BrowserRunHandoffState,
  BrowserRunLiveViewRequest,
  BrowserRunLiveViewResult,
} from "../InteractiveBrowser.ts";
import { BrowserRunSessionLifecycle } from "../internal/browser-session-lifecycle.ts";
import { BrowserRunProtectedBinding, ProtectedProviderIdentity } from "./binding.ts";
import { makeProtectedBrowserPolicy } from "./policy.ts";

/** Host-only receipt. Store atomically with the controller lease; never return it to a model. */
export class BrowserRunProtectedCheckpoint extends Schema.Class<BrowserRunProtectedCheckpoint>(
  "BrowserRunProtectedCheckpoint",
)({
  ...ProtectedProviderIdentity.fields,
  protected: ProtectedBrowserCheckpoint,
  handoffId: Schema.optionalKey(
    Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(1024))),
  ),
}) {}

export interface BrowserRunProtectedSession {
  readonly handle: ProtectedBrowserHandle;
  readonly sessionId: Redacted.Redacted<string>;
  /** Quiesce before publishing a durable human-control request. Persist before detaching. */
  readonly suspend: Effect.Effect<BrowserRunProtectedCheckpoint, ProtectedBrowserError>;
  /** Fences agent tools before dispatch. Persist the returned receipt before detaching. */
  readonly handoff: (
    request: BrowserRunHandoffRequest,
  ) => Effect.Effect<BrowserRunProtectedCheckpoint, ProtectedBrowserError>;
  /** Available only while human control is held; the host must authorize the specific human. */
  readonly getLiveView: (
    request: BrowserRunLiveViewRequest,
  ) => Effect.Effect<BrowserRunLiveViewResult, ProtectedBrowserError>;
  readonly getHandoffState: Effect.Effect<BrowserRunHandoffState, ProtectedBrowserError>;
  /** Requires provider completion and a renewed observation grant. Tools must observe before acting. */
  readonly returnControl: Effect.Effect<void, ProtectedBrowserError>;
  /** Releases this attachment without terminating the remote browser. Only a committed handoff may detach. */
  readonly detach: Effect.Effect<void, ProtectedBrowserError>;
  readonly close: Effect.Effect<typeof ProtectedCleanup.Type>;
}

/**
 * Host authority for one protected page. The consumer owns durable generation fencing, authorized
 * human recipients, checkpoint integrity, vault exposure metadata, and expiry cleanup. Resume only
 * the latest committed suspended receipt; it never replays browser mutations or creates a page.
 * Hosted passes require Unrestricted network policy: attachment-local request interception
 * cannot enforce an ExactHosts policy during detachment. Current credential grants still apply.
 * Provider inactivity/session deadlines still apply while the host is detached.
 */
export class BrowserRunProtectedHost extends Context.Service<
  BrowserRunProtectedHost,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<
      BrowserRunProtectedSession,
      ProtectedBrowserError,
      Scope.Scope | BrowserCredentialAccess
    >;
    readonly resume: (
      checkpoint: BrowserRunProtectedCheckpoint,
    ) => Effect.Effect<
      BrowserRunProtectedSession,
      ProtectedBrowserError,
      Scope.Scope | BrowserCredentialAccess
    >;
    readonly closeSession: (
      sessionId: Redacted.Redacted<string>,
    ) => Effect.Effect<typeof ProtectedCleanup.Type>;
  }
>()("@effect-agent/platform-cloudflare/BrowserRunProtectedHost") {}

const failure = (reason: ProtectedBrowserError["reason"]) =>
  new ProtectedBrowserError({
    reason,
    dispatch: "not-dispatched",
    milestone: "none",
    observation: "protected",
    cleanup: "not-requested",
  });

const Handoff = Schema.Struct({ handoffId: Schema.NonEmptyString.check(Schema.isMaxLength(1024)) });

const HandoffState = Schema.Struct({
  active: Schema.Boolean,
  handoffId: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(1024))),
  durationMs: Schema.optionalKey(Schema.Natural),
});

const LiveView = Schema.Struct({ devtoolsFrontendUrl: Schema.String });

/** Compose with browserRunProtectedBindingLayer and BrowserRunSessionLifecycle. */
export const browserRunProtectedHostLayer = () =>
  Layer.effect(BrowserRunProtectedHost)(
    Effect.gen(function* () {
      const binding = yield* BrowserRunProtectedBinding;
      const lifecycle = yield* BrowserRunSessionLifecycle;
      const crypto = yield* Crypto.Crypto;

      const open = Effect.fn("BrowserRunProtectedHost.open")(function* (
        input: InteractiveBrowserPolicy,
        checkpoint?: BrowserRunProtectedCheckpoint,
      ): Effect.fn.Return<
        BrowserRunProtectedSession,
        ProtectedBrowserError,
        Scope.Scope | BrowserCredentialAccess
      > {
        const policy = yield* Schema.decodeEffect(InteractiveBrowserPolicy)(input).pipe(
          Effect.mapError(() => failure("denied")),
        );

        if (policy.network._tag !== "Unrestricted") return yield* failure("unsupported");

        const remaining =
          policy.maxElapsedMillis -
          ((yield* Clock.currentTimeMillis) -
            (checkpoint?.protected.startedAt ?? (yield* Clock.currentTimeMillis)));

        if (
          remaining <= 0 ||
          (checkpoint !== undefined &&
            checkpoint.protected.startedAt > (yield* Clock.currentTimeMillis))
        )
          return yield* failure("timeout");
        if (checkpoint !== undefined && checkpoint.protected.actions > policy.maxActions)
          return yield* failure("denied");
        const provider = yield* binding.open(policy, checkpoint);

        const state = yield* makeProtectedBrowserPolicy(
          policy,
          Effect.succeed(provider.driver),
          checkpoint?.protected,
        ).pipe(Effect.provideService(Crypto.Crypto, crypto));

        const lock = yield* Semaphore.make(1);
        let handoff = checkpoint;
        let detached = false;

        const control = <A>(effect: Effect.Effect<A, ProtectedBrowserError>) =>
          lock
            .withPermitsIfAvailable(1)(
              Effect.suspend(() => (detached ? Effect.fail(failure("closed")) : effect)),
            )
            .pipe(Effect.flatMap(Effect.fromOption(() => failure("busy"))));

        const fits = (millis: number) =>
          Effect.gen(function* () {
            const started = handoff?.protected.startedAt ?? startedAt;

            if (millis > policy.maxElapsedMillis - ((yield* Clock.currentTimeMillis) - started))
              return yield* failure("timeout");
          });

        const startedAt = checkpoint?.protected.startedAt ?? (yield* Clock.currentTimeMillis);

        const getHandoffState = Effect.gen(function* () {
          if (handoff?.handoffId === undefined) return yield* failure("denied");
          yield* fits(0);
          const raw = yield* provider.command("Cloudflare.getHandoffState", {});

          const result = yield* Schema.decodeUnknownEffect(HandoffState)(raw).pipe(
            Effect.mapError(() => failure("provider")),
          );

          if (
            (result.active && result.handoffId === undefined) ||
            (result.handoffId !== undefined &&
              result.handoffId !== Redacted.value(handoff.handoffId))
          )
            return yield* failure("denied");

          return BrowserRunHandoffState.make({
            active: result.active,
            ...(result.durationMs === undefined ? {} : { durationMs: result.durationMs }),
            ...(result.handoffId === undefined
              ? {}
              : { handoffId: Redacted.make(result.handoffId) }),
          });
        });

        return {
          handle: state.handle,
          sessionId: provider.identity.sessionId,
          suspend: control(
            Effect.gen(function* () {
              if (handoff !== undefined) {
                yield* state.suspend();

                return handoff;
              }
              const suspended = yield* state.suspend();

              handoff = BrowserRunProtectedCheckpoint.make({
                ...provider.identity,
                protected: suspended,
              });

              return handoff;
            }),
          ),
          handoff: (request) =>
            control(
              Effect.gen(function* () {
                if (handoff?.handoffId !== undefined) return yield* failure("busy");

                const decoded = yield* Schema.decodeEffect(BrowserRunHandoffRequest)(request).pipe(
                  Effect.mapError(() => failure("denied")),
                );

                yield* fits(decoded.timeout);
                const suspended = yield* state.suspend(true);

                // An uncertain handoff is never retried; close exactly this provider session.
                const result = yield* provider
                  .command("Cloudflare.handoff", {
                    instructions: decoded.instructions,
                    timeout: decoded.timeout,
                  })
                  .pipe(
                    Effect.flatMap(Schema.decodeUnknownEffect(Handoff)),
                    Effect.catch(() =>
                      state.handle.close.pipe(
                        Effect.flatMap((cleanup) =>
                          Effect.fail(
                            new ProtectedBrowserError({
                              reason: "outcome-unknown",
                              dispatch: "possibly-dispatched",
                              milestone: suspended.milestone,
                              observation: "closed",
                              cleanup,
                            }),
                          ),
                        ),
                      ),
                    ),
                    Effect.onInterrupt(() => state.handle.close),
                  );

                handoff = BrowserRunProtectedCheckpoint.make({
                  ...provider.identity,
                  protected: suspended,
                  handoffId: Redacted.make(result.handoffId),
                });

                return handoff;
              }),
            ),
          getLiveView: (request) =>
            control(
              Effect.gen(function* () {
                if (handoff?.handoffId === undefined) return yield* failure("denied");

                const decoded = yield* Schema.decodeEffect(BrowserRunLiveViewRequest)(request).pipe(
                  Effect.mapError(() => failure("denied")),
                );

                yield* fits(decoded.expiresInMs);

                const raw = yield* provider.command("Cloudflare.getLiveView", {
                  mode: decoded.mode,
                  expiresInMs: decoded.expiresInMs,
                });

                const result = yield* Schema.decodeUnknownEffect(LiveView)(raw).pipe(
                  Effect.mapError(() => failure("provider")),
                );

                return yield* Schema.decodeEffect(BrowserRunLiveViewResult)({
                  devtoolsFrontendUrl: Redacted.make(result.devtoolsFrontendUrl),
                }).pipe(Effect.mapError(() => failure("provider")));
              }),
            ),
          getHandoffState: control(getHandoffState),
          returnControl: control(
            Effect.gen(function* () {
              if ((yield* getHandoffState).active) return yield* failure("busy");
              yield* state.returnControl;
              handoff = undefined;
            }),
          ),
          detach: control(
            Effect.gen(function* () {
              if (handoff === undefined) return yield* failure("denied");
              yield* state.detach(provider.detach);
              detached = true;
            }),
          ),
          close: state.handle.close,
        };
      }, Effect.withTracerEnabled(false));

      return {
        open: (policy: InteractiveBrowserPolicy) => open(policy),
        resume: (checkpoint: BrowserRunProtectedCheckpoint) =>
          Schema.decodeEffect(BrowserRunProtectedCheckpoint)(checkpoint).pipe(
            Effect.mapError(() => failure("denied")),
            Effect.flatMap((decoded) => open(decoded.protected.policy, decoded)),
          ),
        closeSession: (id: Redacted.Redacted<string>) =>
          lifecycle.close(id).pipe(
            Effect.as("confirmed" as const),
            Effect.catchCause(() => Effect.succeed("unconfirmed" as const)),
          ),
      };
    }),
  );
