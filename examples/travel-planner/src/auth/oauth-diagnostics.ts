import { OAuthPendingFlow, OAuthSignInPersistence } from "@yielded/auth/OAuth";
import { and, eq } from "drizzle-orm";
import type { EffectSQLiteDoDatabase } from "drizzle-orm/effect-sqlite-do";
import { Effect, Layer, Ref, Schema } from "effect";

import { oauthFlow } from "./oauth-schema";
import type { AppAuth } from "./server";

export const GithubRejectionReason = Schema.Literals([
  "callback-invalid",
  "request-binding-invalid",
  "state-invalid",
  "flow-missing",
  "flow-not-pending",
  "flow-expired",
  "flow-not-yet-valid",
  "flow-configuration-mismatch",
  "state-mismatch",
  "request-binding-mismatch",
  "issuer-unexpected",
  "issuer-mismatch",
  "flow-rejected",
  "after-claim",
]);

export type GithubRejectionReason = typeof GithubRejectionReason.Type;
export type GithubRejectionReporter = (reason: GithubRejectionReason) => Effect.Effect<void>;

export const reportGithubRejection: GithubRejectionReporter = (reason) =>
  Effect.logWarning("auth.github.callback-rejected").pipe(Effect.annotateLogs({ reason }));

const pending = Schema.fromJsonString(OAuthPendingFlow);

type ClaimInput = Parameters<OAuthSignInPersistence["Service"]["claim"]>[0];

/** Read only after a rejected claim has finished. These observations are diagnostic;
 * the adapter remains the sole authority for authorization and durable mutation. */
const describeRejectedClaim = Effect.fn("Auth.describeRejectedClaim")(function* (
  database: EffectSQLiteDoDatabase,
  input: ClaimInput,
): Effect.fn.Return<GithubRejectionReason> {
  return yield* Effect.gen(function* () {
    const rows = yield* database
      .select({ state: oauthFlow.state, snapshot: oauthFlow.snapshot })
      .from(oauthFlow)
      .where(and(eq(oauthFlow.moduleId, input.moduleId), eq(oauthFlow.flowId, input.flowId)));

    const row = rows[0];

    if (!row) return "flow-missing" as const;
    if (row.state !== "Pending" || row.snapshot === null) return "flow-not-pending" as const;
    const flow = yield* Schema.decodeEffect(pending)(row.snapshot);
    const context = flow.context;

    if (context.responseIssuerMode === "unsupported" && input.responseIssuer !== undefined)
      return "issuer-unexpected" as const;
    if (context.responseIssuerMode === "required" && input.responseIssuer !== context.issuer)
      return "issuer-mismatch" as const;
    if (
      context.generation !== input.generation ||
      context.provider !== input.provider ||
      context.callbackId !== input.callbackId
    )
      return "flow-configuration-mismatch" as const;
    if (context.stateDigest !== input.stateDigest) return "state-mismatch" as const;
    if (
      context.requestBindingVerifier !== input.requestBindingVerifier ||
      context.requestBindingExpiresAtMillis !== input.requestBindingExpiresAtMillis
    )
      return "request-binding-mismatch" as const;
    if (input.nowMillis < context.issuedAtMillis) return "flow-not-yet-valid" as const;
    if (input.nowMillis >= context.expiresAtMillis) return "flow-expired" as const;

    return "flow-rejected" as const;
  }).pipe(
    // Telemetry cannot replace the original rejection, including when its read fails.
    Effect.timeout("100 millis"),
    Effect.catchCause(() => Effect.succeed("flow-rejected" as const)),
  );
});

/** Request-local observations contain only fixed reason labels, never identifiers,
 * raw callback values, credential values, digests, or provider response bodies. */
export const makeGithubDiagnostics = Effect.fn("Auth.makeGithubDiagnostics")(function* (
  AppAuth: AppAuth,
  report: GithubRejectionReporter = reportGithubRejection,
) {
  const reason = yield* Ref.make<GithubRejectionReason>("callback-invalid");
  const binding = AppAuth.strategies.github.binding;

  const bindingLayer = Layer.effect(
    binding.RequestBinding,
    Effect.gen(function* () {
      const original = yield* binding.RequestBinding;

      return binding.RequestBinding.of({
        ...original,
        verify: (flowId, credential) =>
          original.verify(flowId, credential).pipe(
            Effect.tap(() => Ref.set(reason, "state-invalid")),
            Effect.tapError(() => Ref.set(reason, "request-binding-invalid")),
          ),
      });
    }),
  ).pipe(Layer.provide(binding.layer));

  return {
    bindingLayer,
    persistence: (original: OAuthSignInPersistence["Service"], database: EffectSQLiteDoDatabase) =>
      OAuthSignInPersistence.of({
        ...original,
        claim: (input, prepare) =>
          Effect.gen(function* () {
            let rejected = false;

            const receipt = yield* original.claim(input, (decision, journal) => {
              rejected = decision._tag !== "Claimed";

              return prepare(decision, journal);
            });

            yield* Ref.set(
              reason,
              rejected ? yield* describeRejectedClaim(database, input) : "after-claim",
            );

            return receipt;
          }),
      }),
    report: Effect.flatMap(Ref.get(reason), report),
  };
});

export type GithubDiagnostics = Effect.Success<ReturnType<typeof makeGithubDiagnostics>>;
