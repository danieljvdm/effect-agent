import { DateTime, Effect, Schema } from "effect";
import {
  createRemoteJWKSet,
  customFetch,
  jwksCache,
  jwtVerify,
  type JWKSCacheInput,
  type JWTVerifyGetKey,
} from "jose";

import { AccessError, AccessSession, adminEmail, Email } from "../access-domain.ts";

export interface AccessEnvironment {
  readonly ACCESS_TEAM_DOMAIN?: string;
  readonly ACCESS_AUD?: string;
}

const configuration = Schema.Struct({
  ACCESS_TEAM_DOMAIN: Schema.String.check(
    Schema.isMaxLength(256),
    Schema.isPattern(/^https:\/\/[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cloudflareaccess\.com\/?$/),
  ),
  ACCESS_AUD: Schema.String.check(Schema.isMaxLength(256), Schema.isPattern(/^\S+$/)),
});

const nonemptyString = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const timestamp = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

const claims = Schema.Struct({
  iss: nonemptyString,
  aud: Schema.Union([nonemptyString, Schema.Array(nonemptyString)]),
  sub: nonemptyString,
  iat: timestamp,
  exp: timestamp,
  email: Email,
});

const assertion = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(16_384));

const unauthorized = () =>
  new AccessError({
    code: "unauthorized",
    message: "A valid Cloudflare Access session is required.",
  });

/** The resolver seam substitutes signing keys only; signature and claim checks always run. */
export const makeAuthenticate = (
  resolveKey: (certsUrl: URL, signal: AbortSignal) => JWTVerifyGetKey,
) =>
  Effect.fn("Access.authenticate")(function* (
    request: Request,
    env: AccessEnvironment,
  ): Effect.fn.Return<AccessSession, AccessError> {
    const configured = yield* Schema.decodeUnknownEffect(configuration)(env).pipe(
      Effect.mapError(
        () =>
          new AccessError({ code: "unavailable", message: "Cloudflare Access is not configured." }),
      ),
    );

    const token = yield* Schema.decodeUnknownEffect(assertion)(
      request.headers.get("Cf-Access-Jwt-Assertion"),
    ).pipe(Effect.mapError(unauthorized));

    const issuer = new URL(configured.ACCESS_TEAM_DOMAIN).origin;
    const now = yield* DateTime.now;

    const verified = yield* Effect.tryPromise({
      try: (signal) =>
        jwtVerify(token, resolveKey(new URL(`${issuer}/cdn-cgi/access/certs`), signal), {
          algorithms: ["RS256"],
          issuer,
          audience: configured.ACCESS_AUD,
          requiredClaims: ["exp", "iat", "sub", "email"],
          currentDate: DateTime.toDateUtc(now),
        }),
      catch: unauthorized,
    });

    const identity = yield* Schema.decodeUnknownEffect(claims)(verified.payload).pipe(
      Effect.mapError(unauthorized),
    );

    // JOSE validates expiry/nbf; require a meaningful, non-future issuance time too.
    if (
      identity.iat > Math.floor(DateTime.toEpochMillis(now) / 1_000) ||
      identity.exp <= identity.iat
    )
      return yield* unauthorized();
    const email = identity.email.toLowerCase();

    return yield* Schema.decodeUnknownEffect(AccessSession)({
      email,
      isAdmin: email === adminEmail,
    }).pipe(Effect.mapError(unauthorized));
  });

// Only public keys and their freshness timestamp survive requests. In-flight fetches,
// JWTs and authenticated identities belong to each request, including cancellation.
let publicKeys: { readonly url: string; readonly cache: JWKSCacheInput } | undefined;

export const authenticate = makeAuthenticate((url, signal) => {
  if (publicKeys?.url !== url.href) publicKeys = { url: url.href, cache: {} };

  return createRemoteJWKSet(url, {
    [jwksCache]: publicKeys.cache,
    timeoutDuration: 5_000,
    cacheMaxAge: 600_000,
    cooldownDuration: 30_000,
    [customFetch]: (resource, options) =>
      fetch(resource, { ...options, signal: AbortSignal.any([options.signal, signal]) }),
  });
});
