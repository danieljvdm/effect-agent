import * as Auth from "@yielded/auth/Auth";
import * as Email from "@yielded/auth/Email";
import { gitHubOAuthAppProtocolLayer } from "@yielded/auth/GitHub";
import * as AuthHttp from "@yielded/auth/Http";
import * as OAuth from "@yielded/auth/OAuth";
import * as Sessions from "@yielded/auth/Sessions";
import { Layer, Redacted, Schema } from "effect";

import { LoginApi, Registration } from "./contract";

export const AuthConfiguration = Schema.Struct({
  AUTH_ORIGIN: Schema.String.check(
    Schema.makeFilter((value) => {
      try {
        const url = new URL(value);

        return url.protocol === "https:" && url.origin === value;
      } catch {
        return false;
      }
    }),
  ),
  AUTH_BINDING_KEY: Schema.NonEmptyString,
  AUTH_PROOF_KEY: Schema.NonEmptyString,
  AUTH_TRANSACTION_KEY: Schema.NonEmptyString,
  AUTH_GITHUB_CLIENT_ID: Schema.NonEmptyString,
  AUTH_GITHUB_CLIENT_SECRET: Schema.NonEmptyString,
  AUTH_EMAIL_FROM: Schema.NonEmptyString,
});

export type AuthConfiguration = typeof AuthConfiguration.Type;

export const keyring = (material: string) => ({
  activeKeyId: "v1",
  keys: [{ id: "v1", material: Redacted.make(material) }],
});

export const makeAuth = (config: AuthConfiguration) => {
  const email = {
    namespace: "travel-planner/email",
    template: "elsewhere-code",
    digits: 6,
    keys: keyring(config.AUTH_PROOF_KEY),
    policy: {
      lifetimeMillis: 300_000,
      continuationLifetimeMillis: 60_000,
      maximumFailedAttempts: 5,
      maximumDeliveryAttempts: 1,
      deliveryClaimMillis: 15_000,
      deliveryRetryMillis: 30_000,
      requestRetentionMillis: 3_600_000,
      abuse: {
        issues: { limit: 5, windowMillis: 3_600_000 },
        attempts: { limit: 10, windowMillis: 300_000 },
        subjectIssues: { limit: 5, windowMillis: 3_600_000 },
        subjectAttempts: { limit: 10, windowMillis: 300_000 },
        actionIssues: { limit: 1000, windowMillis: 3_600_000 },
        actionAttempts: { limit: 1000, windowMillis: 300_000 },
        resendCooldownMillis: 30_000,
      },
    },
  } as const;

  const AppAuth = Auth.make(LoginApi, {
    sessions: Sessions.stateful({ idleTimeout: "30 days", maxAge: "30 days", renewAfter: "1 day" }),
    strategies: {
      email: Email.makeCode(email),
      emailRegistration: Email.makeRegistration({ ...email, registration: Registration }),
      github: OAuth.makeRegistration({
        namespace: "travel-planner/github",
        registration: Registration,
        policy: {
          generation: 1,
          lifetimeMillis: 300_000,
          claimLifetimeMillis: 30_000,
          settlementTimeoutMillis: 5000,
          retentionMillis: 600_000,
        },
        registrationPolicy: {
          lifetimeMillis: 300_000,
          maximumVerificationAgeMillis: 300_000,
          retentionMillis: 600_000,
        },
      }),
    },
    defaultStrategy: "github",
  });

  const http = AuthHttp.make(AppAuth, {
    origin: config.AUTH_ORIGIN,
    maximumBodyBytes: 32 * 1024,
    cookie: { prefix: "__Host-elsewhere-auth-" },
  });

  const security = Layer.mergeAll(
    Auth.RequestBindingConfig.layer({
      generation: 1,
      lifetimeMillis: 600_000,
      keyring: keyring(config.AUTH_BINDING_KEY),
    }),
    OAuth.OAuthTransactionProtector.xchacha20poly1305(keyring(config.AUTH_TRANSACTION_KEY)),
    OAuth.OAuthReturnTargets.exactRoutes(["/"]),
    Email.EmailReturnTargets.exactRoutes(["/"]),
  );

  const github = gitHubOAuthAppProtocolLayer({
    registrations: [
      {
        configurationGeneration: 2,
        issuance: "active",
        clientId: config.AUTH_GITHUB_CLIENT_ID,
        clientSecret: Redacted.make(config.AUTH_GITHUB_CLIENT_SECRET),
        callbacks: [
          {
            callbackId: OAuth.OAuthCallbackId.make("github"),
            redirectUri: OAuth.OAuthRedirectUri.make(`${config.AUTH_ORIGIN}/auth/github/callback`),
          },
        ],
      },
    ],
    timeoutSeconds: 10,
  });

  return { AppAuth, http, security, github };
};

export type AppAuth = ReturnType<typeof makeAuth>["AppAuth"];
