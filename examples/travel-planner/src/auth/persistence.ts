import * as Drizzle from "@yielded/auth/DrizzleSqliteDo";
import { EmailSignInTargets, EmailUnavailable } from "@yielded/auth/Email";
import {
  OAuthSignInPersistence,
  OAuthRegistrationIntents,
  OAuthUnavailable,
} from "@yielded/auth/OAuth";
import { ProofPersistence } from "@yielded/auth/Proofs";
import { AuthenticationAuthority } from "@yielded/auth/Sessions";
import { eq } from "drizzle-orm";
import type { EffectSQLiteDoDatabase } from "drizzle-orm/effect-sqlite-do";
import { Effect, Layer } from "effect";

import { emailSignInMapping, emailRegistrationMapping } from "./email-schema";
import type { GithubDiagnostics } from "./oauth-diagnostics";
import { oauthSignInMapping, oauthIntentMapping, oauthRegistrationMapping } from "./oauth-schema";
import { proofs } from "./proof-schema";
import { subject, subjectMapping, subjectId, credentialMapping } from "./schema";
import type { AppAuth } from "./server";
import { sessionsMapping } from "./session-schema";

export const persistenceLayer = (
  AppAuth: AppAuth,
  database: EffectSQLiteDoDatabase,
  diagnostics: GithubDiagnostics,
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const proof = yield* Drizzle.makeProofPersistenceServices(database, proofs);
      const email = yield* Drizzle.makeEmailSignInServices(database, emailSignInMapping);

      const emailRegistration = yield* Drizzle.makeEmailRegistrationServices(
        database,
        emailRegistrationMapping,
        proofs,
      );

      const oauth = yield* Drizzle.makeOAuthSignInServices(database, oauthSignInMapping);

      const intents = yield* Drizzle.makeOAuthRegistrationIntentServices(
        database,
        oauthIntentMapping,
      );

      const registration = yield* Drizzle.makeOAuthRegistrationServices(
        database,
        oauthRegistrationMapping,
      );

      const sessions = yield* Drizzle.makeStatefulSessionServices(database, sessionsMapping);

      const authority = yield* Drizzle.makeAuthenticationAuthorityServices(database, {
        subject: subjectMapping,
        credential: credentialMapping,
        subjectId,
        isConstraintConflict: () => false,
      });

      const claims = Effect.fn("Auth.claims")(function* (id: string) {
        const rows = yield* database
          .select({ displayName: subject.displayName })
          .from(subject)
          .where(eq(subject.id, id));

        if (rows.length !== 1) return yield* Effect.fail("Missing account" as const);

        return rows[0]!;
      });

      return Layer.mergeAll(
        Layer.succeed(ProofPersistence, proof.proofPersistence),
        Layer.succeed(EmailSignInTargets, email.emailSignInTargets),
        Layer.succeed(
          AppAuth.strategies.emailRegistration.RegistrationAuthority,
          emailRegistration.registrationAuthority,
        ),
        Layer.succeed(AppAuth.strategies.email.ClaimsForEmail, {
          resolve: (credential) =>
            claims(credential.revision.subjectId).pipe(
              Effect.mapError(() => EmailUnavailable.make({})),
            ),
        }),
        Layer.succeed(AppAuth.strategies.github.ClaimsForOAuth, {
          resolve: (credential) =>
            claims(credential.revision.subjectId).pipe(
              Effect.mapError(() => OAuthUnavailable.make({})),
            ),
        }),
        Layer.succeed(
          OAuthSignInPersistence,
          diagnostics.persistence(oauth.oauthSignInPersistence, database),
        ),
        Layer.succeed(OAuthRegistrationIntents, intents.oauthRegistrationIntents),
        Layer.succeed(
          AppAuth.strategies.github.registration.RegistrationAuthority,
          registration.registrationAuthority,
        ),
        Layer.succeed(
          AppAuth.sessions.StatefulSessionPersistence,
          sessions.statefulSessionPersistence,
        ),
        Layer.succeed(AppAuth.sessions.SessionRepository, sessions.sessionRepository),
        Layer.succeed(AuthenticationAuthority, authority.authenticationAuthority),
      );
    }),
  );
