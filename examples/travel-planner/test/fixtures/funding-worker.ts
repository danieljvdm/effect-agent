import { DurableObject } from "cloudflare:workers";
import { Effect, Redacted, Schema } from "effect";

import { FundingFailpoint, makeFundingStore } from "../../src/auth/funding";
import { initializeAuthStorage } from "../../src/auth/storage";
import { FundingError, GrantFunding, RevokeFunding } from "../../src/funding-domain";
import { credentialForOwner, credentialSourceLayer } from "../../src/server/credentials";

const adminId = "00000000-0000-0000-0000-000000000001";
const emailId = "00000000-0000-0000-0000-000000000002";
const githubId = "00000000-0000-0000-0000-000000000003";
const impostorId = "00000000-0000-0000-0000-000000000004";

export class FundingFixture extends DurableObject {
  fetch(request: Request) {
    const url = new URL(request.url);
    const actor = url.searchParams.get("actor") ?? adminId;

    return Effect.runPromise(
      Effect.gen({ self: this }, function* () {
        yield* initializeAuthStorage(this.ctx.storage);
        const sql = this.ctx.storage.sql;

        if (url.pathname === "/seed") {
          this.ctx.storage.transactionSync(() => {
            for (const [id, name] of [
              [adminId, "Dan"],
              [emailId, "Email reader"],
              [githubId, "GitHub reader"],
              [impostorId, "danieljvdm"],
            ]) {
              sql.exec("insert into auth_subject values (?,1,?,?)", id, "revision", name);
              sql.exec("insert into auth_credential values (?,?,?,1)", id, id, "revision");
            }
            for (const [id, github] of [
              [adminId, "3450486"],
              [githubId, "424242"],
            ]) {
              sql.exec(
                "insert into auth_oauth_tuple (identityKey,provider,issuer,externalSubject,state,version,subjectId) values (?,'github','https://github.com/login/oauth',?,'Owned','v1',?)",
                id,
                github,
                id,
              );
              sql.exec(
                "insert into auth_oauth_credential (moduleId,credentialId,subjectId,identityKey,revision,active) values ('travel-planner/github',?,?,?,'revision',1)",
                id,
                id,
                id,
              );
            }
            sql.exec(
              "insert into auth_identifier values ('travel-planner/email','reader@gmail.com',?,1,'revision')",
              emailId,
            );
            sql.exec(
              "insert into auth_email_credential values ('travel-planner/email',?,?,'travel-planner/email','reader@gmail.com','revision',1)",
              emailId,
              emailId,
            );
          });
        }
        if (url.pathname === "/corrupt")
          sql.exec("update funding_grant set value = ?", '{"version":99}');
        if (url.pathname === "/unsupported") sql.exec("update funding_format set version = 99");
        if (url.pathname === "/mismatch")
          sql.exec("update funding_grant set target = 'mismatched'");
        if (url.pathname === "/disable")
          sql.exec("update auth_credential set active = 0 where subjectId = ?", actor);
        const store = yield* makeFundingStore(this.ctx.storage);

        if (url.pathname === "/grant") {
          const input = yield* Effect.promise(() => request.json()).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(GrantFunding)),
          );

          return Response.json(yield* store.grant(actor, input));
        }
        if (url.pathname === "/revoke") {
          const input = yield* Effect.promise(() => request.json()).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(RevokeFunding)),
          );

          yield* store.revoke(actor, input);

          return Response.json({ revoked: true });
        }
        if (url.pathname === "/list") return Response.json(yield* store.list(actor));

        return Response.json(yield* store.status(actor));
      }).pipe(
        Effect.provideService(FundingFailpoint, {
          hit: (point) => {
            if (point !== url.searchParams.get("point")) return Effect.void;
            switch (url.searchParams.get("mode")) {
              case "defect":
                return Effect.die("Injected fault");
              case "interrupt":
                return Effect.interrupt;
              case "timeout":
                return Effect.never;
              default:
                return Effect.fail(new FundingError({ message: "Injected lost response" }));
            }
          },
        }),
        Effect.timeout("100 millis"),
        Effect.catchCause(() =>
          Effect.succeed(Response.json({ error: "Rejected" }, { status: 400 })),
        ),
      ),
    );
  }
}

export default {
  async fetch(request: Request, env: { STORE: DurableObjectNamespace }) {
    const url = new URL(request.url);
    const name = url.searchParams.get("store") ?? "test";

    if (url.pathname === "/resolve") {
      const actor = url.searchParams.get("actor") ?? emailId;

      return Effect.runPromise(
        credentialForOwner(`account-${actor}`).pipe(
          Effect.provide(
            credentialSourceLayer({
              SERVER_OPENAI_KEY: "sk-fixture-server-key-9876",
              ACCOUNT_THREADS: { getByName: () => ({ modelCredential: async () => "null" }) },
              AUTH: {
                getByName: () => ({
                  fetch: (internal) => {
                    const id = new URL(internal.url).pathname.split("/").at(-1);

                    return env.STORE.getByName(name).fetch(
                      new Request(`https://fixture/status?actor=${id}`),
                    );
                  },
                }),
              },
            }),
          ),
          Effect.match({
            onSuccess: (key) => Response.json({ lastFour: Redacted.value(key).slice(-4) }),
            onFailure: () => Response.json({ denied: true }, { status: 403 }),
          }),
        ),
      );
    }

    return env.STORE.getByName(name).fetch(request);
  },
};
