import { Context, DateTime, Effect, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { FundingError, FundingGrant, GrantFunding, RevokeFunding } from "../funding-domain";
import { AccountId } from "./account";

// GitHub's immutable ID for danieljvdm; neither display names nor registration order grant admin.
export const fundingAdminGithubId = "3450486";
const issuer = "https://github.com/login/oauth";

const unavailable = () =>
  new FundingError({ message: "Funding access is unavailable. Refresh before retrying." });

export const FundingFailpoint = Context.Reference<{
  readonly hit: (
    point:
      | "schema:before"
      | "schema:after"
      | "grant:before"
      | "grant:after"
      | "revoke:before"
      | "revoke:after",
  ) => Effect.Effect<void, FundingError>;
}>("travel-planner/FundingFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

const Identity = Schema.Struct({
  subjectId: AccountId,
  displayName: Schema.String,
});

const Values = Schema.Array(Schema.Struct({ value: Schema.String }));

const Grants = Schema.Array(
  Schema.Struct({
    kind: FundingGrant.fields.kind,
    target: FundingGrant.fields.target,
    value: Schema.String,
  }),
);

const grantJson = Schema.fromJsonString(FundingGrant);

const Email = Schema.String.check(
  Schema.isPattern(/^[^\s@]+@[^\s@]+\.[^\s@]+$/),
  Schema.isMaxLength(320),
);

const GithubLogin = Schema.String.check(
  Schema.isPattern(/^[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,37}[a-zA-Z0-9])?$/),
);

const GithubUser = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  login: GithubLogin,
});

/** Auth-owned funding grants refer only to verified, active identities. No credential is returned. */
export const makeFundingStore = Effect.fn("Funding.store")(function* (
  storage: DurableObjectStorage,
) {
  const failpoint = yield* FundingFailpoint;

  const read = <A>(run: () => A) =>
    Effect.try({
      try: run,
      catch: (error) => (Schema.is(FundingError)(error) ? error : unavailable()),
    });

  yield* failpoint.hit("schema:before");
  yield* read(() =>
    storage.transactionSync(() => {
      const tables = storage.sql
        .exec("select name from sqlite_master where type='table' and name like 'funding_%'")
        .toArray();

      if (tables.length) {
        const rows = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ version: Schema.Int })))(
          storage.sql.exec("select version from funding_format").toArray(),
        );

        if (rows.length !== 1 || rows[0]?.version !== 1) throw unavailable();

        return;
      }
      storage.sql.exec(
        "create table funding_grant (kind text not null, target text not null, value text not null, primary key (kind, target))",
      );
      storage.sql.exec("create table funding_format (version integer not null)");
      storage.sql.exec("insert into funding_format values (1)");
    }),
  );
  yield* failpoint.hit("schema:after");

  const identities = (subjectId: string) => {
    const subjects = Schema.decodeUnknownSync(Schema.Array(Identity))(
      storage.sql
        .exec(
          "select id as subjectId, displayName from auth_subject where id = ? and active = 1",
          subjectId,
        )
        .toArray(),
    );

    const subject = subjects[0];

    if (!subject) throw unavailable();

    const emails = Schema.decodeUnknownSync(Values)(
      storage.sql
        .exec(
          `select i.value from auth_identifier i join auth_email_credential e on e.subjectId = i.subjectId and e.namespace = i.namespace and e.value = i.value join auth_credential c on c.subjectId = e.subjectId and c.credentialId = e.credentialId where i.subjectId = ? and i.namespace = 'travel-planner/email' and e.active = 1 and c.active = 1 and c.revision = e.revision`,
          subjectId,
        )
        .toArray(),
    ).map((row) => row.value.toLowerCase());

    const githubIds = Schema.decodeUnknownSync(Values)(
      storage.sql
        .exec(
          `select t.externalSubject as value from auth_oauth_tuple t join auth_oauth_credential o on o.identityKey = t.identityKey and o.subjectId = t.subjectId join auth_credential c on c.subjectId = o.subjectId and c.credentialId = o.credentialId where t.subjectId = ? and t.provider = 'github' and t.issuer = ? and t.state = 'Owned' and o.active = 1 and c.active = 1 and c.revision = o.revision`,
          subjectId,
          issuer,
        )
        .toArray(),
    ).map((row) => row.value);

    return { ...subject, emails, githubIds, admin: githubIds.includes(fundingAdminGithubId) };
  };

  const grants = () =>
    Schema.decodeUnknownSync(Grants)(
      storage.sql
        .exec("select kind, target, value from funding_grant order by kind, target")
        .toArray(),
    ).map((row) => {
      const grant = Schema.decodeSync(grantJson)(row.value);

      if (grant.kind !== row.kind || grant.target !== row.target) throw unavailable();

      return grant;
    });

  const allowed = (identity: ReturnType<typeof identities>, entries: ReadonlyArray<FundingGrant>) =>
    identity.admin ||
    entries.some((grant) =>
      grant.kind === "account"
        ? grant.target === identity.subjectId
        : grant.kind === "github"
          ? identity.githubIds.includes(grant.target)
          : identity.emails.includes(grant.target),
    );

  const status = (subjectId: string) =>
    read(() => {
      const identity = identities(subjectId);

      return { admin: identity.admin, allowed: allowed(identity, grants()) };
    });

  const requireAdmin = (subjectId: string) =>
    read(() => {
      if (!identities(subjectId).admin)
        throw new FundingError({ message: "Administrator access required." });
    });

  const list = Effect.fn("Funding.list")(function* (actor: string, after?: string) {
    yield* requireAdmin(actor);

    return yield* read(() => {
      const rows = Schema.decodeUnknownSync(Schema.Array(Identity))(
        storage.sql
          .exec(
            "select id as subjectId, displayName from auth_subject where active = 1 and id > ? order by id limit 26",
            after ?? "",
          )
          .toArray(),
      );

      const entries = grants();

      return {
        grants: entries,
        users: rows.slice(0, 25).map((row) => {
          const identity = identities(row.subjectId);

          return { ...identity, allowed: allowed(identity, entries) };
        }),
        next: rows.length > 25 ? rows[24]!.subjectId : null,
      };
    });
  });

  const grant = Effect.fn("Funding.grant")(function* (
    actor: string,
    input: typeof GrantFunding.Type,
  ) {
    yield* requireAdmin(actor);

    const candidate = yield* Schema.decodeEffect(GrantFunding)(input).pipe(
      Effect.mapError(() => new FundingError({ message: "Enter a valid funding recipient." })),
    );

    let target = candidate.value.trim();
    let label = target;

    if (candidate.kind === "email") {
      target = yield* Schema.decodeEffect(Email)(target.toLowerCase()).pipe(
        Effect.mapError(() => new FundingError({ message: "Enter a valid email address." })),
      );
      label = target;
    } else if (candidate.kind === "github") {
      const login = yield* Schema.decodeEffect(GithubLogin)(target.replace(/^@/, "")).pipe(
        Effect.mapError(() => new FundingError({ message: "Enter a GitHub username." })),
      );

      const user = yield* Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient;

        const response = yield* HttpClient.withScope(http).execute(
          HttpClientRequest.get(`https://api.github.com/users/${encodeURIComponent(login)}`).pipe(
            HttpClientRequest.setHeaders({
              "user-agent": "Elsewhere-funding",
              accept: "application/vnd.github+json",
            }),
          ),
        );

        if (response.status !== 200)
          return yield* new FundingError({
            message: "Could not find that GitHub user. Check the username and retry.",
          });

        return yield* response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(GithubUser)));
      }).pipe(
        Effect.scoped,
        Effect.timeout("10 seconds"),
        Effect.mapError(
          () =>
            new FundingError({
              message: "Could not verify that GitHub user. Check the username and retry.",
            }),
        ),
        Effect.provide(FetchHttpClient.layer),
      );

      target = String(user.id);
      label = user.login;
    } else {
      target = yield* Schema.decodeEffect(AccountId)(target).pipe(Effect.mapError(unavailable));
      label = (yield* read(() => identities(target))).displayName;
    }

    const entry = yield* Schema.decodeEffect(FundingGrant)({
      version: 1,
      kind: candidate.kind,
      target,
      label,
      grantedBy: actor,
      grantedAt: DateTime.formatIso(yield* DateTime.now),
    }).pipe(Effect.mapError(unavailable));

    const encoded = yield* Schema.encodeEffect(grantJson)(entry).pipe(Effect.mapError(unavailable));

    yield* failpoint.hit("grant:before");
    yield* read(() =>
      storage.transactionSync(() => {
        if (!identities(actor).admin) throw unavailable();
        // Decode existing data before mutation; unknown formats never get overwritten.
        const entries = grants();

        if (
          entries.length >= 1000 &&
          !entries.some((row) => row.kind === entry.kind && row.target === entry.target)
        )
          throw new FundingError({ message: "The funding allowlist is full." });
        storage.sql.exec(
          "insert into funding_grant (kind,target,value) values (?,?,?) on conflict(kind,target) do update set value=excluded.value",
          entry.kind,
          entry.target,
          encoded,
        );
      }),
    );
    yield* failpoint.hit("grant:after");

    return entry;
  });

  const revoke = Effect.fn("Funding.revoke")(function* (
    actor: string,
    input: typeof RevokeFunding.Type,
  ) {
    yield* requireAdmin(actor);

    const entry = yield* Schema.decodeEffect(RevokeFunding)(input).pipe(
      Effect.mapError(unavailable),
    );

    yield* failpoint.hit("revoke:before");
    yield* read(() =>
      storage.transactionSync(() => {
        if (!identities(actor).admin) throw unavailable();
        grants();
        storage.sql.exec(
          "delete from funding_grant where kind = ? and target = ?",
          entry.kind,
          entry.target,
        );
      }),
    );
    yield* failpoint.hit("revoke:after");
  });

  return { status, list, grant, revoke };
});
