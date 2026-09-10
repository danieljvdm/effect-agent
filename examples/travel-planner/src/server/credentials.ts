import { Context, DateTime, Effect, Layer, Redacted, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { type OpenAiConnection } from "../credential-domain.ts";
import { PlannerError } from "../domain.ts";
import { ownerOfThread } from "./tenancy.ts";

export interface CredentialEnvironment {
  readonly BYOK_ENCRYPTION_KEY?: string;
}

export interface CredentialHost extends CredentialEnvironment {
  readonly THREADS: {
    readonly getByName: (owner: string) => { readonly modelCredential: () => Promise<string> };
  };
}

const Base64 = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9+/]+={0,2}$/));

export const SealedCredential = Schema.Struct({
  version: Schema.Literal(1),
  iv: Base64,
  ciphertext: Base64,
  lastFour: Schema.String.check(Schema.isMinLength(4), Schema.isMaxLength(4)),
  updatedAt: Schema.String,
});

const StoredCredential = Schema.NullOr(SealedCredential);
const Rows = Schema.Array(Schema.Struct({ value: Schema.String }));
const ApiKey = Schema.String.check(Schema.isPattern(/^[\x21-\x7e]{16,512}$/));
const empty: OpenAiConnection = { connected: false, lastFour: null, updatedAt: null };

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "Your OpenAI connection is unavailable. Refresh before retrying.",
  });

export const missingKey = () =>
  new PlannerError({
    code: "invalid",
    message: "Connect your OpenAI API key in Settings to continue planning.",
  });

const storageError = () =>
  new PlannerError({
    code: "storage",
    message:
      "Your OpenAI connection contains unreadable or unsupported data. No changes were made.",
  });

const bytes = (value: string) => Uint8Array.from(atob(value), (char) => char.charCodeAt(0));
const base64 = (value: Uint8Array) => btoa(String.fromCharCode(...value));

const encryptionKey = (env: CredentialEnvironment) =>
  Effect.tryPromise({
    try: async () => {
      const raw = bytes(env.BYOK_ENCRYPTION_KEY ?? "");

      if (raw.byteLength !== 32) throw new Error("Invalid encryption configuration");

      return crypto.subtle.importKey("raw", raw, "AES-GCM", false, ["encrypt", "decrypt"]);
    },
    catch: unavailable,
  });

const aad = (owner: string) => new TextEncoder().encode(`travel-planner:openai:v1:${owner}`);

export const decryptCredential = Effect.fn("decryptCredential")(function* (
  env: CredentialEnvironment,
  owner: string,
  sealed: typeof SealedCredential.Type,
) {
  const key = yield* encryptionKey(env);

  return yield* Effect.tryPromise({
    try: async () =>
      Redacted.make(
        new TextDecoder().decode(
          await crypto.subtle.decrypt(
            {
              name: "AES-GCM",
              iv: bytes(sealed.iv),
              additionalData: aad(owner),
            },
            key,
            bytes(sealed.ciphertext),
          ),
        ),
      ),
    catch: unavailable,
  });
});

/** No key or provider response body enters application diagnostics. This request incurs no inference. */
export const validateOpenAiKey = Effect.fn("validateOpenAiKey")(
  function* (candidate: Redacted.Redacted<string>) {
    const raw = Redacted.value(candidate).trim();

    if (!Schema.is(ApiKey)(raw))
      return yield* new PlannerError({ code: "invalid", message: "Enter a valid OpenAI API key." });
    const http = yield* HttpClient.HttpClient;

    const response = yield* HttpClient.withScope(http)
      .execute(
        HttpClientRequest.get("https://api.openai.com/v1/models").pipe(
          HttpClientRequest.bearerToken(raw),
        ),
      )
      .pipe(
        Effect.timeout("15 seconds"),
        Effect.mapError(
          () =>
            new PlannerError({
              code: "unavailable",
              message:
                "Couldn't verify your key with OpenAI. Try again; your previous key is unchanged.",
            }),
        ),
      );

    if (response.status === 401 || response.status === 403)
      return yield* new PlannerError({
        code: "invalid",
        message: "OpenAI rejected this key. Check that it is active and has model access.",
      });
    if (response.status !== 200)
      return yield* new PlannerError({
        code: "unavailable",
        message:
          "OpenAI couldn't verify the key right now. Try again; your previous key is unchanged.",
      });

    return Redacted.make(raw);
  },
  Effect.scoped,
  Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
);

export const CredentialFailpoint = Context.Reference<{
  readonly hit: (
    point:
      | "schema:before"
      | "schema:after"
      | "save:before"
      | "save:after"
      | "remove:before"
      | "remove:after",
  ) => Effect.Effect<void, PlannerError>;
}>("travel-planner/CredentialFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

export class CredentialStore extends Context.Service<
  CredentialStore,
  {
    readonly sealed: Effect.Effect<typeof StoredCredential.Type, PlannerError>;
    readonly status: Effect.Effect<OpenAiConnection, PlannerError>;
    readonly save: (
      key: Redacted.Redacted<string>,
    ) => Effect.Effect<OpenAiConnection, PlannerError>;
    readonly remove: Effect.Effect<OpenAiConnection, PlannerError>;
  }
>()("travel-planner/CredentialStore") {}

/** A separate mutable secret row, never a journal record, trip field, or model prompt. */
export const credentialStoreLayer = (env: CredentialEnvironment, threadId: string) =>
  Layer.effect(
    CredentialStore,
    Effect.gen(function* () {
      const sql = yield* SqlClient;
      const failpoint = yield* CredentialFailpoint;
      const owner = ownerOfThread(threadId);

      const requireOwner = Effect.suspend(() =>
        threadId === owner ? Effect.void : Effect.fail(unavailable()),
      );

      yield* failpoint.hit("schema:before");
      yield* sql`CREATE TABLE IF NOT EXISTS travel_model_credentials (id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL)`.pipe(
        Effect.mapError(storageError),
      );
      yield* failpoint.hit("schema:after");

      const sealed = Effect.gen(function* () {
        yield* requireOwner;

        const rows = yield* sql`SELECT value FROM travel_model_credentials WHERE id = 1`.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
          Effect.mapError(storageError),
        );

        return rows[0] === undefined
          ? null
          : yield* Schema.decodeUnknownEffect(Schema.fromJsonString(SealedCredential))(
              rows[0].value,
            ).pipe(Effect.mapError(storageError));
      });

      const status = sealed.pipe(
        Effect.map((value): OpenAiConnection =>
          value === null
            ? empty
            : { connected: true, lastFour: value.lastFour, updatedAt: value.updatedAt },
        ),
      );

      const save = Effect.fn("CredentialStore.save")(function* (
        candidate: Redacted.Redacted<string>,
      ) {
        yield* requireOwner;
        yield* sealed;
        const raw = Redacted.value(candidate);

        if (!Schema.is(ApiKey)(raw)) return yield* missingKey();
        const key = yield* encryptionKey(env);
        const iv = yield* Effect.sync(() => crypto.getRandomValues(new Uint8Array(12)));

        const ciphertext = yield* Effect.tryPromise({
          try: () =>
            crypto.subtle.encrypt(
              { name: "AES-GCM", iv, additionalData: aad(owner) },
              key,
              new TextEncoder().encode(raw),
            ),
          catch: unavailable,
        });

        const updatedAt = DateTime.formatIso(yield* DateTime.now);

        const record = {
          version: 1 as const,
          iv: base64(iv),
          ciphertext: base64(new Uint8Array(ciphertext)),
          lastFour: raw.slice(-4),
          updatedAt,
        };

        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(SealedCredential))(
          record,
        ).pipe(Effect.mapError(storageError));

        yield* failpoint.hit("save:before");
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sealed;
              yield* sql`INSERT INTO travel_model_credentials (id, value) VALUES (1, ${encoded}) ON CONFLICT (id) DO UPDATE SET value = excluded.value`;
            }),
          )
          .pipe(Effect.catchTag("SqlError", storageError));
        yield* failpoint.hit("save:after");

        return { connected: true, lastFour: record.lastFour, updatedAt };
      });

      const remove = Effect.gen(function* () {
        yield* requireOwner;
        yield* failpoint.hit("remove:before");
        yield* sql
          .withTransaction(
            Effect.gen(function* () {
              yield* sealed;
              yield* sql`DELETE FROM travel_model_credentials WHERE id = 1`;
            }),
          )
          .pipe(Effect.catchTag("SqlError", storageError));
        yield* failpoint.hit("remove:after");

        return empty;
      });

      return { sealed, status, save, remove };
    }),
  );

/** Private namespace RPC returns ciphertext only. The model host resolves the verified task owner. */
export const credentialForOwner = Effect.fn("credentialForOwner")(function* (
  env: CredentialHost,
  owner: string,
) {
  const encoded = yield* Effect.tryPromise({
    try: () => env.THREADS.getByName(owner).modelCredential(),
    catch: unavailable,
  });

  const sealed = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredCredential))(
    encoded,
  ).pipe(Effect.mapError(unavailable));

  if (sealed === null) return yield* missingKey();

  return yield* decryptCredential(env, owner, sealed);
});

export const encodeStoredCredential = Schema.encodeEffect(Schema.fromJsonString(StoredCredential));
