import { Context, Effect, Encoding, FileSystem, Layer, Redacted, Schema } from "effect";
import { HttpClient, HttpIncomingMessage, HttpClientRequest } from "effect/unstable/http";

import { makeEventSource, type EventSource } from "./event-source.ts";
import type { Principal } from "./ledger.ts";
import type { EventAcknowledgement } from "./subscription.ts";
import { SubscriptionSourceError } from "./subscription.ts";
import { SubscriptionIntake } from "./subscriptions.ts";

const PositiveId = Schema.Int.check(Schema.isGreaterThan(0));

const GitHubName = Schema.NonEmptyString.check(
  Schema.isMaxLength(100),
  Schema.isPattern(/^[A-Za-z0-9_.-]+$/),
);

const GitCommitSha = Schema.String.check(Schema.isPattern(/^(?:[0-9a-f]{40}|[0-9a-f]{64})$/));
const GitHubConclusion = Schema.NonEmptyString.check(Schema.isMaxLength(64));
const GitHubStatus = Schema.NonEmptyString.check(Schema.isMaxLength(32));

const strict = <S extends Schema.Top>(schema: S) =>
  schema.pipe(Schema.annotate({ parseOptions: { onExcessProperty: "error" } }));

/** One repository whose identity and credentials are selected by the host. */
export const GitHubRepository = strict(
  Schema.Struct({
    id: PositiveId,
    owner: GitHubName,
    name: GitHubName,
  }),
);

export type GitHubRepository = typeof GitHubRepository.Type;

/** Exact workflow attempt requested by an Agent through a host-permitted source. */
export const GitHubWorkflowRunWatch = strict(
  Schema.Struct({
    runId: PositiveId,
    attempt: PositiveId,
    expectedHeadSha: GitCommitSha,
  }),
);

export type GitHubWorkflowRunWatch = typeof GitHubWorkflowRunWatch.Type;

/**
 * Small canonical completion shared by webhook intake and REST reconciliation. Provider delivery
 * IDs, URLs, actors, and mutable metadata are deliberately omitted from durable event identity.
 */
export const GitHubWorkflowRunCompletion = strict(
  Schema.Struct({
    repositoryId: PositiveId,
    runId: PositiveId,
    attempt: PositiveId,
    headSha: GitCommitSha,
    conclusion: GitHubConclusion,
  }),
);

export type GitHubWorkflowRunCompletion = typeof GitHubWorkflowRunCompletion.Type;

// Provider wire payloads intentionally project the fields we own. GitHub adds many fields and
// may add more; canonical values below remain strict after this projection.
const GitHubWorkflowRunWire = Schema.Struct({
  id: PositiveId,
  run_attempt: PositiveId,
  head_sha: GitCommitSha,
  status: GitHubStatus,
  conclusion: Schema.NullOr(GitHubConclusion),
  repository: Schema.Struct({ id: PositiveId }),
});

export const GitHubWorkflowRunWebhook = Schema.Struct({
  action: Schema.Literal("completed"),
  repository: Schema.Struct({ id: PositiveId }),
  workflow_run: GitHubWorkflowRunWire,
});

export type GitHubWorkflowRunWebhook = typeof GitHubWorkflowRunWebhook.Type;

export const GitHubWorkflowRunAttempt = GitHubWorkflowRunWire;
export type GitHubWorkflowRunAttempt = typeof GitHubWorkflowRunAttempt.Type;

export const GitHubWorkflowRunSourceVersion = {
  name: "github-workflow-run-completed",
  version: "1",
} as const;

export class GitHubWorkflowRunsError extends Schema.TaggedError<GitHubWorkflowRunsError>()(
  "GitHubWorkflowRunsError",
  {
    reason: Schema.Literals([
      "rate-limited",
      "unavailable",
      "unauthorized",
      "not-found",
      "invalid-response",
    ]),
    retryable: Schema.Boolean,
  },
) {}

export interface GitHubWorkflowRunAttemptRequest {
  readonly repository: GitHubRepository;
  readonly runId: number;
  readonly attempt: number;
}

/**
 * Credentialed, read-only GitHub boundary. Implementations must authorize the repository and read
 * the exact attempt endpoint; the source never receives or persists a credential.
 */
export class GitHubWorkflowRuns extends Context.Service<
  GitHubWorkflowRuns,
  {
    readonly getAttempt: (
      request: GitHubWorkflowRunAttemptRequest,
    ) => Effect.Effect<GitHubWorkflowRunAttempt, GitHubWorkflowRunsError>;
  }
>()("@effect-agent/thread/GitHubWorkflowRuns") {}

export interface GitHubWorkflowRunsHttpOptions {
  /** Repository this credentialed adapter is authorized to read. */
  readonly repository: GitHubRepository;
  readonly token: Redacted.Redacted<string>;
  readonly apiUrl?: string | undefined;
  readonly userAgent?: string | undefined;
}

const httpFailure = (
  reason: GitHubWorkflowRunsError["reason"],
  retryable: boolean,
): GitHubWorkflowRunsError => GitHubWorkflowRunsError.make({ reason, retryable });

/**
 * GitHub REST adapter for `GET /repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{attempt}`.
 * It performs one bounded read. Scheduling retries and backoff remain owned by SubscriptionDriver.
 */
export const githubWorkflowRunsHttpLayer = (
  options: GitHubWorkflowRunsHttpOptions,
): Layer.Layer<GitHubWorkflowRuns, GitHubWorkflowRunsError, HttpClient.HttpClient> =>
  Layer.effect(
    GitHubWorkflowRuns,
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;

      const permittedRepository = yield* Schema.decodeUnknownEffect(GitHubRepository)(
        options.repository,
      ).pipe(Effect.mapError(() => httpFailure("invalid-response", false)));

      const apiUrl = (options.apiUrl ?? "https://api.github.com").replace(/\/$/, "");

      const getAttempt = Effect.fn("GitHubWorkflowRuns.getAttempt")(function* (
        request: GitHubWorkflowRunAttemptRequest,
      ) {
        const repository = yield* Schema.decodeUnknownEffect(GitHubRepository)(
          request.repository,
        ).pipe(Effect.mapError(() => httpFailure("invalid-response", false)));

        if (
          repository.id !== permittedRepository.id ||
          repository.owner !== permittedRepository.owner ||
          repository.name !== permittedRepository.name
        ) {
          return yield* httpFailure("unauthorized", false);
        }

        const watch = yield* Schema.decodeUnknownEffect(GitHubWorkflowRunWatch)({
          runId: request.runId,
          attempt: request.attempt,
          expectedHeadSha: "0".repeat(40),
        }).pipe(Effect.mapError(() => httpFailure("invalid-response", false)));

        const url = `${apiUrl}/repos/${repository.owner}/${repository.name}/actions/runs/${String(watch.runId)}/attempts/${String(watch.attempt)}`;

        const response = yield* client
          .execute(
            HttpClientRequest.get(url).pipe(
              HttpClientRequest.setHeaders({
                Accept: "application/vnd.github+json",
                "X-GitHub-Api-Version": "2026-03-10",
                "User-Agent": options.userAgent ?? "effect-agent-subscriptions",
              }),
              HttpClientRequest.bearerToken(options.token),
            ),
          )
          .pipe(Effect.mapError(() => httpFailure("unavailable", true)));

        if (
          response.status === 429 ||
          (response.status === 403 &&
            (response.headers["x-ratelimit-remaining"] === "0" ||
              response.headers["retry-after"] !== undefined))
        ) {
          return yield* httpFailure("rate-limited", true);
        }
        if (response.status === 401 || response.status === 403) {
          return yield* httpFailure("unauthorized", false);
        }
        if (response.status === 404) return yield* httpFailure("not-found", true);
        if (response.status < 200 || response.status >= 300) {
          return yield* httpFailure("unavailable", response.status >= 500);
        }

        return yield* response.json.pipe(
          Effect.provideService(HttpIncomingMessage.MaxBodySize, FileSystem.Size(1024 * 1024)),
          Effect.mapError(() => httpFailure("invalid-response", false)),
          Effect.flatMap(Schema.decodeUnknownEffect(GitHubWorkflowRunAttempt)),
          Effect.mapError(() => httpFailure("invalid-response", false)),
        );
      });

      return GitHubWorkflowRuns.of({ getAttempt });
    }),
  );

const sourceError = (code: string, retryable: boolean) =>
  SubscriptionSourceError.make({ code, retryable });

const completionIdentity = (completion: GitHubWorkflowRunCompletion): string =>
  `github-workflow-run:${String(completion.repositoryId)}:${String(completion.runId)}:${String(completion.attempt)}:completed`;

const watchIdentity = (repositoryId: number, watch: GitHubWorkflowRunWatch): string =>
  `github-workflow-run:${String(repositoryId)}:${String(watch.runId)}:${String(watch.attempt)}:completed`;

const completionFromWire = Effect.fn("GitHubWorkflowRun.completionFromWire")(function* (
  repository: GitHubRepository,
  watch: GitHubWorkflowRunWatch,
  wire: GitHubWorkflowRunAttempt,
) {
  if (
    wire.repository.id !== repository.id ||
    wire.id !== watch.runId ||
    wire.run_attempt !== watch.attempt ||
    wire.head_sha !== watch.expectedHeadSha
  ) {
    return yield* sourceError("github-identity-mismatch", false);
  }
  if (wire.status !== "completed") return null;
  if (wire.conclusion === null) {
    return yield* sourceError("github-completed-without-conclusion", false);
  }

  return GitHubWorkflowRunCompletion.make({
    repositoryId: repository.id,
    runId: wire.id,
    attempt: wire.run_attempt,
    headSha: wire.head_sha,
    conclusion: wire.conclusion,
  });
});

export interface GitHubWorkflowRunSourceOptions {
  readonly repository: GitHubRepository;
}

/**
 * Build the exact-attempt completion source, shared by all destination Agents in this partition.
 * Reconciliation depends on GitHub retaining a readable exact attempt.
 */
export const makeGitHubWorkflowRunSource = Effect.fn("makeGitHubWorkflowRunSource")(function* (
  options: GitHubWorkflowRunSourceOptions,
): Effect.fn.Return<EventSource, SubscriptionSourceError, GitHubWorkflowRuns> {
  const repository = yield* Schema.decodeUnknownEffect(GitHubRepository)(options.repository).pipe(
    Effect.mapError(() => sourceError("github-repository-configuration", false)),
  );

  const runs = yield* GitHubWorkflowRuns;

  return yield* makeEventSource({
    source: GitHubWorkflowRunSourceVersion,
    continuity:
      "Webhook delivery is not replayed by GitHub; reconciliation can recover only while the exact workflow run attempt remains readable and authorized.",
    event: GitHubWorkflowRunCompletion.check(
      Schema.makeFilter((completion) => completion.repositoryId === repository.id, {
        title: "GitHub completion belongs to the host-bound repository",
      }),
    ),
    parameters: GitHubWorkflowRunWatch,
    identity: completionIdentity,
    eventKey: completionIdentity,
    parameterKey: (watch) => watchIdentity(repository.id, watch),
    matches: (completion, watch) =>
      completion.repositoryId === repository.id &&
      completion.runId === watch.runId &&
      completion.attempt === watch.attempt &&
      completion.headSha === watch.expectedHeadSha,
    reconcile: (watch) =>
      runs.getAttempt({ repository, runId: watch.runId, attempt: watch.attempt }).pipe(
        Effect.mapError((error) => sourceError(`github-${error.reason}`, error.retryable)),
        Effect.flatMap((wire) => completionFromWire(repository, watch, wire)),
      ),
  });
});

export class GitHubWebhookVerificationError extends Schema.TaggedError<GitHubWebhookVerificationError>()(
  "GitHubWebhookVerificationError",
  {
    reason: Schema.Literals([
      "invalid-signature",
      "invalid-event",
      "invalid-payload",
      "payload-too-large",
      "crypto-unavailable",
    ]),
  },
) {}

/** Host-held webhook secret verification; callers cannot bypass it by supplying parsed JSON. */
export class GitHubWebhookSignatureVerifier extends Context.Service<
  GitHubWebhookSignatureVerifier,
  {
    readonly verify: (
      body: Uint8Array,
      signature: string,
    ) => Effect.Effect<void, GitHubWebhookVerificationError>;
  }
>()("@effect-agent/thread/GitHubWebhookSignatureVerifier") {}

/** Narrow host WebCrypto capability used for GitHub HMAC-SHA256 verification. */
export interface GitHubWebhookSubtleCrypto<Key> {
  readonly importKey: (
    format: "raw",
    keyData: ArrayBuffer,
    algorithm: { readonly name: "HMAC"; readonly hash: "SHA-256" },
    extractable: false,
    usages: ["verify"],
  ) => PromiseLike<Key>;
  readonly verify: (
    algorithm: "HMAC",
    key: Key,
    signature: ArrayBuffer,
    data: ArrayBuffer,
  ) => PromiseLike<boolean>;
}

/** GitHub-compatible HMAC-SHA256 verifier over the exact raw request bytes. */
export const webCryptoGitHubWebhookSignatureVerifierLayer = <Key>(
  secret: Redacted.Redacted<string>,
  subtle: GitHubWebhookSubtleCrypto<Key>,
): Layer.Layer<GitHubWebhookSignatureVerifier> =>
  Layer.effect(
    GitHubWebhookSignatureVerifier,
    Effect.sync(() => {
      const verify = Effect.fn("GitHubWebhookSignatureVerifier.verify")(function* (
        body: Uint8Array,
        signature: string,
      ) {
        if (!/^sha256=[0-9a-f]{64}$/.test(signature)) {
          return yield* GitHubWebhookVerificationError.make({ reason: "invalid-signature" });
        }

        const signatureBytes = yield* Effect.fromResult(
          Encoding.decodeHex(signature.slice("sha256=".length)),
        ).pipe(
          Effect.mapError(() =>
            GitHubWebhookVerificationError.make({ reason: "invalid-signature" }),
          ),
        );

        const keyBytes = new TextEncoder().encode(Redacted.value(secret));
        const keyBuffer = new ArrayBuffer(keyBytes.byteLength);

        new Uint8Array(keyBuffer).set(keyBytes);

        const key = yield* Effect.tryPromise({
          try: () =>
            subtle.importKey("raw", keyBuffer, { name: "HMAC", hash: "SHA-256" }, false, [
              "verify",
            ]),
          catch: () => GitHubWebhookVerificationError.make({ reason: "crypto-unavailable" }),
        });

        const signatureBuffer = new ArrayBuffer(signatureBytes.byteLength);

        new Uint8Array(signatureBuffer).set(signatureBytes);
        const bodyBuffer = new ArrayBuffer(body.byteLength);

        new Uint8Array(bodyBuffer).set(body);

        const valid = yield* Effect.tryPromise({
          try: () => subtle.verify("HMAC", key, signatureBuffer, bodyBuffer),
          catch: () => GitHubWebhookVerificationError.make({ reason: "crypto-unavailable" }),
        });

        if (!valid) {
          return yield* GitHubWebhookVerificationError.make({ reason: "invalid-signature" });
        }
      });

      return GitHubWebhookSignatureVerifier.of({ verify });
    }),
  );

export interface VerifiedGitHubWorkflowRunWebhookRequest {
  readonly body: Uint8Array;
  readonly eventHeader: string | undefined;
  readonly signatureHeader: string | undefined;
  readonly principal: Principal;
}

/**
 * Verify raw ingress, decode only a completed `workflow_run`, and hand its canonical completion to
 * durable intake. The acknowledgement means routing work is retained; it is not a Submission Receipt.
 */
export const acceptVerifiedGitHubWorkflowRunWebhook = Effect.fn(
  "acceptVerifiedGitHubWorkflowRunWebhook",
)(function* (
  request: VerifiedGitHubWorkflowRunWebhookRequest,
): Effect.fn.Return<
  EventAcknowledgement,
  | GitHubWebhookVerificationError
  | Effect.Error<ReturnType<SubscriptionIntake["Service"]["accept"]>>,
  GitHubWebhookSignatureVerifier | SubscriptionIntake
> {
  if (request.body.byteLength > 1024 * 1024) {
    return yield* GitHubWebhookVerificationError.make({ reason: "payload-too-large" });
  }
  if (request.eventHeader !== "workflow_run" || request.signatureHeader === undefined) {
    return yield* GitHubWebhookVerificationError.make({ reason: "invalid-event" });
  }
  const verifier = yield* GitHubWebhookSignatureVerifier;

  yield* verifier.verify(request.body, request.signatureHeader);

  const text = yield* Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(request.body),
    catch: () => GitHubWebhookVerificationError.make({ reason: "invalid-payload" }),
  });

  const webhook = yield* Schema.decodeUnknownEffect(
    Schema.fromJsonString(GitHubWorkflowRunWebhook),
  )(text).pipe(
    Effect.mapError(() => GitHubWebhookVerificationError.make({ reason: "invalid-payload" })),
  );

  if (webhook.repository.id !== webhook.workflow_run.repository.id) {
    return yield* GitHubWebhookVerificationError.make({ reason: "invalid-payload" });
  }
  if (webhook.workflow_run.status !== "completed" || webhook.workflow_run.conclusion === null) {
    return yield* GitHubWebhookVerificationError.make({ reason: "invalid-payload" });
  }

  const completion = GitHubWorkflowRunCompletion.make({
    repositoryId: webhook.repository.id,
    runId: webhook.workflow_run.id,
    attempt: webhook.workflow_run.run_attempt,
    headSha: webhook.workflow_run.head_sha,
    conclusion: webhook.workflow_run.conclusion,
  });

  const intake = yield* SubscriptionIntake;

  return yield* intake.accept(request.principal, GitHubWorkflowRunSourceVersion, completion);
});
