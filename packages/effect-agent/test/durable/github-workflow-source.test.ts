import { describe, expect, it } from "@effect/vitest";
import { Effect, Redacted, Schema } from "effect";
import {
  acceptVerifiedGitHubWorkflowRunWebhook,
  GitHubRepository,
  GitHubWebhookSignatureVerifier,
  GitHubWorkflowRunAttempt,
  GitHubWorkflowRuns,
  makeGitHubWorkflowRunSource,
  webCryptoGitHubWebhookSignatureVerifierLayer,
} from "effect-agent/git-hub-workflow-source";
import { AgentId, ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, Digest } from "effect-agent/records";
import { Principal } from "effect-agent/submission-ledger";
import { SubscriptionRecord } from "effect-agent/subscription";
import { SubscriptionIntake } from "effect-agent/subscriptions";

const SHA = "a".repeat(40);
const DIGEST = Schema.decodeSync(Digest)("b".repeat(64));
const repository = GitHubRepository.make({ id: 101, owner: "effect", name: "agent" });
const principal = Schema.decodeSync(Principal)("github-webhook");

const record = Schema.decodeSync(SubscriptionRecord)({
  schemaVersion: 1,
  key: {
    partition: { tenantId: "tenant", address: "github:101" },
    ownerId: "owner",
    subscriptionId: "subscription",
  },
  creationFingerprint: DIGEST,
  createdBy: principal,
  createdAtMillis: 1,
  ordinal: 1,
  configurationRevision: 1,
  configurationFingerprint: DIGEST,
  creationConfiguration: {
    source: { name: "github-workflow-run-completed", version: "1" },
    matchingKey: "github-workflow-run:101:202:3:completed",
    parameters: { runId: 202, attempt: 3, expectedHeadSha: SHA },
    context: { reason: "release" },
    mode: "once",
    expiresAtMillis: 10_000,
    destination: {
      _tag: "ExistingThread",
      threadId: Schema.decodeSync(ThreadId)("thread"),
    },
    deliveryPrincipal: principal,
    agentId: Schema.decodeSync(AgentId)("agent"),
    definitions: DefinitionDigests.make({ agent: DIGEST, model: DIGEST, tools: DIGEST }),
  },
  configuration: {
    source: { name: "github-workflow-run-completed", version: "1" },
    matchingKey: "github-workflow-run:101:202:3:completed",
    parameters: { runId: 202, attempt: 3, expectedHeadSha: SHA },
    context: { reason: "release" },
    mode: "once",
    expiresAtMillis: 10_000,
    destination: {
      _tag: "ExistingThread",
      threadId: Schema.decodeSync(ThreadId)("thread"),
    },
    deliveryPrincipal: principal,
    agentId: Schema.decodeSync(AgentId)("agent"),
    definitions: DefinitionDigests.make({ agent: DIGEST, model: DIGEST, tools: DIGEST }),
  },
  state: "active",
  recovery: null,
});

const completedAttemptWire: unknown = {
  id: 202,
  run_attempt: 3,
  head_sha: SHA,
  status: "completed",
  conclusion: "success",
  repository: { id: 101, full_name: "effect/agent" },
  html_url: "https://github.com/effect/agent/actions/runs/202",
  actor: { login: "octocat" },
};

const completedAttempt = Schema.decodeUnknownSync(GitHubWorkflowRunAttempt)(completedAttemptWire);

const sourceWith = (attempt: typeof GitHubWorkflowRunAttempt.Type) =>
  makeGitHubWorkflowRunSource({
    repository,
  }).pipe(
    Effect.provideService(
      GitHubWorkflowRuns,
      GitHubWorkflowRuns.of({ getAttempt: () => Effect.succeed(attempt) }),
    ),
  );

describe("GitHub workflow completion source", () => {
  it.effect("verifies GitHub's published HMAC vector and rejects tampering before intake", () =>
    Effect.gen(function* () {
      // https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
      const signature = "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17";
      const verifier = yield* GitHubWebhookSignatureVerifier;

      yield* verifier.verify(new TextEncoder().encode("Hello, World!"), signature);

      const changed = yield* Effect.flip(
        verifier.verify(new TextEncoder().encode("Hello, World?"), signature),
      );

      expect(changed.reason).toBe("invalid-signature");
      const malformed = yield* Effect.flip(verifier.verify(new Uint8Array(), "sha256=bad"));

      expect(malformed.reason).toBe("invalid-signature");

      let intakes = 0;

      const rejected = yield* Effect.flip(
        acceptVerifiedGitHubWorkflowRunWebhook({
          body: new TextEncoder().encode("{}"),
          eventHeader: "workflow_run",
          signatureHeader: signature,
          principal,
        }).pipe(
          Effect.provideService(
            SubscriptionIntake,
            SubscriptionIntake.of({
              accept: () =>
                Effect.sync(() => {
                  intakes++;

                  return {
                    partition: record.key.partition,
                    eventId: "invalid",
                    acceptedAtMillis: 1,
                  };
                }),
              status: () => Effect.die("unused"),
            }),
          ),
        ),
      );

      expect(rejected).toMatchObject({ reason: "invalid-signature" });
      expect(intakes).toBe(0);
    }).pipe(
      Effect.provide(
        webCryptoGitHubWebhookSignatureVerifierLayer(
          Redacted.make("It's a Secret to Everybody"),
          globalThis.crypto.subtle,
        ),
      ),
    ),
  );

  it.effect("fails closed on a different attempt identity", () =>
    Effect.gen(function* () {
      const source = yield* sourceWith({ ...completedAttempt, run_attempt: 4 });
      const reconcile = source.reconcile;

      if (reconcile === undefined) return yield* Effect.die("source has no reconciler");
      const failure = yield* Effect.flip(reconcile(record));

      expect(failure).toMatchObject({
        _tag: "SubscriptionSourceError",
        code: "github-identity-mismatch",
        retryable: false,
      });
    }),
  );
});
