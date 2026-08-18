import { createHash } from "node:crypto";

import {
  GitCommitSha,
  PatchDigest,
  WorkOrderAttemptPolicy,
  WorkOrderCheckResult,
  WorkOrderDigest,
  WorkOrderHost,
  WorkOrderReport,
  type ImplementationWorkspace,
  type PatchSnapshot,
  type PublishedWorkOrder,
  type WorkOrderMission,
} from "@effect-agent/example-pr-work-orders";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { type Crypto, Effect, FileSystem, Layer, Path, Schema } from "effect";

import {
  authenticateDelivery,
  ObservedActionsIdentity,
  signGitHubDelivery,
} from "../src/authenticate.ts";
import { constructWorkOrder } from "../src/construct.ts";
import {
  DEFAULT_MENTION_COMMAND,
  DEFAULT_REACTION_CONTENT,
  type DeliveryUnauthentic,
  type DispatchTargetRejected,
  type DispatchUnauthorized,
  GITHUB_WRITE_TOKEN_ENV,
  IsolatedCheckRequest,
  IsolatedCheckSpec,
  IngressPolicy,
  IngressPolicyConfig,
  MODEL_SECRET_ENV,
  PlatformDelivery,
  type PresentationFailure,
  PublisherRequest,
  PublisherTrust,
  type PublisherVerificationFailure,
  PullRequestView,
  ReviewCommentView,
  type StaleCommentAnchor,
} from "../src/contracts.ts";
import { pullRequestFromWire, reviewCommentFromWire } from "../src/github-live.ts";
import { makeFakeGitHub } from "../src/github.ts";
import { handleWorkOrderDelivery, WorkOrderImplementer } from "../src/ingress.ts";
import { IsolatedChecks } from "../src/isolation.ts";
import { parseDispatchTarget } from "../src/parse-event.ts";
import { IsolatedPublisher } from "../src/publisher.ts";
import { pullRequestNumberFromEvent } from "../src/run-delivery.ts";
import { FileBackedAttemptStore, IngressStoreFailpoint } from "../src/store.ts";

const HEAD = Schema.decodeUnknownSync(GitCommitSha)("a".repeat(40));
const STALE = Schema.decodeUnknownSync(GitCommitSha)("b".repeat(40));
const DIGEST = Schema.decodeUnknownSync(WorkOrderDigest)("c".repeat(64));
const EMPTY_DIGEST = Schema.decodeUnknownSync(PatchDigest)(
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
);
const SECRET = "ingress-test-webhook-secret";
const REPOSITORY = "acme/widgets";
const PULL = 17;
const ACTOR_ID = "42";
const FILE_PATH = "src/value.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Assert<Value extends true> = Value;

const typedHandle = handleWorkOrderDelivery(null as unknown as PlatformDelivery);
type TypedHandleError = Effect.Error<typeof typedHandle>;
type TypedHandleServices = Effect.Services<typeof typedHandle>;
type HandleKeepsUnauthentic = Assert<
  Equal<Extract<TypedHandleError, DeliveryUnauthentic>, DeliveryUnauthentic>
>;
type HandleKeepsUnauthorized = Assert<
  Equal<Extract<TypedHandleError, DispatchUnauthorized>, DispatchUnauthorized>
>;
type HandleKeepsTarget = Assert<
  Equal<Extract<TypedHandleError, DispatchTargetRejected>, DispatchTargetRejected>
>;
type HandleKeepsStale = Assert<
  Equal<Extract<TypedHandleError, StaleCommentAnchor>, StaleCommentAnchor>
>;
type HandleKeepsPresentation = Assert<
  Equal<Extract<TypedHandleError, PresentationFailure>, PresentationFailure>
>;
type HandleUnknownExcluded = Assert<Equal<unknown extends TypedHandleError ? true : false, false>>;
type HandleRequiresPolicy = Assert<
  Equal<Extract<TypedHandleServices, IngressPolicy>, IngressPolicy>
>;
type HandleRequiresStore = Assert<
  Equal<Extract<TypedHandleServices, FileBackedAttemptStore>, FileBackedAttemptStore>
>;
type HandleRequiresHost = Assert<Equal<Extract<TypedHandleServices, WorkOrderHost>, WorkOrderHost>>;
type HandleRequiresCrypto = Assert<
  Equal<Extract<TypedHandleServices, Crypto.Crypto>, Crypto.Crypto>
>;

const defaultTrustFields = {
  workOrderId: "wo-1",
  workOrderDigest: DIGEST,
  repository: REPOSITORY,
  pullRequestNumber: PULL,
  expectedHeadSha: HEAD,
  allowedPaths: [FILE_PATH],
  patchDigest: EMPTY_DIGEST,
  requiredChecks: [{ name: "fixture-check", status: "passed", summary: "ok" }] as const,
};

const typedPublish = IsolatedPublisher.layer({
  stateDir: "/tmp",
}).pipe(
  Layer.build,
  Effect.flatMap(() =>
    Effect.flatMap(IsolatedPublisher, (publisher) =>
      publisher.publish(null as unknown as PublisherRequest),
    ),
  ),
);
type TypedPublishError = Effect.Error<typeof typedPublish>;
type PublishKeepsVerification = Assert<
  Equal<Extract<TypedPublishError, PublisherVerificationFailure>, PublisherVerificationFailure>
>;

const policyConfig = IngressPolicyConfig.make({
  repository: REPOSITORY,
  pullRequestNumber: PULL,
  authorizedActorIds: [ACTOR_ID],
  mentionCommand: DEFAULT_MENTION_COMMAND,
  reactionContent: DEFAULT_REACTION_CONTENT,
  webhookSecret: SECRET,
});

const targetComment = (overrides?: {
  readonly commitSha?: typeof HEAD;
  readonly omitPath?: boolean;
}) =>
  ReviewCommentView.make({
    commentId: "1001",
    threadId: "99",
    authorId: "7",
    authorLogin: "reviewer",
    commitSha: overrides?.commitSha ?? HEAD,
    ...(overrides?.omitPath === true ? {} : { path: FILE_PATH }),
    startLine: 1,
    endLine: 1,
    body: "The exported answer must be 42.",
  });

const pullRequest = (overrides?: Partial<PullRequestView>) =>
  PullRequestView.make({
    repository: REPOSITORY,
    pullRequestNumber: PULL,
    headSha: HEAD,
    headRepository: REPOSITORY,
    headIsFork: false,
    baseRepository: REPOSITORY,
    ...overrides,
  });

const mentionPayload = (overrides?: {
  readonly inReplyTo?: number;
  readonly senderId?: number;
  readonly senderLogin?: string;
  readonly repository?: string;
  readonly pullRequestNumber?: number;
}) => ({
  action: "created" as const,
  comment: {
    id: 1002,
    ...(overrides && "inReplyTo" in overrides && overrides.inReplyTo !== undefined
      ? { in_reply_to_id: overrides.inReplyTo }
      : {}),
    body: DEFAULT_MENTION_COMMAND,
    user: { id: overrides?.senderId ?? 42, login: overrides?.senderLogin ?? "alice" },
  },
  pull_request: { number: overrides?.pullRequestNumber ?? PULL },
  repository: { full_name: overrides?.repository ?? REPOSITORY, fork: false },
  sender: { id: overrides?.senderId ?? 42, login: overrides?.senderLogin ?? "alice" },
});

const loadFixture = Effect.fn("loadFixture")(function* (name: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const text = yield* fs.readFileString(path.join(import.meta.dirname, "fixtures", name));
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(text);
});

const signedDelivery = Effect.fn("signedDelivery")(function* (input: {
  readonly deliveryId: string;
  readonly eventName: string;
  readonly payload: unknown;
}) {
  const rawBody = JSON.stringify(input.payload);
  return PlatformDelivery.make({
    deliveryId: input.deliveryId,
    eventName: input.eventName,
    rawBody,
    signature: yield* signGitHubDelivery(SECRET, rawBody),
  });
});

const emptyPatch: PatchSnapshot = {
  digest: EMPTY_DIGEST,
  changedPaths: [],
  preview: "",
  truncated: false,
};

const stubHostLayer = Layer.succeed(
  WorkOrderHost,
  WorkOrderHost.of({
    requiredChecks: [],
    currentHead: Effect.succeed(HEAD),
    authorizeDispatch: () => Effect.void,
    requireCurrentHead: () => Effect.void,
    withWorktree: (_order, run) => {
      const workspace: ImplementationWorkspace = {
        readFile: () => Effect.succeed(""),
        search: () => Effect.succeed([]),
        applyEdit: () => Effect.void,
        inspectPatch: Effect.succeed(emptyPatch),
        requestCheck: (name) =>
          Effect.succeed(
            WorkOrderCheckResult.make({ name, status: "passed", summary: "stub check" }),
          ),
      };
      return run({
        allowedPaths: new Set([FILE_PATH]),
        modelWorkspace: workspace,
        inspectPatch: workspace.inspectPatch,
        runCheck: workspace.requestCheck,
        observedChecks: Effect.succeed([]),
        commitAndPublish: ({ order, report, patch, checks }) =>
          Effect.succeed({
            _tag: "published",
            workOrderId: order.workOrderId,
            workOrderDigest: report.workOrderDigest,
            previousHeadSha: order.headSha,
            publishedHeadSha: HEAD,
            patchDigest: patch.digest,
            changedPaths: patch.changedPaths,
            checks,
          } satisfies PublishedWorkOrder),
      });
    },
  }),
);

const countingImplementer = () => {
  let invocations = 0;
  const layer = Layer.succeed(
    WorkOrderImplementer,
    WorkOrderImplementer.of({
      run: (mission: WorkOrderMission) => {
        invocations += 1;
        return Effect.succeed(
          WorkOrderReport.make({
            workOrderDigest: mission.workOrderDigest,
            headSha: mission.order.headSha,
            disposition: "not-applicable",
            changedPaths: [],
            checks: [],
            summary: "The instruction is already satisfied.",
          }),
        );
      },
    }),
  );
  return { layer, invocations: () => invocations };
};

const NOTES_PATCH = "diff --git a/notes.md b/notes.md\n--- a/notes.md\n+++ b/notes.md\n";
const NOTES_DIGEST = Schema.decodeUnknownSync(PatchDigest)(
  createHash("sha256").update(NOTES_PATCH).digest("hex"),
);
const TRADITIONAL_PATCH = "--- a/secret\n+++ b/secret\n";
const TRADITIONAL_DIGEST = Schema.decodeUnknownSync(PatchDigest)(
  createHash("sha256").update(TRADITIONAL_PATCH).digest("hex"),
);

const defaultTrust = (overrides?: Partial<PublisherTrust>) =>
  PublisherTrust.make({
    ...defaultTrustFields,
    requiredChecks: [...defaultTrustFields.requiredChecks],
    ...overrides,
  });

const publisherRequest = (overrides?: {
  readonly patch?: string;
  readonly trust?: Partial<PublisherTrust>;
}) =>
  PublisherRequest.make({
    patch: overrides?.patch ?? "",
    trust: defaultTrust(overrides?.trust),
  });

const withIngress = <A, E, R>(
  input: {
    readonly comments?: ReadonlyMap<string, ReviewCommentView>;
    readonly pull?: PullRequestView;
    readonly implementer?: ReturnType<typeof countingImplementer>;
  },
  apply: (env: {
    readonly github: ReturnType<typeof makeFakeGitHub>;
    readonly implementer: ReturnType<typeof countingImplementer>;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-work-order-ingress-" });
      const github = makeFakeGitHub({
        repository: REPOSITORY,
        pullRequest: input.pull ?? pullRequest(),
        comments: input.comments ?? new Map([["1001", targetComment()]]),
      });
      const implementer = input.implementer ?? countingImplementer();
      return yield* apply({ github, implementer }).pipe(
        Effect.provide(
          Layer.mergeAll(
            github.layer,
            IngressPolicy.layer(policyConfig),
            ObservedActionsIdentity.layerAbsent,
            IsolatedChecks.layer,
            FileBackedAttemptStore.layer(directory).pipe(
              Layer.provide(IngressStoreFailpoint.layer),
            ),
            stubHostLayer,
            WorkOrderAttemptPolicy.layerMemory,
            implementer.layer,
          ),
        ),
      );
    }),
  );

describe("PR work-order ingress", () => {
  it.effect(
    "WOI-013 keeps expected authentication, targeting, admission, isolation, publication, and presentation failures in E",
    () =>
      Effect.sync(() => {
        const proofs = {
          handleKeepsUnauthentic: true as HandleKeepsUnauthentic,
          handleKeepsUnauthorized: true as HandleKeepsUnauthorized,
          handleKeepsTarget: true as HandleKeepsTarget,
          handleKeepsStale: true as HandleKeepsStale,
          handleKeepsPresentation: true as HandleKeepsPresentation,
          handleUnknownExcluded: true as HandleUnknownExcluded,
          handleRequiresPolicy: true as HandleRequiresPolicy,
          handleRequiresStore: true as HandleRequiresStore,
          handleRequiresHost: true as HandleRequiresHost,
          handleRequiresCrypto: true as HandleRequiresCrypto,
          publishKeepsVerification: true as PublishKeepsVerification,
        };
        expect(proofs).toEqual({
          handleKeepsUnauthentic: true,
          handleKeepsUnauthorized: true,
          handleKeepsTarget: true,
          handleKeepsStale: true,
          handleKeepsPresentation: true,
          handleUnknownExcluded: true,
          handleRequiresPolicy: true,
          handleRequiresStore: true,
          handleRequiresHost: true,
          handleRequiresCrypto: true,
          publishKeepsVerification: true,
        });
      }),
  );

  it.effect(
    "WOI-001 WOI-005 a mention reply with in_reply_to constructs one work order and invokes the host once",
    () =>
      withIngress({}, ({ implementer }) =>
        Effect.gen(function* () {
          const result = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-mention",
              eventName: "pull_request_review_comment",
              payload: yield* loadFixture("mention-reply.json"),
            }),
          );
          expect(result._tag).toBe("settled");
          if (result._tag !== "settled") return;
          expect(result.disposition).toBe("not-applicable");
          expect(implementer.invocations()).toBe(1);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-001 WOI-005 a reaction on that comment constructs one work order and invokes the host once",
    () =>
      withIngress({}, ({ implementer }) =>
        Effect.gen(function* () {
          const result = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-reaction",
              eventName: "reaction",
              payload: yield* loadFixture("reaction.json"),
            }),
          );
          expect(result._tag).toBe("settled");
          expect(implementer.invocations()).toBe(1);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-001 a mention without a unique inline target is rejected and does not invoke the implementer",
    () =>
      withIngress({}, ({ implementer }) =>
        Effect.gen(function* () {
          const rejected = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-no-target",
              eventName: "pull_request_review_comment",
              payload: yield* loadFixture("mention-no-reply.json"),
            }),
          ).pipe(Effect.flip);
          expect(rejected._tag).toBe("DispatchTargetRejected");
          expect(implementer.invocations()).toBe(0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-001 a PR conversation comment, review summary, or pathless event is rejected",
    () =>
      withIngress(
        { comments: new Map([["1001", targetComment({ omitPath: true })]]) },
        ({ implementer }) =>
          Effect.gen(function* () {
            const conversation = yield* handleWorkOrderDelivery(
              yield* signedDelivery({
                deliveryId: "evt-issue",
                eventName: "issue_comment",
                payload: yield* loadFixture("issue-comment.json"),
              }),
            ).pipe(Effect.flip);
            const review = yield* handleWorkOrderDelivery(
              yield* signedDelivery({
                deliveryId: "evt-review",
                eventName: "pull_request_review",
                payload: yield* loadFixture("review-summary.json"),
              }),
            ).pipe(Effect.flip);
            const pathless = yield* handleWorkOrderDelivery(
              yield* signedDelivery({
                deliveryId: "evt-pathless",
                eventName: "reaction",
                payload: yield* loadFixture("pathless-reaction.json"),
              }),
            ).pipe(Effect.flip);
            expect(conversation._tag).toBe("DispatchTargetRejected");
            expect(review._tag).toBe("DispatchTargetRejected");
            expect(pathless._tag).toBe("DispatchTargetRejected");
            expect(implementer.invocations()).toBe(0);
          }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-002 an unauthorized actor id is rejected even when the login matches a configured human",
    () =>
      withIngress({}, ({ implementer }) =>
        Effect.gen(function* () {
          const rejected = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-unauth",
              eventName: "pull_request_review_comment",
              payload: yield* loadFixture("unauthorized-actor.json"),
            }),
          ).pipe(Effect.flip);
          expect(rejected).toMatchObject({ _tag: "DispatchUnauthorized", actorId: "99" });
          expect(implementer.invocations()).toBe(0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-003 a fork or foreign repository is rejected", () =>
    Effect.gen(function* () {
      yield* withIngress({ pull: pullRequest({ headIsFork: true }) }, ({ implementer }) =>
        Effect.gen(function* () {
          const rejected = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-fork",
              eventName: "pull_request_review_comment",
              payload: mentionPayload({ inReplyTo: 1001 }),
            }),
          ).pipe(Effect.flip);
          expect(rejected._tag).toBe("UntrustedPullRequest");
          expect(implementer.invocations()).toBe(0);
        }),
      );
      yield* withIngress({}, ({ implementer }) =>
        Effect.gen(function* () {
          const rejected = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-foreign",
              eventName: "pull_request_review_comment",
              payload: yield* loadFixture("foreign-repo.json"),
            }),
          ).pipe(Effect.flip);
          expect(rejected._tag).toBe("UntrustedPullRequest");
          expect(implementer.invocations()).toBe(0);
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-004 a comment anchored to an older SHA is rejected against the current head", () =>
    withIngress(
      { comments: new Map([["1001", targetComment({ commitSha: STALE })]]) },
      ({ implementer }) =>
        Effect.gen(function* () {
          const rejected = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-stale",
              eventName: "pull_request_review_comment",
              payload: mentionPayload({ inReplyTo: 1001 }),
            }),
          ).pipe(Effect.flip);
          expect(rejected).toMatchObject({
            _tag: "StaleCommentAnchor",
            sourceSha: STALE,
            headSha: HEAD,
          });
          expect(implementer.invocations()).toBe(0);
        }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 duplicate delivery of the same eventId returns the stored outcome", () =>
    withIngress({}, ({ implementer, github }) =>
      Effect.gen(function* () {
        const delivery = yield* signedDelivery({
          deliveryId: "evt-dup",
          eventName: "pull_request_review_comment",
          payload: mentionPayload({ inReplyTo: 1001 }),
        });
        const first = yield* handleWorkOrderDelivery(delivery);
        const second = yield* handleWorkOrderDelivery(delivery);
        expect(first).toEqual(second);
        expect(implementer.invocations()).toBe(1);
        expect(github.replies()).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 a second explicit dispatch has a distinct work-order id", () =>
    withIngress({}, ({ implementer }) =>
      Effect.gen(function* () {
        const first = yield* handleWorkOrderDelivery(
          yield* signedDelivery({
            deliveryId: "evt-a",
            eventName: "pull_request_review_comment",
            payload: mentionPayload({ inReplyTo: 1001 }),
          }),
        );
        const second = yield* handleWorkOrderDelivery(
          yield* signedDelivery({
            deliveryId: "evt-b",
            eventName: "pull_request_review_comment",
            payload: mentionPayload({ inReplyTo: 1001 }),
          }),
        );
        expect(first._tag).toBe("settled");
        expect(second._tag).toBe("settled");
        if (first._tag !== "settled" || second._tag !== "settled") return;
        expect(first.workOrderId).not.toBe(second.workOrderId);
        expect(implementer.invocations()).toBe(2);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 a crash after claim records an incomplete settlement and does not retry", () =>
    withIngress({}, ({ implementer }) =>
      Effect.gen(function* () {
        const delivery = yield* signedDelivery({
          deliveryId: "evt-crash",
          eventName: "pull_request_review_comment",
          payload: mentionPayload({ inReplyTo: 1001 }),
        });
        const target = yield* parseDispatchTarget(delivery);
        const order = yield* constructWorkOrder(target, delivery.deliveryId);
        const store = yield* FileBackedAttemptStore;
        expect((yield* store.claim(order))._tag).toBe("claimed");
        const rejected = yield* handleWorkOrderDelivery(delivery).pipe(Effect.flip);
        expect(rejected._tag).toBe("AttemptIncomplete");
        expect(implementer.invocations()).toBe(0);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-007 the check process environment contains neither a GitHub write token nor a provider secret",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          process.env[GITHUB_WRITE_TOKEN_ENV] = "ghs_parent_write_token";
          process.env[MODEL_SECRET_ENV] = "sk_parent_model_secret";
          const fs = yield* FileSystem.FileSystem;
          const worktree = yield* fs.makeTempDirectoryScoped({ prefix: "ingress-check-" });
          const checks = yield* IsolatedChecks;
          const isolated = yield* checks
            .run(
              IsolatedCheckRequest.make({
                worktreeRoot: worktree,
                checks: [
                  IsolatedCheckSpec.make({
                    name: "fixture-check",
                    command: process.execPath,
                    args: ["-e", "process.stdout.write('ok')"],
                  }),
                ],
              }),
            )
            .pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  delete process.env[GITHUB_WRITE_TOKEN_ENV];
                  delete process.env[MODEL_SECRET_ENV];
                }),
              ),
            );
          expect(isolated.environment).toEqual({
            process: "check",
            hasWriteToken: false,
            hasModelSecret: false,
          });
          expect(isolated.results).toEqual([
            { name: "fixture-check", status: "passed", summary: "ok" },
          ]);
        }),
      ).pipe(Effect.provide(Layer.mergeAll(IsolatedChecks.layer, NodeServices.layer))),
  );

  it.effect("WOI-001 Actions authentication binds the trusted event payload and delivery id", () =>
    Effect.gen(function* () {
      const rawBody = JSON.stringify(mentionPayload({ inReplyTo: 1001 }));
      const delivery = PlatformDelivery.make({
        deliveryId: "actions-run-1:1",
        eventName: "pull_request_review_comment",
        rawBody,
      });
      const matching = Layer.succeed(
        ObservedActionsIdentity,
        ObservedActionsIdentity.of({
          read: Effect.succeed({
            repository: REPOSITORY,
            eventName: "pull_request_review_comment",
            eventPayload: rawBody,
            deliveryId: "actions-run-1:1",
          }),
        }),
      );
      const actionsPolicy = Layer.mergeAll(matching, IngressPolicy.layer(policyConfig));
      yield* authenticateDelivery(delivery).pipe(Effect.provide(actionsPolicy));
      const forged = yield* authenticateDelivery(
        PlatformDelivery.make({
          deliveryId: "actions-run-1:1",
          eventName: "pull_request_review_comment",
          rawBody: JSON.stringify({ forged: true }),
        }),
      ).pipe(Effect.provide(actionsPolicy), Effect.flip);
      const swappedId = yield* authenticateDelivery(
        PlatformDelivery.make({
          deliveryId: "other-run:1",
          eventName: "pull_request_review_comment",
          rawBody,
        }),
      ).pipe(Effect.provide(actionsPolicy), Effect.flip);
      expect(forged._tag).toBe("DeliveryUnauthentic");
      expect(swappedId._tag).toBe("DeliveryUnauthentic");
    }),
  );

  it.effect(
    "WOI-008 WOI-009 the publisher rejects a digest, path, identity, or head mismatch and does not update the ref",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "ingress-publish-" });
          yield* fs.writeFileString(path.join(stateDir, "head"), HEAD);
          const writeExpected = (trust: PublisherTrust) =>
            Schema.encodeEffect(Schema.fromJsonString(PublisherTrust))(trust).pipe(
              Effect.flatMap((text) =>
                fs.writeFileString(path.join(stateDir, "expected.json"), text),
              ),
            );
          const publish = (request: PublisherRequest, expected?: PublisherTrust) =>
            writeExpected(expected ?? request.trust).pipe(
              Effect.andThen(
                Effect.gen(function* () {
                  const publisher = yield* IsolatedPublisher;
                  return yield* publisher.publish(request);
                }).pipe(Effect.provide(IsolatedPublisher.layer({ stateDir })), Effect.flip),
              ),
            );
          const digest = yield* publish(
            publisherRequest({
              patch: "diff --git a/src/value.ts b/src/value.ts\n",
            }),
          );
          const forbidden = yield* publish(
            publisherRequest({
              patch: NOTES_PATCH,
              trust: { patchDigest: NOTES_DIGEST },
            }),
          );
          const unproven = yield* publish(
            publisherRequest({
              patch: TRADITIONAL_PATCH,
              trust: { patchDigest: TRADITIONAL_DIGEST },
            }),
          );
          const substituted = yield* publish(
            publisherRequest({ trust: { workOrderId: "wo-forged" } }),
            defaultTrust(),
          );
          yield* fs.writeFileString(path.join(stateDir, "head"), STALE);
          const head = yield* publish(publisherRequest());
          const after = yield* fs.readFileString(path.join(stateDir, "head"));
          expect(digest._tag).toBe("PublisherVerificationFailure");
          expect(forbidden).toMatchObject({
            _tag: "PublisherVerificationFailure",
            reason: "path-not-allowed",
          });
          expect(unproven).toMatchObject({
            _tag: "PublisherVerificationFailure",
            reason: "path-not-allowed",
          });
          expect(substituted).toMatchObject({
            _tag: "PublisherVerificationFailure",
            reason: "identity-mismatch",
          });
          expect(head._tag).toBe("StalePullRequestHead");
          expect(after.trim()).toBe(STALE);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-009 a successful update followed by lock cleanup failure reports publication uncertainty",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const stateDir = yield* fs.makeTempDirectoryScoped({ prefix: "ingress-publish-lock-" });
          yield* fs.writeFileString(path.join(stateDir, "head"), HEAD);
          const request = publisherRequest();
          yield* Schema.encodeEffect(Schema.fromJsonString(PublisherTrust))(request.trust).pipe(
            Effect.flatMap((text) =>
              fs.writeFileString(path.join(stateDir, "expected.json"), text),
            ),
          );
          const result = yield* Effect.gen(function* () {
            const publisher = yield* IsolatedPublisher;
            return yield* publisher.publish(request);
          }).pipe(
            Effect.provide(
              IsolatedPublisher.layer({
                stateDir,
                failpoint: "lock-release",
              }),
            ),
            Effect.flip,
          );
          const after = yield* fs.readFileString(path.join(stateDir, "head"));
          const lockRemains = yield* fs.exists(path.join(stateDir, "head.lock"));
          expect(result._tag).toBe("PublicationUncertainty");
          if (result._tag !== "PublicationUncertainty") return;
          expect(result.observedHeadSha).toBeDefined();
          expect(after.trim()).toBe(result.observedHeadSha);
          expect(lockRemains).toBe(true);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-010 a published or settled run posts one thread reply and does not resolve the thread",
    () =>
      withIngress({}, ({ github }) =>
        Effect.gen(function* () {
          const result = yield* handleWorkOrderDelivery(
            yield* signedDelivery({
              deliveryId: "evt-reply",
              eventName: "pull_request_review_comment",
              payload: mentionPayload({ inReplyTo: 1001 }),
            }),
          );
          expect(result._tag).toBe("settled");
          expect(github.replies()).toEqual([
            {
              commentId: "1001",
              reply: {
                kind: "settled",
                body: "settled not-applicable: The instruction is already satisfied.",
              },
            },
          ]);
          expect(github.resolveCount()).toBe(0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-011 the enabled workflow does not hold a model secret or commit write token", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const workflow = yield* fs.readFileString(
        path.resolve(import.meta.dirname, "../../../.github/workflows/pr-work-order.yml"),
      );
      expect(workflow).toContain("pr-work-order-ingress");
      expect(workflow).toContain("contents: read");
      expect(workflow).not.toContain("contents: write");
      expect(workflow).not.toContain("OPENAI_API_KEY");
      expect(workflow).not.toContain("EFFECT_AGENT_MODEL_SECRET");
      expect(workflow).not.toContain("EFFECT_AGENT_GITHUB_WRITE_TOKEN");
      expect(workflow).not.toContain("@effect-agent/pr-review");
      expect(workflow).not.toContain("./action");
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("the Actions entrypoint reads the pull-request number from the trusted event", () =>
    Effect.gen(function* () {
      const number = yield* pullRequestNumberFromEvent(
        JSON.stringify({ pull_request: { number: PULL } }),
      );
      const rejected = yield* pullRequestNumberFromEvent("{}").pipe(Effect.flip);
      expect(number).toBe(PULL);
      expect(rejected._tag).toBe("DispatchTargetRejected");
    }),
  );

  it.effect("the live GitHub adapter maps API wires onto ingress views", () =>
    Effect.sync(() => {
      const pull = pullRequestFromWire(REPOSITORY, {
        number: PULL,
        head: {
          sha: HEAD,
          repo: { full_name: REPOSITORY, fork: false },
        },
        base: { repo: { full_name: REPOSITORY, fork: false } },
      });
      const comment = reviewCommentFromWire({
        id: 1001,
        user: { id: 7, login: "reviewer" },
        commit_id: HEAD,
        path: FILE_PATH,
        line: 1,
        start_line: 1,
        original_line: 1,
        body: "The exported answer must be 42.",
        pull_request_url: `https://api.github.com/repos/${REPOSITORY}/pulls/${String(PULL)}`,
      });
      expect(pull).toMatchObject({
        repository: REPOSITORY,
        pullRequestNumber: PULL,
        headSha: HEAD,
        headIsFork: false,
      });
      expect(comment).toMatchObject({
        commentId: "1001",
        authorId: "7",
        path: FILE_PATH,
        commitSha: HEAD,
      });
    }),
  );
});
