import { createHash } from "node:crypto";

import {
  createWorkOrder,
  GitCommitSha,
  PatchDigest,
  ProposedWorkOrder,
  WorkOrderAttemptPolicy,
  WorkOrderCheckResult,
  WorkOrderDigest,
  WorkOrderHost,
  WorkOrderIdentity,
  WorkOrderReport,
  workOrderDigest,
  type ImplementationWorkspace,
  type PatchSnapshot,
  type PublishedWorkOrder,
  type WorkOrderMission,
} from "@effect-agent/example-pr-work-orders";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  type Crypto,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Ref,
  Schema,
} from "effect";
import { Yaml } from "effect/unstable/encoding";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { isolatedCheckContainerArguments, reproduceCheckedPatch } from "../src/action-checks.ts";
import {
  CheckedFile,
  CheckedWorkOrder,
  FailedTerminal,
  WorkOrderAdmission,
} from "../src/action-contracts.ts";
import { liveWorkOrderGitHubLayer, WorkOrderGitHub } from "../src/action-github.ts";
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
import { claimedState, completedState, WorkOrderJournalAuthenticator } from "../src/journal.ts";
import { parseDispatchTarget } from "../src/parse-event.ts";
import { completeModifiedPaths } from "../src/patch.ts";
import { IsolatedPublisher } from "../src/publisher.ts";
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

const WorkOrderWorkflowPermissions = Schema.Union([
  Schema.String,
  Schema.Record(Schema.String, Schema.String),
]);
const WorkOrderWorkflowStep = Schema.Struct({
  uses: Schema.optionalKey(Schema.String),
  with: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  run: Schema.optionalKey(Schema.String),
});
const WorkOrderWorkflowJob = Schema.Struct({
  uses: Schema.optionalKey(Schema.String),
  permissions: Schema.optionalKey(WorkOrderWorkflowPermissions),
  secrets: Schema.optionalKey(Schema.Union([Schema.Literal("inherit"), Schema.Unknown])),
  steps: Schema.optionalKey(Schema.Array(WorkOrderWorkflowStep)),
});
const WorkOrderWorkflowFile = Schema.Struct({
  permissions: Schema.optionalKey(WorkOrderWorkflowPermissions),
  jobs: Schema.Record(Schema.String, WorkOrderWorkflowJob),
});
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const assertExactPermissions = (
  permissions: unknown,
  label: string,
  expected: Readonly<Record<string, "read" | "write" | "none">>,
) => {
  expect(permissions, `${label} must declare an explicit permissions map`).toBeTypeOf("object");
  expect(permissions).not.toBeNull();
  expect(Array.isArray(permissions)).toBe(false);
  if (!isRecord(permissions)) return;
  expect(permissions).toEqual(expected);
  for (const [scope, access] of Object.entries(permissions)) {
    expect(["read", "write", "none"], `${label} ${scope} must be an access level`).toContain(
      access,
    );
  }
};

const secretAccesses = (value: unknown): Array<string> => {
  const found: Array<string> = [];
  const visit = (node: unknown) => {
    if (typeof node === "string") {
      for (const match of node.matchAll(/\bsecrets\s*\.\s*([A-Z0-9_]+)/g)) {
        if (match[1] !== undefined) found.push(match[1]);
      }
      if (/\bsecrets\b/.test(node) && !/\bsecrets\s*\.\s*[A-Z0-9_]+/.test(node)) {
        found.push("*");
      }
      return;
    }
    if (Array.isArray(node)) {
      for (const child of node) visit(child);
      return;
    }
    if (!isRecord(node)) return;
    if (node.secrets === "inherit") found.push("inherit");
    for (const child of Object.values(node)) visit(child);
  };
  visit(value);
  return found.sort();
};

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
    headRef: "feature",
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
        collectPatch: workspace.inspectPatch.pipe(
          Effect.map((snapshot) => ({ snapshot, patch: "" })),
        ),
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
      }).pipe(Effect.provide(NodeServices.layer)),
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

  it.effect("WOI-011 the enabled workflow enforces the five-job credential boundary", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const contents = yield* fs.readFileString(
        path.resolve(import.meta.dirname, "../../../.github/workflows/pr-work-order.yml"),
      );
      const parsed = Yaml.parse(contents);
      const workflow = yield* Schema.decodeUnknownEffect(WorkOrderWorkflowFile)(parsed);
      expect(Object.keys(workflow.jobs).sort()).toEqual([
        "admit",
        "checks",
        "implement",
        "present",
        "publish",
      ]);
      assertExactPermissions(workflow.permissions, "workflow", {});
      const expectedPermissions = {
        admit: { contents: "read", "pull-requests": "write" },
        implement: { contents: "read" },
        checks: { contents: "read" },
        publish: { contents: "write", "pull-requests": "read" },
        present: { contents: "read", "pull-requests": "write" },
      } as const;
      const expectedSecrets = {
        admit: ["PR_WORK_ORDER_STATE_SECRET"],
        implement: ["OPENAI_API_KEY"],
        checks: [],
        publish: ["PR_WORK_ORDER_STATE_SECRET"],
        present: ["PR_WORK_ORDER_STATE_SECRET"],
      } as const;
      for (const [jobName, job] of Object.entries(workflow.jobs)) {
        const knownJob = yield* Schema.decodeUnknownEffect(
          Schema.Literals(["admit", "implement", "checks", "publish", "present"]),
        )(jobName);
        assertExactPermissions(
          job.permissions ?? workflow.permissions,
          `job ${jobName}`,
          expectedPermissions[knownJob],
        );
        expect(
          job.secrets,
          `${jobName} must not use reusable-workflow secret inheritance`,
        ).toBeUndefined();
        expect(job.uses, `${jobName} must not call a reusable workflow`).toBeUndefined();
        const checkouts = (job.steps ?? []).filter((step) =>
          (step.uses ?? "").startsWith("actions/checkout@"),
        );
        expect(checkouts, `${jobName} must check out trusted base code`).not.toHaveLength(0);
        const trustedCheckout = checkouts.find(
          (checkout) => checkout.with?.path === ".effect-agent/trusted",
        );
        expect(trustedCheckout).toBeDefined();
        expect(trustedCheckout?.with?.ref).toBe("${{ github.event.pull_request.base.sha }}");
        for (const checkout of checkouts) {
          expect(checkout.with?.["persist-credentials"]).toBe(false);
          expect(checkout.uses).toMatch(/^actions\/checkout@[0-9a-f]{40}$/);
        }
        expect(secretAccesses(job)).toEqual([...expectedSecrets[knownJob]]);
        for (const step of job.steps ?? []) {
          expect(step.uses ?? "").not.toContain("@effect-agent/pr-review");
          expect(step.uses).not.toBe("./action");
          expect(step.run ?? "").not.toContain("@effect-agent/pr-review");
          if (step.uses?.includes("/.effect-agent/trusted/work-order-action")) {
            expect(step.uses).toBe("./.effect-agent/trusted/work-order-action");
          }
          if (step.uses !== undefined && !step.uses.startsWith("./")) {
            expect(step.uses).toMatch(/@[0-9a-f]{40}$/);
          }
        }
      }
      for (const jobName of ["implement", "checks"] as const) {
        const headCheckout = workflow.jobs[jobName]?.steps?.find(
          (step) => step.with?.path === "worktree",
        );
        expect(headCheckout?.with?.ref).toBe("${{ github.event.pull_request.head.sha }}");
        expect(headCheckout?.with?.repository).toBe("${{ github.repository }}");
      }
      expect(workflow.jobs.publish?.steps?.some((step) => step.with?.path === "worktree")).toBe(
        false,
      );
      expect(workflow.jobs.present?.steps?.some((step) => step.with?.path === "worktree")).toBe(
        false,
      );
      const checkAction = workflow.jobs.checks?.steps?.find(
        (step) => step.with?.phase === "checks",
      );
      expect(checkAction?.with?.["check-container-image"]).toMatch(
        /^ghcr\.io\/voidzero-dev\/vite-plus:[^@]+@sha256:[0-9a-f]{64}$/,
      );
      expect(
        workflow.jobs.checks?.steps
          ?.filter((step) => step.run !== undefined)
          .map((step) => step.run),
      ).toEqual([]);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-011 secret scanning rejects a secrets context hidden by braces", () =>
    Effect.sync(() => {
      expect(
        secretAccesses({
          env: { KEY: "${{ format('{0}', 'x') && secrets.OPENAI_API_KEY }}" },
        }),
      ).toEqual(["OPENAI_API_KEY"]);
      expect(
        secretAccesses({
          env: { TOKEN: "${{ secrets.GITHUB_TOKEN }}" },
        }),
      ).toEqual(["GITHUB_TOKEN"]);
      expect(secretAccesses({ secrets: "inherit" })).toEqual(["inherit"]);
    }),
  );

  it("WOI-007 gives pull-request checks only a networkless worktree container", () => {
    const image = `docker.io/library/node:24@sha256:${"a".repeat(64)}`;
    const args = isolatedCheckContainerArguments({
      args: ["run", "ready"],
      command: "vp",
      containerImage: image,
      containerName: "effect-agent-check",
      network: "none",
      root: "/runner/work/repository",
      runnerUser: "1001:1001",
      runtimeRoot: "/runner/temp/work-order-runtime",
    });
    expect(args).toContain("none");
    expect(args).toContain("--read-only");
    expect(args).toContain("--cap-drop");
    expect(args).toContain("no-new-privileges");
    expect(args.filter((value) => value.startsWith("type=bind"))).toEqual([
      "type=bind,src=/runner/work/repository,dst=/workspace",
      "type=bind,src=/runner/temp/work-order-runtime,dst=/runtime",
    ]);
    expect(args.slice(args.indexOf(image))).toEqual([image, "vp", "run", "ready"]);
    expect(args.join(" ")).not.toMatch(/state|secret|token|docker\.sock/i);
  });

  it.effect("WOI-006 authenticates durable claimed and completed journal state", () =>
    Effect.gen(function* () {
      const authenticator = yield* WorkOrderJournalAuthenticator;
      const claimed = claimedState({
        eventId: "review-comment:1002",
        repository: REPOSITORY,
        pullRequestNumber: PULL,
        sourceCommentId: "1001",
        workOrderId: "wo-journal",
        workOrderDigest: DIGEST,
        expectedHeadSha: HEAD,
        runId: "run-1",
      });
      const body = yield* authenticator.render(claimed, "Implementation is pending.");
      const decoded = yield* authenticator.extract(body);
      expect(Option.getOrUndefined(decoded)).toEqual(claimed);

      const terminal = FailedTerminal.make({
        workOrderId: claimed.workOrderId,
        workOrderDigest: claimed.workOrderDigest,
        headSha: claimed.expectedHeadSha,
        errorTag: "RequiredCheckFailed",
        detail: "a required check failed",
      });
      const completed = completedState(claimed, terminal);
      const completedBody = yield* authenticator.render(completed, "No patch was published.");
      expect(Option.getOrUndefined(yield* authenticator.extract(completedBody))).toEqual(completed);

      const tampered = completedBody.replace(/([0-9a-f])(?= -->)/, (hex) =>
        hex === "0" ? "1" : "0",
      );
      expect(Option.isNone(yield* authenticator.extract(tampered))).toBe(true);
    }).pipe(
      Effect.provide(
        WorkOrderJournalAuthenticator.layer(Redacted.make("journal-test-secret-with-entropy")),
      ),
    ),
  );

  it.effect("WOI-008 derives complete patch paths and rejects path ambiguity or escapes", () =>
    Effect.gen(function* () {
      const valid = [
        "diff --git a/src/value.ts b/src/value.ts",
        `index ${"1".repeat(40)}..${"2".repeat(40)} 100644`,
        "--- a/src/value.ts",
        "+++ b/src/value.ts",
        "@@ -1 +1 @@",
        "-export const value = 1;",
        "+export const value = 42;",
        "",
      ].join("\n");
      expect(yield* completeModifiedPaths(valid)).toEqual(["src/value.ts"]);

      const rejected = yield* Effect.all([
        completeModifiedPaths(
          "diff --git a/src/old.ts b/src/new.ts\nrename from src/old.ts\nrename to src/new.ts\n",
        ).pipe(Effect.flip),
        completeModifiedPaths(
          "diff --git a/src/new.ts b/src/new.ts\nnew file mode 100644\n--- /dev/null\n+++ b/src/new.ts\n",
        ).pipe(Effect.flip),
        completeModifiedPaths(
          "diff --git a/../secret b/../secret\n--- a/../secret\n+++ b/../secret\n",
        ).pipe(Effect.flip),
        completeModifiedPaths("diff --git a/src/value.ts b/src/value.ts\n").pipe(Effect.flip),
      ]);
      expect(rejected.map((failure) => failure._tag)).toEqual([
        "PublisherVerificationFailure",
        "PublisherVerificationFailure",
        "PublisherVerificationFailure",
        "PublisherVerificationFailure",
      ]);
    }),
  );

  it.effect(
    "WOI-009 uses GitHub's expected-head commit CAS and preserves stale/uncertain outcomes",
    () =>
      Effect.gen(function* () {
        const published = yield* Schema.decodeUnknownEffect(GitCommitSha)("d".repeat(40));
        const order = yield* createWorkOrder(
          WorkOrderIdentity.make({
            version: 1,
            repository: REPOSITORY,
            pullRequestNumber: PULL,
            headSha: HEAD,
            source: {
              commentId: "1001",
              authorId: "7",
              authorLogin: "reviewer",
              commitSha: HEAD,
              path: FILE_PATH,
              body: "The exported answer must be 42.",
            },
            dispatch: {
              kind: "mention",
              eventId: "review-comment:1002",
              actorId: ACTOR_ID,
              actorLogin: "alice",
            },
          }),
        );
        const digest = yield* workOrderDigest(order);
        const baseContent = "export const value = 1;\n";
        const finalContent = "export const value = 42;\n";
        const gitBlob = (content: string) =>
          createHash("sha1")
            .update(`blob ${String(Buffer.byteLength(content))}\0`)
            .update(content)
            .digest("hex");
        const patch = [
          "diff --git a/src/value.ts b/src/value.ts",
          `index ${gitBlob(baseContent)}..${gitBlob(finalContent)} 100644`,
          "--- a/src/value.ts",
          "+++ b/src/value.ts",
          "@@ -1 +1 @@",
          "-export const value = 1;",
          "+export const value = 42;",
          "",
        ].join("\n");
        const patchDigest = yield* Schema.decodeUnknownEffect(PatchDigest)(
          createHash("sha256").update(patch).digest("hex"),
        );
        const admission = WorkOrderAdmission.make({
          version: 1,
          order,
          workOrderDigest: digest,
          journalCommentId: "2001",
          runId: "run-1",
        });
        const proposal = ProposedWorkOrder.make({
          order,
          workOrderDigest: digest,
          patch,
          patchDigest,
          changedPaths: [FILE_PATH],
          requiredChecks: ["ready"],
        });
        const checked = CheckedWorkOrder.make({
          version: 1,
          admission,
          proposal,
          checks: [WorkOrderCheckResult.make({ name: "ready", status: "passed", summary: "ok" })],
          files: [CheckedFile.make({ path: FILE_PATH, content: finalContent })],
        });
        yield* reproduceCheckedPatch({
          checked,
          expectedHeadFiles: new Map([[FILE_PATH, baseContent]]),
        });
        const substituted = CheckedWorkOrder.make({
          version: 1,
          admission,
          proposal,
          checks: checked.checks,
          files: [
            CheckedFile.make({ path: FILE_PATH, content: "export const value = 'forged';\n" }),
          ],
        });
        expect(
          (yield* reproduceCheckedPatch({
            checked: substituted,
            expectedHeadFiles: new Map([[FILE_PATH, baseContent]]),
          }).pipe(Effect.flip))._tag,
        ).toBe("WorkOrderValidationFailure");
        const pullWire = (headSha: typeof HEAD) => ({
          number: PULL,
          head: {
            sha: headSha,
            ref: "feature",
            repo: { full_name: REPOSITORY, fork: false },
          },
          base: { repo: { full_name: REPOSITORY, fork: false } },
        });
        const publishWith = (client: HttpClient.HttpClient) =>
          Effect.gen(function* () {
            const github = yield* WorkOrderGitHub;
            return yield* github.publish({ checked, message: "fix: implement work order" });
          }).pipe(
            Effect.provide(
              liveWorkOrderGitHubLayer({
                token: "github-token",
                apiUrl: "https://api.github.test",
                graphqlUrl: "https://api.github.test/graphql",
              }).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient)(client))),
            ),
          );

        const bodies = yield* Ref.make<ReadonlyArray<string>>([]);
        const successClient = HttpClient.make((request, url) => {
          if (request.method === "GET" && url.pathname === `/repos/${REPOSITORY}/pulls/${PULL}`) {
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new Response(JSON.stringify(pullWire(HEAD)), {
                  status: 200,
                  headers: { "content-type": "application/json" },
                }),
              ),
            );
          }
          if (request.method === "POST" && url.pathname === "/graphql") {
            const body =
              request.body._tag === "Uint8Array"
                ? new TextDecoder().decode(request.body.body)
                : "unexpected-body";
            return Ref.update(bodies, (previous) => [...previous, body]).pipe(
              Effect.as(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({
                      data: { createCommitOnBranch: { commit: { oid: published } } },
                    }),
                    { status: 200, headers: { "content-type": "application/json" } },
                  ),
                ),
              ),
            );
          }
          return Effect.die(new Error(`unexpected request ${request.method} ${url.href}`));
        });
        expect(yield* publishWith(successClient)).toBe(published);
        expect(yield* Ref.get(bodies)).toHaveLength(1);
        expect((yield* Ref.get(bodies))[0]).toContain(`"expectedHeadOid":"${HEAD}"`);
        expect((yield* Ref.get(bodies))[0]).toContain(`"path":"${FILE_PATH}"`);

        const staleClient = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify(pullWire(STALE)), {
                status: 200,
                headers: { "content-type": "application/json" },
              }),
            ),
          ),
        );
        expect((yield* publishWith(staleClient).pipe(Effect.flip))._tag).toBe(
          "StalePullRequestHead",
        );

        const gets = yield* Ref.make(0);
        const uncertainClient = HttpClient.make((request) => {
          if (request.method === "GET") {
            return Ref.updateAndGet(gets, (count) => count + 1).pipe(
              Effect.map(() =>
                HttpClientResponse.fromWeb(
                  request,
                  new Response(JSON.stringify(pullWire(HEAD)), {
                    status: 200,
                    headers: { "content-type": "application/json" },
                  }),
                ),
              ),
            );
          }
          return Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response("unconfirmed", { status: 503 })),
          );
        });
        const uncertain = yield* publishWith(uncertainClient).pipe(Effect.flip);
        expect(uncertain._tag).toBe("PublicationUncertainty");
        expect(yield* Ref.get(gets)).toBe(2);
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("the live GitHub adapter maps API wires onto ingress views", () =>
    Effect.sync(() => {
      const pull = pullRequestFromWire(REPOSITORY, {
        number: PULL,
        head: {
          sha: HEAD,
          ref: "feature",
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
