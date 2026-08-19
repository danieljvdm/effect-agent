import { createHash } from "node:crypto";

import {
  createWorkOrder,
  GitCommitSha,
  PatchDigest,
  ProposedWorkOrder,
  WorkOrderCheckResult,
  WorkOrderDigest,
  WorkOrderIdentity,
  workOrderDigest,
} from "@effect-agent/example-pr-work-orders";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  ConfigProvider,
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

import {
  isolatedCheckContainerArguments,
  reproduceCheckedPatch,
  restoreFreshCheckWorkspace,
} from "../src/action-checks.ts";
import {
  CheckedFile,
  CheckedWorkOrder,
  FailedTerminal,
  JournalComment,
  WorkOrderAdmission,
  type WorkOrderActionFailure,
  terminalMatchesAdmission,
} from "../src/action-contracts.ts";
import { liveWorkOrderGitHubLayer, WorkOrderGitHub } from "../src/action-github.ts";
import { admitWorkOrder, type AdmissionRequest } from "../src/action.ts";
import { constructWorkOrder } from "../src/construct.ts";
import {
  DEFAULT_MENTION_COMMAND,
  type DispatchTargetRejected,
  type DispatchUnauthorized,
  GitHubApiFailure,
  IngressPolicy,
  IngressPolicyConfig,
  type IngressStoreFailure,
  PlatformDelivery,
  PullRequestView,
  ReviewCommentView,
  type StaleCommentAnchor,
  type UntrustedPullRequest,
} from "../src/contracts.ts";
import { pullRequestFromWire, reviewCommentFromWire } from "../src/github-live.ts";
import type { GitHubApi } from "../src/github.ts";
import { makeFakeGitHub } from "../src/github.ts";
import {
  claimedState,
  completedState,
  journalStatesEqual,
  WorkOrderJournalAuthenticator,
} from "../src/journal.ts";
import { parseDispatchTarget } from "../src/parse-event.ts";
import { completeModifiedPaths } from "../src/patch.ts";

const HEAD = Schema.decodeUnknownSync(GitCommitSha)("a".repeat(40));
const STALE = Schema.decodeUnknownSync(GitCommitSha)("b".repeat(40));
const DIGEST = Schema.decodeUnknownSync(WorkOrderDigest)("c".repeat(64));
const REPOSITORY = "acme/widgets";
const PULL = 17;
const ACTOR_ID = "42";
const STATE_AUTHOR_ID = "41898282";
const JOURNAL_SECRET = Redacted.make("ingress-test-journal-secret-with-entropy");
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
      const exactReference =
        /\bsecrets\s*(?:\.\s*([A-Za-z_][A-Za-z0-9_]*)|\[\s*(['"])([A-Za-z_][A-Za-z0-9_]*)\2\s*\])/g;
      for (const match of node.matchAll(exactReference)) {
        const name = match[1] ?? match[3];
        if (name !== undefined) found.push(name.toUpperCase());
      }
      if (/\bsecrets\b/.test(node.replaceAll(exactReference, ""))) {
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

const typedAdmission = admitWorkOrder(null as unknown as AdmissionRequest);
type TypedAdmissionError = Effect.Error<typeof typedAdmission>;
type TypedAdmissionServices = Effect.Services<typeof typedAdmission>;
type AdmissionKeepsTarget = Assert<
  Equal<Extract<TypedAdmissionError, DispatchTargetRejected>, DispatchTargetRejected>
>;
type AdmissionKeepsUnauthorized = Assert<
  Equal<Extract<TypedAdmissionError, DispatchUnauthorized>, DispatchUnauthorized>
>;
type AdmissionKeepsUntrusted = Assert<
  Equal<Extract<TypedAdmissionError, UntrustedPullRequest>, UntrustedPullRequest>
>;
type AdmissionKeepsStale = Assert<
  Equal<Extract<TypedAdmissionError, StaleCommentAnchor>, StaleCommentAnchor>
>;
type AdmissionKeepsGitHub = Assert<
  Equal<Extract<TypedAdmissionError, GitHubApiFailure>, GitHubApiFailure>
>;
type AdmissionKeepsJournal = Assert<
  Equal<Extract<TypedAdmissionError, IngressStoreFailure>, IngressStoreFailure>
>;
type AdmissionKeepsConflict = Assert<
  Equal<Extract<TypedAdmissionError, WorkOrderActionFailure>, WorkOrderActionFailure>
>;
type AdmissionUnknownExcluded = Assert<
  Equal<unknown extends TypedAdmissionError ? true : false, false>
>;
type AdmissionRequiresPolicy = Assert<
  Equal<Extract<TypedAdmissionServices, IngressPolicy>, IngressPolicy>
>;
type AdmissionRequiresGitHub = Assert<Equal<Extract<TypedAdmissionServices, GitHubApi>, GitHubApi>>;
type AdmissionRequiresJournal = Assert<
  Equal<Extract<TypedAdmissionServices, WorkOrderGitHub>, WorkOrderGitHub>
>;
type AdmissionRequiresAuthenticator = Assert<
  Equal<
    Extract<TypedAdmissionServices, WorkOrderJournalAuthenticator>,
    WorkOrderJournalAuthenticator
  >
>;

const policyConfig = IngressPolicyConfig.make({
  repository: REPOSITORY,
  pullRequestNumber: PULL,
  authorizedActorIds: [ACTOR_ID],
  mentionCommand: DEFAULT_MENTION_COMMAND,
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
  readonly commentId?: number;
  readonly body?: string;
  readonly senderId?: number;
  readonly senderLogin?: string;
  readonly repository?: string;
  readonly pullRequestNumber?: number;
}) => ({
  action: "created" as const,
  comment: {
    id: overrides?.commentId ?? 1002,
    ...(overrides && "inReplyTo" in overrides && overrides.inReplyTo !== undefined
      ? { in_reply_to_id: overrides.inReplyTo }
      : {}),
    body: overrides?.body ?? DEFAULT_MENTION_COMMAND,
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

const mentionDelivery = (
  payload: unknown,
  overrides?: {
    readonly deliveryId?: string;
    readonly eventName?: string;
  },
) =>
  PlatformDelivery.make({
    deliveryId: overrides?.deliveryId ?? "review-comment:1002",
    eventName: overrides?.eventName ?? "pull_request_review_comment",
    rawBody: JSON.stringify(payload),
  });

const makeFakeJournal = (options?: {
  readonly initial?: ReadonlyArray<JournalComment>;
  readonly echo?: (body: string) => string;
  readonly authorId?: string;
}) => {
  const comments: Array<JournalComment> = [...(options?.initial ?? [])];
  let created = 0;
  const unused = (operation: string) =>
    GitHubApiFailure.make({ operation, reason: "not used by admission" });
  const layer = Layer.succeed(
    WorkOrderGitHub,
    WorkOrderGitHub.of({
      getPullRequest: () => unused("get pull request"),
      listReviewComments: () => Effect.sync(() => [...comments]),
      createReply: (input) =>
        Effect.sync(() => {
          created += 1;
          const comment = JournalComment.make({
            id: `journal-${String(1000 + created)}`,
            authorId: options?.authorId ?? STATE_AUTHOR_ID,
            inReplyToId: input.commentId,
            body: options?.echo === undefined ? input.body : options.echo(input.body),
          });
          comments.push(comment);
          return comment;
        }),
      updateComment: () => unused("update comment"),
      getFileContent: () => unused("get file content"),
      publish: () => unused("publish"),
    }),
  );
  return {
    layer,
    created: () => created,
    comments: () => [...comments] as ReadonlyArray<JournalComment>,
  };
};

const readAdmissionArtifactFile = (file: string) =>
  Effect.flatMap(FileSystem.FileSystem, (files) =>
    files
      .readFileString(file)
      .pipe(
        Effect.flatMap((text) =>
          Schema.decodeUnknownEffect(Schema.fromJsonString(WorkOrderAdmission))(text),
        ),
      ),
  );

const withAdmission = <A, E, R>(
  input: {
    readonly comments?: ReadonlyMap<string, ReviewCommentView>;
    readonly pull?: PullRequestView;
    readonly journal?: ReturnType<typeof makeFakeJournal>;
  },
  apply: (env: {
    readonly journal: ReturnType<typeof makeFakeJournal>;
    readonly outputs: Effect.Effect<string, never, FileSystem.FileSystem>;
    readonly admission: ReturnType<typeof readAdmissionArtifactFile>;
  }) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "pr-work-order-admit-" });
      const stateDirectory = path.join(directory, "state");
      const outputsFile = path.join(directory, "outputs.txt");
      const github = makeFakeGitHub({
        repository: REPOSITORY,
        pullRequest: input.pull ?? pullRequest(),
        comments: input.comments ?? new Map([["1001", targetComment()]]),
      });
      const journal = input.journal ?? makeFakeJournal();
      const outputs = Effect.flatMap(FileSystem.FileSystem, (files) =>
        files.readFileString(outputsFile).pipe(Effect.orElseSucceed(() => "")),
      );
      const admission = readAdmissionArtifactFile(path.join(stateDirectory, "admission.json"));
      return yield* apply({ journal, outputs, admission }).pipe(
        Effect.provide(
          Layer.mergeAll(
            github.layer,
            journal.layer,
            IngressPolicy.layer(policyConfig),
            WorkOrderJournalAuthenticator.layer(JOURNAL_SECRET),
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({
                EFFECT_AGENT_STATE_AUTHOR_ID: STATE_AUTHOR_ID,
                EFFECT_AGENT_ARTIFACT_DIRECTORY: stateDirectory,
                GITHUB_OUTPUT: outputsFile,
              }),
            ),
          ),
        ),
      );
    }),
  );

describe("PR work-order ingress", () => {
  it.effect(
    "WOI-013 keeps expected targeting, admission, journal, and publication failures in E and dependencies in R",
    () =>
      Effect.sync(() => {
        const proofs = {
          admissionKeepsTarget: true as AdmissionKeepsTarget,
          admissionKeepsUnauthorized: true as AdmissionKeepsUnauthorized,
          admissionKeepsUntrusted: true as AdmissionKeepsUntrusted,
          admissionKeepsStale: true as AdmissionKeepsStale,
          admissionKeepsGitHub: true as AdmissionKeepsGitHub,
          admissionKeepsJournal: true as AdmissionKeepsJournal,
          admissionKeepsConflict: true as AdmissionKeepsConflict,
          admissionUnknownExcluded: true as AdmissionUnknownExcluded,
          admissionRequiresPolicy: true as AdmissionRequiresPolicy,
          admissionRequiresGitHub: true as AdmissionRequiresGitHub,
          admissionRequiresJournal: true as AdmissionRequiresJournal,
          admissionRequiresAuthenticator: true as AdmissionRequiresAuthenticator,
        };
        expect(proofs).toEqual({
          admissionKeepsTarget: true,
          admissionKeepsUnauthorized: true,
          admissionKeepsUntrusted: true,
          admissionKeepsStale: true,
          admissionKeepsGitHub: true,
          admissionKeepsJournal: true,
          admissionKeepsConflict: true,
          admissionUnknownExcluded: true,
          admissionRequiresPolicy: true,
          admissionRequiresGitHub: true,
          admissionRequiresJournal: true,
          admissionRequiresAuthenticator: true,
        });
      }),
  );

  it.effect(
    "WOI-001 WOI-005 WOI-006 an exact mention reply admits one work order and persists an authenticated claim",
    () =>
      withAdmission({}, ({ journal, outputs, admission }) =>
        Effect.gen(function* () {
          const delivery = mentionDelivery(yield* loadFixture("mention-reply.json"), {
            deliveryId: "review-comment:2001",
          });
          yield* admitWorkOrder({ delivery, runId: "run-1" });
          expect(journal.created()).toBe(1);
          const created = journal.comments()[0];
          expect(created).toMatchObject({ authorId: STATE_AUTHOR_ID, inReplyToId: "1001" });
          const authenticator = yield* WorkOrderJournalAuthenticator;
          const state = Option.getOrUndefined(yield* authenticator.extract(created?.body ?? ""));
          expect(state).toMatchObject({
            _tag: "claimed",
            eventId: "review-comment:2001",
            repository: REPOSITORY,
            pullRequestNumber: PULL,
            sourceCommentId: "1001",
            expectedHeadSha: HEAD,
            runId: "run-1",
          });
          const admitted = yield* admission;
          expect(admitted.order.workOrderId).toBe(state?.workOrderId);
          expect(admitted.workOrderDigest).toBe(state?.workOrderDigest);
          expect(admitted.journalCommentId).toBe(created?.id);
          const written = yield* outputs;
          expect(written).toContain("should-run=true\n");
          expect(written).toContain("duplicate=false\n");
          expect(written).toContain("stored-outcome=claimed\n");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-001 a conversation comment, review summary, approximate command, missing reply target, or pathless target is rejected before any claim",
    () =>
      withAdmission(
        { comments: new Map([["1001", targetComment({ omitPath: true })]]) },
        ({ journal }) =>
          Effect.gen(function* () {
            const admit = (delivery: PlatformDelivery) =>
              admitWorkOrder({ delivery, runId: "run-1" }).pipe(Effect.flip);
            const conversation = yield* admit(
              mentionDelivery(yield* loadFixture("issue-comment.json"), {
                eventName: "issue_comment",
              }),
            );
            const review = yield* admit(
              mentionDelivery(yield* loadFixture("review-summary.json"), {
                eventName: "pull_request_review",
              }),
            );
            const approximate = yield* admit(
              mentionDelivery(
                mentionPayload({ inReplyTo: 1001, body: `${DEFAULT_MENTION_COMMAND} please` }),
              ),
            );
            const untargeted = yield* admit(
              mentionDelivery(yield* loadFixture("mention-no-reply.json")),
            );
            const pathless = yield* admit(mentionDelivery(mentionPayload({ inReplyTo: 1001 })));
            expect(conversation._tag).toBe("DispatchTargetRejected");
            expect(review._tag).toBe("DispatchTargetRejected");
            expect(approximate._tag).toBe("DispatchTargetRejected");
            expect(untargeted._tag).toBe("DispatchTargetRejected");
            expect(pathless._tag).toBe("DispatchTargetRejected");
            expect(journal.created()).toBe(0);
          }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-002 an unauthorized actor id is rejected even when the login matches a configured human",
    () =>
      withAdmission({}, ({ journal }) =>
        Effect.gen(function* () {
          const rejected = yield* admitWorkOrder({
            delivery: mentionDelivery(yield* loadFixture("unauthorized-actor.json")),
            runId: "run-1",
          }).pipe(Effect.flip);
          expect(rejected).toMatchObject({ _tag: "DispatchUnauthorized", actorId: "99" });
          expect(journal.created()).toBe(0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-003 a fork or foreign repository is rejected", () =>
    Effect.gen(function* () {
      yield* withAdmission({ pull: pullRequest({ headIsFork: true }) }, ({ journal }) =>
        Effect.gen(function* () {
          const rejected = yield* admitWorkOrder({
            delivery: mentionDelivery(mentionPayload({ inReplyTo: 1001 })),
            runId: "run-1",
          }).pipe(Effect.flip);
          expect(rejected._tag).toBe("UntrustedPullRequest");
          expect(journal.created()).toBe(0);
        }),
      );
      yield* withAdmission({}, ({ journal }) =>
        Effect.gen(function* () {
          const rejected = yield* admitWorkOrder({
            delivery: mentionDelivery(yield* loadFixture("foreign-repo.json")),
            runId: "run-1",
          }).pipe(Effect.flip);
          expect(rejected._tag).toBe("UntrustedPullRequest");
          expect(journal.created()).toBe(0);
        }),
      );
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-004 a comment anchored to an older SHA is rejected against the current head", () =>
    withAdmission(
      { comments: new Map([["1001", targetComment({ commitSha: STALE })]]) },
      ({ journal }) =>
        Effect.gen(function* () {
          const rejected = yield* admitWorkOrder({
            delivery: mentionDelivery(mentionPayload({ inReplyTo: 1001 })),
            runId: "run-1",
          }).pipe(Effect.flip);
          expect(rejected).toMatchObject({
            _tag: "StaleCommentAnchor",
            sourceSha: STALE,
            headSha: HEAD,
          });
          expect(journal.created()).toBe(0);
        }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 duplicate delivery of the same dispatch returns the stored outcome", () =>
    withAdmission({}, ({ journal, outputs }) =>
      Effect.gen(function* () {
        const delivery = mentionDelivery(mentionPayload({ inReplyTo: 1001 }));
        yield* admitWorkOrder({ delivery, runId: "run-1" });
        yield* admitWorkOrder({ delivery, runId: "run-2" });
        expect(journal.created()).toBe(1);
        const written = yield* outputs;
        expect(written).toContain("duplicate=true\n");
        expect(written).toContain("stored-outcome=incomplete\n");
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 a second explicit dispatch has a distinct work-order id", () =>
    withAdmission({}, ({ journal }) =>
      Effect.gen(function* () {
        yield* admitWorkOrder({
          delivery: mentionDelivery(mentionPayload({ inReplyTo: 1001 })),
          runId: "run-1",
        });
        yield* admitWorkOrder({
          delivery: mentionDelivery(mentionPayload({ inReplyTo: 1001, commentId: 1003 }), {
            deliveryId: "review-comment:1003",
          }),
          runId: "run-2",
        });
        expect(journal.created()).toBe(2);
        const authenticator = yield* WorkOrderJournalAuthenticator;
        const states = yield* Effect.forEach(journal.comments(), (comment) =>
          authenticator.extract(comment.body).pipe(Effect.map(Option.getOrUndefined)),
        );
        expect(states[0]?.workOrderId).toBeDefined();
        expect(states[1]?.workOrderId).toBeDefined();
        expect(states[0]?.workOrderId).not.toBe(states[1]?.workOrderId);
        expect(states[0]?.eventId).not.toBe(states[1]?.eventId);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-006 an interrupted claimed attempt returns the stored incomplete outcome and is not replayed",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const github = makeFakeGitHub({
            repository: REPOSITORY,
            pullRequest: pullRequest(),
            comments: new Map([["1001", targetComment()]]),
          });
          const delivery = mentionDelivery(mentionPayload({ inReplyTo: 1001 }));
          const order = yield* Effect.gen(function* () {
            const target = yield* parseDispatchTarget(delivery);
            return yield* constructWorkOrder(target, delivery.deliveryId);
          }).pipe(Effect.provide(Layer.mergeAll(github.layer, IngressPolicy.layer(policyConfig))));
          const digest = yield* workOrderDigest(order);
          const claimed = claimedState({
            eventId: delivery.deliveryId,
            repository: REPOSITORY,
            pullRequestNumber: PULL,
            sourceCommentId: "1001",
            workOrderId: order.workOrderId,
            workOrderDigest: digest,
            expectedHeadSha: HEAD,
            runId: "run-0",
          });
          const authenticator = WorkOrderJournalAuthenticator.layer(JOURNAL_SECRET);
          const claimedBody = yield* Effect.flatMap(WorkOrderJournalAuthenticator, (journal) =>
            journal.render(claimed, "Implementation is pending."),
          ).pipe(Effect.provide(authenticator));
          const interrupted = makeFakeJournal({
            initial: [
              JournalComment.make({
                id: "journal-1",
                authorId: STATE_AUTHOR_ID,
                inReplyToId: "1001",
                body: claimedBody,
              }),
            ],
          });
          yield* withAdmission({ journal: interrupted }, ({ journal, outputs }) =>
            Effect.gen(function* () {
              yield* admitWorkOrder({ delivery, runId: "run-1" });
              expect(journal.created()).toBe(0);
              const written = yield* outputs;
              expect(written).toContain("should-run=false\n");
              expect(written).toContain("duplicate=true\n");
              expect(written).toContain("stored-outcome=incomplete\n");
            }),
          );
          const failedBody = yield* Effect.flatMap(WorkOrderJournalAuthenticator, (journal) =>
            journal.render(
              completedState(
                claimed,
                FailedTerminal.make({
                  workOrderId: order.workOrderId,
                  workOrderDigest: digest,
                  headSha: HEAD,
                  errorTag: "RequiredCheckFailed",
                  detail: "a required check failed",
                }),
              ),
              "No patch was published.",
            ),
          ).pipe(Effect.provide(authenticator));
          const settledJournal = makeFakeJournal({
            initial: [
              JournalComment.make({
                id: "journal-1",
                authorId: STATE_AUTHOR_ID,
                inReplyToId: "1001",
                body: failedBody,
              }),
            ],
          });
          yield* withAdmission({ journal: settledJournal }, ({ journal, outputs }) =>
            Effect.gen(function* () {
              yield* admitWorkOrder({ delivery, runId: "run-1" });
              expect(journal.created()).toBe(0);
              expect(yield* outputs).toContain("stored-outcome=failed\n");
            }),
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "WOI-006 admission fails closed on a mutated echo, foreign author, or ambiguous journal",
    () =>
      Effect.gen(function* () {
        const delivery = mentionDelivery(mentionPayload({ inReplyTo: 1001 }));
        const admitFlipped = withAdmission(
          { journal: makeFakeJournal({ echo: (body) => body.replace("pending", "tampered") }) },
          () => admitWorkOrder({ delivery, runId: "run-1" }).pipe(Effect.flip),
        );
        const mutated = yield* admitFlipped;
        expect(mutated).toMatchObject({
          _tag: "WorkOrderActionFailure",
          errorTag: "AdmissionConflict",
        });
        const foreign = yield* withAdmission(
          { journal: makeFakeJournal({ authorId: "999" }) },
          () => admitWorkOrder({ delivery, runId: "run-1" }).pipe(Effect.flip),
        );
        expect(foreign).toMatchObject({
          _tag: "WorkOrderActionFailure",
          errorTag: "AdmissionConflict",
        });
        yield* withAdmission({}, ({ journal }) =>
          Effect.gen(function* () {
            yield* admitWorkOrder({ delivery, runId: "run-1" });
            const claim = journal.comments()[0];
            expect(claim).toBeDefined();
            if (claim === undefined) return;
            const ambiguous = makeFakeJournal({
              initial: [claim, JournalComment.make({ ...claim, id: "journal-2" })],
            });
            const conflict = yield* withAdmission({ journal: ambiguous }, () =>
              admitWorkOrder({ delivery, runId: "run-2" }).pipe(Effect.flip),
            );
            expect(conflict).toMatchObject({
              _tag: "WorkOrderActionFailure",
              errorTag: "AdmissionConflict",
            });
          }),
        );
      }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-011 the enabled workflow enforces the five-job credential boundary", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const contents = yield* fs.readFileString(
        path.resolve(import.meta.dirname, "../../../.github/workflows/pr-work-order.yml"),
      );
      const parsed = Yaml.parse(contents);
      expect(isRecord(parsed)).toBe(true);
      if (!isRecord(parsed)) return;
      expect(isRecord(parsed.concurrency)).toBe(true);
      if (!isRecord(parsed.concurrency)) return;
      expect(Object.keys(parsed.concurrency).sort()).toEqual(["cancel-in-progress", "group"]);
      expect(parsed.concurrency.group).toBe(
        "pr-work-order-${{ github.repository_id }}-${{ github.event.comment.id }}",
      );
      expect(parsed.concurrency["cancel-in-progress"]).toBe(false);
      expect(isRecord(parsed.jobs)).toBe(true);
      if (!isRecord(parsed.jobs)) return;
      const rawJobs = parsed.jobs;
      expect(
        secretAccesses(
          Object.fromEntries(Object.entries(parsed).filter(([key]) => key !== "jobs")),
        ),
        "workflow-level configuration must not expose a secret to every job",
      ).toEqual([]);
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
        const rawJob = rawJobs[jobName];
        expect(isRecord(rawJob), `${jobName} must remain a raw YAML job object`).toBe(true);
        if (!isRecord(rawJob)) continue;
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
        expect(secretAccesses(rawJob)).toEqual([...expectedSecrets[knownJob]]);
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
      const workflowText = JSON.stringify(workflow);
      expect(workflowText).not.toContain("state/terminal.json");
      expect(workflowText).toContain("state/implementation-terminal.json");
      expect(workflowText).toContain("state/checks-terminal.json");
      expect(workflowText).toContain("state/publication-terminal.json");
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
      expect(
        secretAccesses({
          env: {
            TOKENS:
              "${{ secrets.OPENAI_API_KEY }} ${{ secrets['EXTRA_TOKEN'] }} ${{ secrets[inputs.dynamic] }}",
          },
        }),
      ).toEqual(["*", "EXTRA_TOKEN", "OPENAI_API_KEY"]);
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

    const installArgs = isolatedCheckContainerArguments({
      args: ["install", "--frozen-lockfile", "--ignore-scripts"],
      command: "/home/vp/.vite-plus/bin/vp",
      containerImage: image,
      containerName: "effect-agent-install",
      network: "bridge",
      root: "/runner/work/repository",
      runnerUser: "1001:1001",
      runtimeRoot: "/runner/temp/work-order-runtime",
    });
    expect(installArgs.slice(installArgs.indexOf(image))).toEqual([
      image,
      "/home/vp/.vite-plus/bin/vp",
      "install",
      "--frozen-lockfile",
      "--ignore-scripts",
    ]);
    expect(installArgs.find((value) => value.startsWith("PATH="))).not.toContain("/workspace");
  });

  it.effect("WOI-007 restores an independent checkout and runtime for every required check", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const repositoryPath = yield* fs.makeTempDirectoryScoped({ prefix: "check-source-" });
      const runtimeRoot = yield* fs.makeTempDirectoryScoped({ prefix: "check-runtime-" });
      yield* fs.writeFileString(path.join(repositoryPath, "tracked.txt"), "validated patch\n");
      yield* fs.makeDirectory(path.join(runtimeRoot, ".vite-plus"), { recursive: true });
      yield* fs.writeFileString(path.join(runtimeRoot, ".vite-plus", "cache"), "trusted cache\n");

      const first = yield* restoreFreshCheckWorkspace({
        repositoryPath,
        runtimeRoot,
        checkName: "first",
        index: 0,
      });
      const second = yield* restoreFreshCheckWorkspace({
        repositoryPath,
        runtimeRoot,
        checkName: "second",
        index: 1,
      });
      yield* fs.writeFileString(path.join(first.repositoryPath, "tracked.txt"), "poisoned\n");
      yield* fs.writeFileString(path.join(first.runtimeRoot, ".vite-plus", "cache"), "poisoned\n");

      expect(first.repositoryPath).not.toBe(second.repositoryPath);
      expect(first.runtimeRoot).not.toBe(second.runtimeRoot);
      expect(yield* fs.readFileString(path.join(second.repositoryPath, "tracked.txt"))).toBe(
        "validated patch\n",
      );
      expect(yield* fs.readFileString(path.join(second.runtimeRoot, ".vite-plus", "cache"))).toBe(
        "trusted cache\n",
      );
      const released = yield* restoreFreshCheckWorkspace({
        repositoryPath,
        runtimeRoot,
        checkName: "released",
        index: 2,
      }).pipe(Effect.scoped);
      expect(yield* fs.exists(released.repositoryPath)).toBe(false);
      expect(yield* fs.exists(released.runtimeRoot)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("WOI-006 authenticates claimed and completed GitHub journal state", () =>
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
      const fakeMarker = `<!-- effect-agent-work-order:v1:e30=.${"0".repeat(64)} -->`;
      const shadowed = yield* authenticator.render(claimed, fakeMarker);
      expect(Option.getOrUndefined(yield* authenticator.extract(shadowed))).toEqual(claimed);

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
      expect(journalStatesEqual(claimed, claimed)).toBe(true);
      expect(journalStatesEqual(claimed, completed)).toBe(false);
      expect(
        journalStatesEqual(
          completed,
          completedState(
            claimed,
            FailedTerminal.make({ ...terminal, detail: "a different authenticated result" }),
          ),
        ),
      ).toBe(false);

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
      const headerLikePayload = [
        "diff --git a/src/value.ts b/src/value.ts",
        `index ${"1".repeat(40)}..${"2".repeat(40)} 100644`,
        "--- a/src/value.ts",
        "+++ b/src/value.ts",
        "@@ -1 +1 @@",
        "--- const oldDivider = true;",
        "+++ const newDivider = true;",
        "",
      ].join("\n");
      expect(yield* completeModifiedPaths(headerLikePayload)).toEqual(["src/value.ts"]);
      const withoutFinalNewlines = [
        "diff --git a/src/value.ts b/src/value.ts",
        `index ${"1".repeat(40)}..${"2".repeat(40)} 100644`,
        "--- a/src/value.ts",
        "+++ b/src/value.ts",
        "@@ -1 +1 @@",
        "-old",
        "\\ No newline at end of file",
        "+new",
        "\\ No newline at end of file",
        "",
      ].join("\n");
      expect(yield* completeModifiedPaths(withoutFinalNewlines)).toEqual(["src/value.ts"]);

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
        completeModifiedPaths(
          `${valid}--- a/src/hidden.ts\n+++ b/src/hidden.ts\n@@ -1 +1 @@\n-old\n+new\n`,
        ).pipe(Effect.flip),
        completeModifiedPaths(
          `diff --git a/src/value.ts b/src/value.ts\n--- a/src/value.ts\n+++ b/src/value.ts\n@@ -1,2 +1,2 @@\n-old\n+new\n`,
        ).pipe(Effect.flip),
      ]);
      expect(rejected.map((failure) => failure._tag)).toEqual([
        "PublisherVerificationFailure",
        "PublisherVerificationFailure",
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
        expect(
          terminalMatchesAdmission(
            admission,
            FailedTerminal.make({
              workOrderId: order.workOrderId,
              workOrderDigest: digest,
              headSha: HEAD,
              errorTag: "RequiredCheckFailed",
              detail: "failed",
            }),
          ),
        ).toBe(true);
        expect(
          terminalMatchesAdmission(
            admission,
            FailedTerminal.make({
              workOrderId: order.workOrderId,
              workOrderDigest: digest,
              headSha: STALE,
              errorTag: "RequiredCheckFailed",
              detail: "failed",
            }),
          ),
        ).toBe(false);
        expect(
          terminalMatchesAdmission(
            admission,
            FailedTerminal.make({
              workOrderId: "wo-forged",
              workOrderDigest: digest,
              headSha: HEAD,
              errorTag: "RequiredCheckFailed",
              detail: "failed",
            }),
          ),
        ).toBe(false);
        expect(
          terminalMatchesAdmission(
            admission,
            FailedTerminal.make({
              workOrderId: order.workOrderId,
              workOrderDigest: DIGEST,
              headSha: HEAD,
              errorTag: "RequiredCheckFailed",
              detail: "failed",
            }),
          ),
        ).toBe(false);
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
              Effect.map((count) =>
                HttpClientResponse.fromWeb(
                  request,
                  new Response(JSON.stringify(pullWire(count === 1 ? HEAD : STALE)), {
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
        if (uncertain._tag === "PublicationUncertainty") {
          expect(uncertain.observedHeadSha).toBe(STALE);
        }
        expect(yield* Ref.get(gets)).toBe(2);
      }).pipe(Effect.provide(NodeServices.layer)),
    // Reproduces the patch through real git worktrees; the vitest default 5s
    // budget flakes on a loaded machine.
    30_000,
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
