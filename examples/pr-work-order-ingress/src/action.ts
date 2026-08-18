import {
  makeImplementationAgent,
  normalizeWorkspacePath,
  prepareWorkOrder,
  workOrderDigest,
  workOrderIdFor,
  workOrderIdentityOf,
  localGitWorkOrderHostLayer,
} from "@effect-agent/example-pr-work-orders";
import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  Config,
  Console,
  Crypto,
  Duration,
  Effect,
  Encoding,
  FileSystem,
  Layer,
  Option,
  Redacted,
  Schema,
} from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import {
  readAdmissionArtifact,
  readCheckedArtifact,
  readProposalArtifact,
  readTerminalArtifactOption,
  writeArtifact,
} from "./action-artifacts.ts";
import { reproduceCheckedPatch, validateProposedWorkOrder } from "./action-checks.ts";
import {
  ActionCheckSpecs,
  type CheckedWorkOrder,
  FailedTerminal,
  type JournalClaimed,
  PublishedTerminal,
  type WorkOrderAdmission,
  WorkOrderActionFailure,
  WorkOrderAdmission as WorkOrderAdmissionSchema,
  type WorkOrderTerminal,
  terminalMatchesAdmission,
  terminalFromSettlement,
} from "./action-contracts.ts";
import { liveWorkOrderGitHubLayer, WorkOrderGitHub } from "./action-github.ts";
import { authenticateDelivery, ObservedActionsIdentity } from "./authenticate.ts";
import { constructWorkOrder } from "./construct.ts";
import {
  DEFAULT_MENTION_COMMAND,
  DEFAULT_REACTION_CONTENT,
  IngressPolicy,
  IngressPolicyConfig,
  PlatformDelivery,
  PublisherVerificationFailure,
} from "./contracts.ts";
import { liveGitHubApiLayer } from "./github-live.ts";
import {
  claimedState,
  completedState,
  journalStatesEqual,
  WorkOrderJournalAuthenticator,
} from "./journal.ts";
import { parseDispatchTarget } from "./parse-event.ts";
import { completeModifiedPaths } from "./patch.ts";

const ActionPhase = Schema.Literals(["admit", "implement", "checks", "publish", "present"]);
type ActionPhase = typeof ActionPhase.Type;

const ActionsEvent = Schema.Struct({
  comment: Schema.Struct({
    id: Schema.Int.check(Schema.isGreaterThan(0)),
    in_reply_to_id: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  }),
  pull_request: Schema.Struct({
    number: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
});

interface AdmissionRuntimeContext {
  readonly repository: string;
  readonly eventName: string;
  readonly rawBody: string;
  readonly runId: string;
  readonly eventId: string;
  readonly delivery: PlatformDelivery;
  readonly policy: IngressPolicyConfig;
}

export interface AdmissionRequest {
  readonly delivery: PlatformDelivery;
  readonly runId: string;
}

const StringArray = Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
  Schema.isMaxLength(100),
);
const StableActorId = Schema.String.check(Schema.isPattern(/^[1-9][0-9]{0,39}$/));
const JournalSecret = Schema.NonEmptyString.check(
  Schema.isMinLength(32),
  Schema.isMaxLength(1_024),
);

const ErrorView = Schema.Struct({
  _tag: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  reason: Schema.optionalKey(Schema.String),
  detail: Schema.optionalKey(Schema.String),
  operation: Schema.optionalKey(Schema.String),
});

const describeError = (error: unknown): { readonly tag: string; readonly detail: string } =>
  Option.match(Schema.decodeUnknownOption(ErrorView)(error), {
    onNone: () => ({ tag: "WorkOrderFailure", detail: "work-order phase failed" }),
    onSome: (view) => ({
      tag: view._tag,
      detail: (view.detail ?? view.reason ?? view.operation ?? view._tag).slice(0, 2_048),
    }),
  });

const terminalFailure = (admission: WorkOrderAdmission, error: unknown): FailedTerminal => {
  const described = describeError(error);
  return FailedTerminal.make({
    workOrderId: admission.order.workOrderId,
    workOrderDigest: admission.workOrderDigest,
    headSha: admission.order.headSha,
    errorTag: described.tag,
    detail: described.detail,
  });
};

const outputLine = (name: string, value: string): string =>
  `${name}=${value.replaceAll("\n", " ").slice(0, 1_000)}\n`;

const writeOutputs = Effect.fn("workOrderAction.writeOutputs")(function* (
  entries: ReadonlyArray<readonly [string, string]>,
) {
  const target = yield* Config.string("GITHUB_OUTPUT").pipe(Config.withDefault(""));
  if (target === "") return;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(
    target,
    entries.map(([name, value]) => outputLine(name, value)).join(""),
    { flag: "a" },
  );
});

const parseJsonConfig = Effect.fn("workOrderAction.parseJsonConfig")(function* <
  S extends Schema.Top,
>(name: string, schema: S) {
  const text = yield* Config.string(name).pipe(Config.withDefault("[]"));
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError((error) =>
      WorkOrderActionFailure.make({
        phase: "admit",
        errorTag: "InvalidActionConfiguration",
        detail: `${name}: ${error.message}`.slice(0, 2_048),
      }),
    ),
  );
});

const actorIds = Effect.fn("workOrderAction.actorIds")(function* () {
  const raw = yield* Config.nonEmptyString("EFFECT_AGENT_AUTHORIZED_ACTOR_IDS");
  return yield* Schema.decodeUnknownEffect(Schema.NonEmptyArray(StableActorId))(
    raw
      .split(",")
      .map((part) => part.trim())
      .filter((part) => part.length > 0),
  ).pipe(
    Effect.mapError((error) =>
      WorkOrderActionFailure.make({
        phase: "admit",
        errorTag: "InvalidActionConfiguration",
        detail: error.message.slice(0, 2_048),
      }),
    ),
  );
});

const stableActorId = (name: string) => Config.schema(StableActorId, name);

const actionChecks = () => parseJsonConfig("EFFECT_AGENT_CHECKS", ActionCheckSpecs);
const supportPaths = () => parseJsonConfig("EFFECT_AGENT_SUPPORT_PATHS", StringArray);

const githubOptions = Effect.fn("workOrderAction.githubOptions")(function* () {
  const token = Redacted.value(yield* Config.redacted("EFFECT_AGENT_GITHUB_TOKEN"));
  const apiUrl = yield* Config.option(Config.nonEmptyString("GITHUB_API_URL"));
  const graphqlUrl = yield* Config.option(Config.nonEmptyString("GITHUB_GRAPHQL_URL"));
  return {
    token,
    ...(Option.isSome(apiUrl) ? { apiUrl: apiUrl.value } : {}),
    ...(Option.isSome(graphqlUrl) ? { graphqlUrl: graphqlUrl.value } : {}),
  };
});

const journalLayer = Effect.fn("workOrderAction.journalLayer")(function* () {
  const options = yield* githubOptions();
  const configuredSecret = yield* Config.redacted("EFFECT_AGENT_STATE_SECRET");
  const secret = yield* Schema.decodeUnknownEffect(JournalSecret)(
    Redacted.value(configuredSecret),
  ).pipe(
    Effect.map(Redacted.make),
    Effect.mapError(() =>
      WorkOrderActionFailure.make({
        phase: "admit",
        errorTag: "InvalidActionConfiguration",
        detail: "state secret must contain between 32 and 1,024 characters",
      }),
    ),
  );
  return Layer.merge(
    liveWorkOrderGitHubLayer(options),
    WorkOrderJournalAuthenticator.layer(secret),
  );
});

const admissionContext = Effect.fn("workOrderAction.admissionContext")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const repository = yield* Config.nonEmptyString("GITHUB_REPOSITORY");
  const eventName = yield* Config.nonEmptyString("GITHUB_EVENT_NAME");
  const eventPath = yield* Config.nonEmptyString("GITHUB_EVENT_PATH");
  const runId = yield* Config.nonEmptyString("GITHUB_RUN_ID");
  const actions = yield* Config.string("GITHUB_ACTIONS").pipe(Config.withDefault("false"));
  if (actions !== "true" || eventName !== "pull_request_review_comment") {
    return yield* WorkOrderActionFailure.make({
      phase: "admit",
      errorTag: "DeliveryUnauthentic",
      detail: "admission requires a trusted pull_request_review_comment Actions event",
    });
  }
  const rawBody = yield* fs.readFileString(eventPath);
  const event = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ActionsEvent))(
    rawBody,
  ).pipe(
    Effect.mapError(() =>
      WorkOrderActionFailure.make({
        phase: "admit",
        errorTag: "DispatchTargetRejected",
        detail: "trusted event payload does not name one review comment",
      }),
    ),
  );
  const eventId = `review-comment:${String(event.comment.id)}`;
  const configuredActors = yield* actorIds();
  const policy = IngressPolicyConfig.make({
    repository,
    pullRequestNumber: event.pull_request.number,
    authorizedActorIds: [...configuredActors],
    mentionCommand: DEFAULT_MENTION_COMMAND,
    reactionContent: DEFAULT_REACTION_CONTENT,
    webhookSecret: "actions-identity",
  });
  const delivery = PlatformDelivery.make({ deliveryId: eventId, eventName, rawBody });
  return {
    repository,
    eventName,
    rawBody,
    runId,
    eventId,
    delivery,
    policy,
  } satisfies AdmissionRuntimeContext;
});

export const admitWorkOrder = Effect.fn("workOrderAction.admit")(function* (
  request: AdmissionRequest,
) {
  const { delivery, runId } = request;
  yield* authenticateDelivery(delivery);
  const target = yield* parseDispatchTarget(delivery);
  const order = yield* constructWorkOrder(target, delivery.deliveryId);
  const eventId = order.dispatch.eventId;
  const repository = order.repository;
  const digest = yield* workOrderDigest(order);
  const stateAuthorId = yield* stableActorId("EFFECT_AGENT_STATE_AUTHOR_ID");
  const journal = yield* WorkOrderGitHub;
  const authenticator = yield* WorkOrderJournalAuthenticator;
  const comments = yield* journal.listReviewComments(repository, order.pullRequestNumber);
  const matching = [] as Array<{
    readonly commentId: string;
    readonly state: JournalClaimed | ReturnType<typeof completedState>;
  }>;
  for (const comment of comments) {
    if (comment.authorId !== stateAuthorId || comment.inReplyToId !== order.source.commentId)
      continue;
    const decoded = yield* authenticator.extract(comment.body);
    if (
      Option.isSome(decoded) &&
      decoded.value.eventId === eventId &&
      decoded.value.workOrderId === order.workOrderId &&
      decoded.value.workOrderDigest === digest
    ) {
      matching.push({ commentId: comment.id, state: decoded.value });
    }
  }
  if (matching.length > 1) {
    return yield* WorkOrderActionFailure.make({
      phase: "admit",
      errorTag: "AdmissionConflict",
      detail: "more than one authenticated admission journal exists for this dispatch",
    });
  }
  const existing = matching[0];
  if (existing !== undefined) {
    const outcome =
      existing.state._tag === "completed" ? existing.state.terminal._tag : "incomplete";
    yield* Console.log(`Duplicate delivery ${eventId}: stored outcome ${outcome}.`);
    yield* writeOutputs([
      ["should-run", "false"],
      ["duplicate", "true"],
      ["stored-outcome", outcome],
      ["work-order-id", order.workOrderId],
    ]);
    return;
  }
  const claimed = claimedState({
    eventId,
    repository,
    pullRequestNumber: order.pullRequestNumber,
    sourceCommentId: order.source.commentId,
    workOrderId: order.workOrderId,
    workOrderDigest: digest,
    expectedHeadSha: order.headSha,
    runId,
  });
  const body = yield* authenticator.render(
    claimed,
    `Effect Agent admitted work order \`${order.workOrderId.slice(0, 12)}\` at \`${order.headSha.slice(0, 12)}\`. Implementation is pending.`,
  );
  const created = yield* journal.createReply({
    repository,
    pullRequestNumber: order.pullRequestNumber,
    commentId: order.source.commentId,
    body,
  });
  const acknowledged = yield* authenticator.extract(created.body);
  if (
    created.authorId !== stateAuthorId ||
    created.inReplyToId !== order.source.commentId ||
    created.body !== body ||
    Option.isNone(acknowledged) ||
    !journalStatesEqual(claimed, acknowledged.value)
  ) {
    return yield* WorkOrderActionFailure.make({
      phase: "admit",
      errorTag: "AdmissionConflict",
      detail:
        "created admission journal did not acknowledge the exact authenticated claim and thread target",
    });
  }
  yield* writeArtifact(
    "admission",
    WorkOrderAdmissionSchema.make({
      version: 1,
      order,
      workOrderDigest: digest,
      journalCommentId: created.id,
      runId,
    }),
  );
  yield* writeOutputs([
    ["should-run", "true"],
    ["duplicate", "false"],
    ["stored-outcome", "claimed"],
    ["work-order-id", order.workOrderId],
  ]);
});

const implement = Effect.fn("workOrderAction.implement")(function* () {
  const admission = yield* readAdmissionArtifact;
  const repositoryPath = yield* Config.nonEmptyString("EFFECT_AGENT_REPOSITORY_PATH").pipe(
    Config.withDefault("worktree"),
  );
  const configuredActors = yield* actorIds();
  const configuredChecks = yield* actionChecks();
  const configuredSupportPaths = yield* supportPaths();
  const timeoutMinutes = yield* Config.schema(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60 })),
    "EFFECT_AGENT_MAX_DURATION_MINUTES",
  ).pipe(Config.withDefault(8));
  const provider = yield* Config.literals(["openai", "anthropic"], "EFFECT_AGENT_PROVIDER").pipe(
    Config.withDefault("openai"),
  );
  const modelName = yield* Config.option(Config.nonEmptyString("EFFECT_AGENT_MODEL"));
  const host = localGitWorkOrderHostLayer({
    repositoryPath,
    repository: admission.order.repository,
    pullRequestNumber: admission.order.pullRequestNumber,
    headRef: "HEAD",
    authorizedActorIds: [...configuredActors],
    checks: [],
    requiredChecks: configuredChecks.map((check) => check.name),
    allowedSupportPaths: [...configuredSupportPaths],
  });
  const run =
    provider === "anthropic"
      ? prepareWorkOrder({
          order: admission.order,
          implement: makeImplementationAgent(
            AnthropicLanguageModel.model(
              Option.getOrElse(modelName, () => "claude-sonnet-5"),
              { max_tokens: 8_000 },
            ),
          ).run,
          timeout: Duration.minutes(timeoutMinutes),
        }).pipe(
          Effect.provide(
            Layer.merge(
              host,
              AnthropicClient.layerConfig({ apiKey: Config.redacted("ANTHROPIC_API_KEY") }).pipe(
                Layer.provide(FetchHttpClient.layer),
              ),
            ),
          ),
        )
      : prepareWorkOrder({
          order: admission.order,
          implement: makeImplementationAgent(
            OpenAiLanguageModel.model(
              Option.getOrElse(modelName, () => "gpt-5.6-sol"),
              {
                max_output_tokens: 32_000,
                store: false,
                strictJsonSchema: true,
              },
            ),
          ).run,
          timeout: Duration.minutes(timeoutMinutes),
        }).pipe(
          Effect.provide(
            Layer.merge(
              host,
              OpenAiClient.layerConfig({ apiKey: Config.redacted("OPENAI_API_KEY") }).pipe(
                Layer.provide(FetchHttpClient.layer),
              ),
            ),
          ),
        );
  yield* run.pipe(
    Effect.matchEffect({
      onSuccess: (result) =>
        result._tag === "proposed"
          ? writeArtifact("proposal", result).pipe(
              Effect.andThen(writeOutputs([["candidate", "true"]])),
            )
          : writeArtifact("settlement", result).pipe(
              Effect.andThen(
                writeArtifact("implementationTerminal", terminalFromSettlement(result)),
              ),
              Effect.andThen(writeOutputs([["candidate", "false"]])),
            ),
      onFailure: (error) =>
        writeArtifact("implementationTerminal", terminalFailure(admission, error)).pipe(
          Effect.andThen(writeOutputs([["candidate", "false"]])),
        ),
    }),
  );
});

const checks = Effect.fn("workOrderAction.checks")(function* () {
  const admission = yield* readAdmissionArtifact;
  const proposal = yield* readProposalArtifact;
  const configuredChecks = yield* actionChecks();
  const containerImage = yield* Config.nonEmptyString("EFFECT_AGENT_CHECK_CONTAINER_IMAGE");
  const runnerUser = yield* Config.nonEmptyString("EFFECT_AGENT_RUNNER_USER");
  const repositoryPath = yield* Config.nonEmptyString("EFFECT_AGENT_REPOSITORY_PATH").pipe(
    Config.withDefault("worktree"),
  );
  yield* validateProposedWorkOrder({
    admission,
    proposal,
    repositoryPath,
    checks: configuredChecks,
    containerImage,
    runnerUser,
  }).pipe(
    Effect.matchEffect({
      onSuccess: (checked) =>
        writeArtifact("checked", checked).pipe(
          Effect.andThen(writeOutputs([["validated", "true"]])),
        ),
      onFailure: (error) =>
        writeArtifact("checksTerminal", terminalFailure(admission, error)).pipe(
          Effect.andThen(writeOutputs([["validated", "false"]])),
        ),
    }),
  );
});

const exactStrings = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean => {
  const a = [...left].sort();
  const b = [...right].sort();
  return a.length === b.length && a.every((value, index) => value === b[index]);
};

const verifyPublisherEnvelope = Effect.fn("verifyPublisherEnvelope")(function* (input: {
  readonly checked: CheckedWorkOrder;
  readonly authorizedActorIds: ReadonlyArray<string>;
  readonly supportPaths: ReadonlyArray<string>;
  readonly requiredChecks: ReadonlyArray<string>;
}) {
  const { admission, proposal } = input.checked;
  const expectedId = yield* workOrderIdFor(workOrderIdentityOf(admission.order));
  const expectedDigest = yield* workOrderDigest(admission.order);
  if (
    admission.order.workOrderId !== expectedId ||
    admission.workOrderDigest !== expectedDigest ||
    proposal.order.workOrderId !== expectedId ||
    proposal.workOrderDigest !== expectedDigest
  ) {
    return yield* PublisherVerificationFailure.make({
      reason: "identity-mismatch",
      detail: "publisher could not reproduce the admitted work-order identity",
    });
  }
  if (!input.authorizedActorIds.includes(admission.order.dispatch.actorId)) {
    return yield* PublisherVerificationFailure.make({
      reason: "identity-mismatch",
      detail: "publisher policy does not authorize the dispatch actor id",
    });
  }
  const allowed = new Set([
    yield* normalizeWorkspacePath(admission.order.source.path).pipe(
      Effect.mapError(() =>
        PublisherVerificationFailure.make({
          reason: "path-not-allowed",
          detail: "source path is not normalized",
        }),
      ),
    ),
    ...(yield* Effect.forEach(input.supportPaths, (path) =>
      normalizeWorkspacePath(path).pipe(
        Effect.mapError(() =>
          PublisherVerificationFailure.make({
            reason: "path-not-allowed",
            detail: "support path is not normalized",
          }),
        ),
      ),
    )),
  ]);
  const paths = yield* completeModifiedPaths(proposal.patch);
  if (
    paths.some((path) => !allowed.has(path)) ||
    !exactStrings(paths, proposal.changedPaths) ||
    !exactStrings(
      paths,
      input.checked.files.map((file) => file.path),
    )
  ) {
    return yield* PublisherVerificationFailure.make({
      reason: "path-not-allowed",
      detail: "publisher-derived patch paths differ from the fixed allowlist or checked files",
    });
  }
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto
    .digest("SHA-256", new TextEncoder().encode(proposal.patch))
    .pipe(Effect.map(Encoding.encodeHex));
  if (digest !== proposal.patchDigest) {
    return yield* PublisherVerificationFailure.make({
      reason: "digest-mismatch",
      detail: "publisher-computed patch digest differs from the checked digest",
    });
  }
  if (
    !exactStrings(input.requiredChecks, proposal.requiredChecks) ||
    !exactStrings(
      input.requiredChecks,
      input.checked.checks.map((check) => check.name),
    ) ||
    input.checked.checks.some((check) => check.status !== "passed")
  ) {
    return yield* PublisherVerificationFailure.make({
      reason: "check-evidence",
      detail: "publisher required-check evidence is incomplete, false, or failing",
    });
  }
});

const publish = Effect.fn("workOrderAction.publish")(function* () {
  const admission = yield* readAdmissionArtifact;
  const checked = yield* readCheckedArtifact;
  const configuredActors = yield* actorIds();
  const configuredSupportPaths = yield* supportPaths();
  const configuredChecks = yield* actionChecks();
  const stateAuthorId = yield* stableActorId("EFFECT_AGENT_STATE_AUTHOR_ID");
  const message = yield* Config.nonEmptyString("EFFECT_AGENT_COMMIT_MESSAGE").pipe(
    Config.withDefault(`fix: implement work order ${admission.order.workOrderId}`),
  );
  const github = yield* WorkOrderGitHub;
  const authenticator = yield* WorkOrderJournalAuthenticator;
  const publication = Effect.gen(function* () {
    if (
      checked.admission.order.workOrderId !== admission.order.workOrderId ||
      checked.admission.workOrderDigest !== admission.workOrderDigest ||
      checked.admission.journalCommentId !== admission.journalCommentId ||
      checked.admission.runId !== admission.runId
    ) {
      return yield* PublisherVerificationFailure.make({
        reason: "identity-mismatch",
        detail: "checked envelope does not belong to the transferred admission",
      });
    }
    const comments = yield* github.listReviewComments(
      admission.order.repository,
      admission.order.pullRequestNumber,
    );
    const journal = comments.find((comment) => comment.id === admission.journalCommentId);
    if (
      journal === undefined ||
      journal.authorId !== stateAuthorId ||
      journal.inReplyToId !== admission.order.source.commentId
    ) {
      return yield* PublisherVerificationFailure.make({
        reason: "identity-mismatch",
        detail: "publisher could not find the bot-authored admission journal reply",
      });
    }
    const signed = yield* authenticator.extract(journal.body);
    if (
      Option.isNone(signed) ||
      signed.value._tag !== "claimed" ||
      signed.value.repository !== admission.order.repository ||
      signed.value.pullRequestNumber !== admission.order.pullRequestNumber ||
      signed.value.sourceCommentId !== admission.order.source.commentId ||
      signed.value.eventId !== admission.order.dispatch.eventId ||
      signed.value.workOrderId !== admission.order.workOrderId ||
      signed.value.workOrderDigest !== admission.workOrderDigest ||
      signed.value.expectedHeadSha !== admission.order.headSha ||
      signed.value.runId !== admission.runId
    ) {
      return yield* PublisherVerificationFailure.make({
        reason: "identity-mismatch",
        detail: "publisher rejected the authenticated admission trust envelope",
      });
    }
    yield* verifyPublisherEnvelope({
      checked,
      authorizedActorIds: configuredActors,
      supportPaths: configuredSupportPaths,
      requiredChecks: configuredChecks.map((check) => check.name),
    });
    const expectedHeadFiles = yield* Effect.forEach(checked.files, (file) =>
      github
        .getFileContent({
          repository: admission.order.repository,
          path: file.path,
          ref: admission.order.headSha,
        })
        .pipe(Effect.map((content) => [file.path, content] as const)),
    );
    yield* reproduceCheckedPatch({
      checked,
      expectedHeadFiles: new Map(expectedHeadFiles),
    });
    return yield* github.publish({ checked, message });
  });
  yield* publication.pipe(
    Effect.matchEffect({
      onSuccess: (publishedHeadSha) =>
        writeArtifact(
          "publicationTerminal",
          PublishedTerminal.make({
            workOrderId: admission.order.workOrderId,
            workOrderDigest: admission.workOrderDigest,
            previousHeadSha: admission.order.headSha,
            publishedHeadSha,
            changedPaths: checked.proposal.changedPaths,
          }),
        ).pipe(Effect.andThen(writeOutputs([["published", "true"]]))),
      onFailure: (error) =>
        writeArtifact("publicationTerminal", terminalFailure(admission, error)).pipe(
          Effect.andThen(writeOutputs([["published", "false"]])),
        ),
    }),
  );
});

const visibleTerminal = (terminal: WorkOrderTerminal): string => {
  switch (terminal._tag) {
    case "published":
      return `Effect Agent published \`${terminal.publishedHeadSha.slice(0, 12)}\` after host validation and required checks. Changed paths: ${terminal.changedPaths.map((path) => `\`${path}\``).join(", ")}.`;
    case "settled":
      return `Effect Agent settled this work order as **${terminal.disposition}**. No patch was published.`;
    case "failed":
      return `Effect Agent did not publish this work order: **${terminal.errorTag}**.`;
  }
};

const present = Effect.fn("workOrderAction.present")(function* () {
  const admission = yield* readAdmissionArtifact;
  const existingTerminal = yield* readTerminalArtifactOption();
  const publicationAttempted = yield* Config.boolean("EFFECT_AGENT_PUBLICATION_ATTEMPTED").pipe(
    Config.withDefault(false),
  );
  if (existingTerminal !== undefined && !terminalMatchesAdmission(admission, existingTerminal)) {
    return yield* WorkOrderActionFailure.make({
      phase: "present",
      errorTag: "PresentationFailure",
      detail: "terminal artifact does not belong to the admitted work order and expected head",
    });
  }
  const terminal =
    existingTerminal ??
    FailedTerminal.make({
      workOrderId: admission.order.workOrderId,
      workOrderDigest: admission.workOrderDigest,
      headSha: admission.order.headSha,
      errorTag: publicationAttempted ? "PublicationUncertainty" : "AttemptIncomplete",
      detail: publicationAttempted
        ? "publication may have been attempted but no authenticated terminal artifact arrived"
        : "the admitted attempt ended before producing a terminal artifact",
    });
  const stateAuthorId = yield* stableActorId("EFFECT_AGENT_STATE_AUTHOR_ID");
  const github = yield* WorkOrderGitHub;
  const authenticator = yield* WorkOrderJournalAuthenticator;
  const comments = yield* github.listReviewComments(
    admission.order.repository,
    admission.order.pullRequestNumber,
  );
  const comment = comments.find((entry) => entry.id === admission.journalCommentId);
  if (
    comment === undefined ||
    comment.authorId !== stateAuthorId ||
    comment.inReplyToId !== admission.order.source.commentId
  ) {
    return yield* WorkOrderActionFailure.make({
      phase: "present",
      errorTag: "PresentationFailure",
      detail: "authenticated admission journal comment was not found",
    });
  }
  const decoded = yield* authenticator.extract(comment.body);
  if (
    Option.isNone(decoded) ||
    decoded.value._tag !== "claimed" ||
    decoded.value.eventId !== admission.order.dispatch.eventId ||
    decoded.value.repository !== admission.order.repository ||
    decoded.value.pullRequestNumber !== admission.order.pullRequestNumber ||
    decoded.value.sourceCommentId !== admission.order.source.commentId ||
    decoded.value.workOrderId !== admission.order.workOrderId ||
    decoded.value.workOrderDigest !== admission.workOrderDigest ||
    decoded.value.expectedHeadSha !== admission.order.headSha ||
    decoded.value.runId !== admission.runId
  ) {
    return yield* WorkOrderActionFailure.make({
      phase: "present",
      errorTag: "PresentationFailure",
      detail: "admission journal state is not the claimed work order owned by this run",
    });
  }
  const completed = completedState(decoded.value, terminal);
  const body = yield* authenticator.render(completed, visibleTerminal(terminal));
  const updated = yield* github.updateComment({
    repository: admission.order.repository,
    commentId: admission.journalCommentId,
    body,
  });
  const acknowledged = yield* authenticator.extract(updated.body);
  if (
    updated.id !== admission.journalCommentId ||
    updated.authorId !== stateAuthorId ||
    updated.inReplyToId !== admission.order.source.commentId ||
    updated.body !== body ||
    Option.isNone(acknowledged) ||
    !journalStatesEqual(completed, acknowledged.value)
  ) {
    return yield* WorkOrderActionFailure.make({
      phase: "present",
      errorTag: "PresentationFailure",
      detail:
        "updated admission journal did not acknowledge the exact authenticated terminal state and thread target",
    });
  }
  yield* writeOutputs([["outcome", terminal._tag]]);
  if (terminal._tag === "failed") {
    return yield* WorkOrderActionFailure.make({
      phase: "present",
      errorTag: terminal.errorTag,
      detail: terminal.detail,
    });
  }
});

export const workOrderActionProgram = Effect.gen(function* () {
  const phase = yield* Config.schema(ActionPhase, "EFFECT_AGENT_PHASE");
  yield* Console.log(`Effect Agent work-order phase: ${phase}`);
  switch (phase) {
    case "admit": {
      const context = yield* admissionContext();
      const options = yield* githubOptions();
      const trustedIdentity = ObservedActionsIdentity.of({
        read: Effect.succeed({
          repository: context.repository,
          eventName: context.eventName,
          eventPayload: context.rawBody,
          deliveryId: context.eventId,
        }),
      });
      const admissionLayer = Layer.mergeAll(
        liveGitHubApiLayer(options),
        IngressPolicy.layer(context.policy),
        Layer.succeed(ObservedActionsIdentity, trustedIdentity),
        Layer.unwrap(journalLayer()),
      );
      return yield* admitWorkOrder({ delivery: context.delivery, runId: context.runId }).pipe(
        Effect.provide(admissionLayer),
      );
    }
    case "implement":
      return yield* implement();
    case "checks":
      return yield* checks();
    case "publish":
      return yield* publish().pipe(Effect.provide(Layer.unwrap(journalLayer())));
    case "present":
      return yield* present().pipe(Effect.provide(Layer.unwrap(journalLayer())));
  }
});
