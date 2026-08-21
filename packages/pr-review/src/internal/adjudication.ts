import { Context, DateTime, Effect, Layer, Schema } from "effect";

import { INLINE_FINDING_TITLE_PATTERN } from "./retirement.ts";
import {
  adjudicationIdentity,
  MAX_STORED_ADJUDICATIONS,
  StoredAdjudication,
  type AdjudicationDisposition,
  type StoredReviewFinding,
} from "./review-state.ts";

// ---------------------------------------------------------------------------
// Maintainer adjudication. GitHub reads stay behind ReviewAdjudicationHost;
// this module owns only the deterministic verb grammar, fail-closed
// authorization, later-wins resolution, and prompt-context rendering. Only an
// explicit, authorized `/adjudicate` verb adjudicates — free-text rebuttals
// are deliberately never parsed, because only an explicit verb is auditable
// and fail-closed (model output and third-party comments are untrusted
// input, AGENTS.md rule 11).
// ---------------------------------------------------------------------------

const PositiveLine = Schema.Int.check(Schema.isGreaterThan(0));

/** Maximum authorized command candidates retained for one inline thread. */
export const MAX_THREAD_ADJUDICATION_COMMANDS = 100;

/** One reply or top-level comment observed through the adjudication host. */
export class AdjudicationComment extends Schema.Class<AdjudicationComment>(
  "@effect-agent/pr-review/AdjudicationComment",
)({
  body: Schema.String.check(Schema.isMaxLength(65_536)),
  /** GitHub's author_association for the comment author, verbatim. */
  authorAssociation: Schema.String.check(Schema.isMaxLength(40)),
  authorLogin: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
  /** Creation time; a comment without one loses every later-wins tie. */
  createdAt: Schema.NullOr(Schema.DateTimeUtc),
  /** Stable zero-based order in the source listing, before thread grouping. */
  sourceOrder: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** One of the action's own inline finding threads, replies in creation order. */
export class AdjudicableThread extends Schema.Class<AdjudicableThread>(
  "@effect-agent/pr-review/AdjudicableThread",
)({
  path: Schema.NonEmptyString.check(Schema.isMaxLength(500)),
  startLine: Schema.NullOr(PositiveLine),
  endLine: Schema.NullOr(PositiveLine),
  /** The root comment's body; its first line carries the finding title. */
  rootBody: Schema.String.check(Schema.isMaxLength(65_536)),
  replies: Schema.Array(AdjudicationComment).check(
    Schema.isMaxLength(MAX_THREAD_ADJUDICATION_COMMANDS),
  ),
}) {}

/** A GitHub adjudication read failed. */
export class ReviewAdjudicationFailure extends Schema.TaggedError<ReviewAdjudicationFailure>()(
  "ReviewAdjudicationFailure",
  {
    operation: Schema.String,
    reason: Schema.String,
  },
) {
  override get message() {
    return `Review adjudication operation '${this.operation}' failed: ${this.reason}`;
  }
}

/**
 * Host-side GitHub reads used by adjudication. Domain code never reaches into
 * REST directly, and deterministic tests substitute this port. Both listings
 * return comments in creation order.
 */
export class ReviewAdjudicationHost extends Context.Service<
  ReviewAdjudicationHost,
  {
    /** This action's own inline finding threads with their replies. */
    readonly listFindingThreads: Effect.Effect<
      ReadonlyArray<AdjudicableThread>,
      ReviewAdjudicationFailure
    >;
    /** Top-level pull-request conversation comments. */
    readonly listIssueComments: Effect.Effect<
      ReadonlyArray<AdjudicationComment>,
      ReviewAdjudicationFailure
    >;
  }
>()("@effect-agent/pr-review/ReviewAdjudicationHost") {}

/** Explicit program-edge adapter for runs that intentionally perform no host reads. */
export const noReviewAdjudicationHost = ReviewAdjudicationHost.of({
  listFindingThreads: Effect.succeed([]),
  listIssueComments: Effect.succeed([]),
});

/** Layer form of {@link noReviewAdjudicationHost}. */
export const noReviewAdjudicationHostLayer =
  Layer.succeed(ReviewAdjudicationHost)(noReviewAdjudicationHost);

// ---------------------------------------------------------------------------
// Verb grammar. A body whose first line starts with `/adjudicate` is a
// command; a command that fails the grammar is malformed and ignored rather
// than guessed at. Fail-closed authorization: only OWNER, MEMBER, and
// COLLABORATOR authors may adjudicate.
// ---------------------------------------------------------------------------

/** author_associations allowed to adjudicate; everything else is ignored. */
export const AUTHORIZED_ADJUDICATION_ASSOCIATIONS: ReadonlySet<string> = new Set([
  "OWNER",
  "MEMBER",
  "COLLABORATOR",
]);

const AdjudicationDispositionSchema = Schema.Literals(["accepted-risk", "refuted", "obsolete"]);

const THREAD_COMMAND_PATTERN = /^\/adjudicate[ \t]+([a-z-]+)[ \t]*(?::[ \t]*(.*\S))?[ \t]*$/;
const ISSUE_COMMAND_PATTERN =
  /^\/adjudicate[ \t]+([a-z-]+)[ \t]+"([^"\n]+)"[ \t]*(?::[ \t]*(.*\S))?[ \t]*$/;

export interface ParsedAdjudicationCommand {
  readonly disposition: AdjudicationDisposition;
  /** Present only for the issue-comment grammar's quoted target title. */
  readonly title?: string | undefined;
  readonly reason?: string | undefined;
}

const firstLine = (body: string): string => (body.split("\n", 1)[0] ?? "").trim();

const boundedReason = (raw: string | undefined): string | undefined => {
  if (raw === undefined) return undefined;
  const trimmed = raw.trim().slice(0, 300);
  return trimmed.length === 0 ? undefined : trimmed;
};

/**
 * Parse one inline-thread reply: `/adjudicate <disposition>(: <reason>)?`.
 * The thread itself names the target identity. Returns undefined for a
 * non-command body and "malformed" for a command that fails the grammar.
 */
export const parseThreadAdjudication = (
  body: string,
): ParsedAdjudicationCommand | "malformed" | undefined => {
  const line = firstLine(body);
  if (!line.startsWith("/adjudicate")) return undefined;
  const match = THREAD_COMMAND_PATTERN.exec(line);
  const disposition = match?.[1];
  if (disposition === undefined || !Schema.is(AdjudicationDispositionSchema)(disposition)) {
    return "malformed";
  }
  return { disposition, reason: boundedReason(match?.[2]) };
};

/**
 * Parse one top-level PR comment:
 * `/adjudicate <disposition> "<exact title>"(: <reason>)?`. The quoted title
 * is required because the conversation names no finding thread; it targets
 * the title-alone identity of an unanchored concern.
 */
export const parseIssueAdjudication = (
  body: string,
): ParsedAdjudicationCommand | "malformed" | undefined => {
  const line = firstLine(body);
  if (!line.startsWith("/adjudicate")) return undefined;
  const match = ISSUE_COMMAND_PATTERN.exec(line);
  const disposition = match?.[1];
  const title = match?.[2];
  if (
    disposition === undefined ||
    !Schema.is(AdjudicationDispositionSchema)(disposition) ||
    title === undefined ||
    title.length > 120
  ) {
    return "malformed";
  }
  return { disposition, title, reason: boundedReason(match?.[3]) };
};

/** The finding identity an inline thread names, or undefined when unparsable. */
export const threadFindingTarget = (
  thread: AdjudicableThread,
):
  | {
      readonly path: string;
      readonly startLine: number;
      readonly endLine: number;
      readonly title: string;
    }
  | undefined => {
  if (thread.startLine === null || thread.endLine === null) return undefined;
  const title = INLINE_FINDING_TITLE_PATTERN.exec(firstLine(thread.rootBody))?.[1];
  if (title === undefined || title.length > 120) return undefined;
  return {
    path: thread.path,
    startLine: thread.startLine,
    endLine: thread.endLine,
    title,
  };
};

// ---------------------------------------------------------------------------
// Deterministic derivation: authorization, later-wins, bounded storage.
// ---------------------------------------------------------------------------

interface AdjudicationCandidate {
  readonly adjudication: StoredAdjudication;
  readonly epochMillis: number;
  readonly sourceOrder: number;
}

export interface DerivedAdjudications {
  readonly adjudications: ReadonlyArray<StoredAdjudication>;
  /** Commands ignored fail-closed: unauthorized authors and malformed bodies. */
  readonly ignored: ReadonlyArray<string>;
  /** Later-wins winners dropped oldest-first at the storage bound. */
  readonly droppedOldest: number;
}

/**
 * Derive the standing adjudications from the host's listings. Every command
 * is screened fail-closed (authorization, grammar, a parsable target); later
 * adjudications of the same identity win by comment creation order; the
 * result is capped at the ReviewState bound dropping the oldest winners.
 */
export const deriveAdjudications = (input: {
  readonly threads: ReadonlyArray<AdjudicableThread>;
  readonly issueComments: ReadonlyArray<AdjudicationComment>;
}): DerivedAdjudications => {
  const candidates: Array<AdjudicationCandidate> = [];
  const ignored: Array<string> = [];
  const admit = (
    comment: AdjudicationComment,
    command: ParsedAdjudicationCommand,
    target: {
      readonly path?: string | undefined;
      readonly startLine?: number | undefined;
      readonly endLine?: number | undefined;
      readonly title: string;
    },
  ): void => {
    candidates.push({
      adjudication: StoredAdjudication.make({
        ...(target.path === undefined ? {} : { path: target.path }),
        ...(target.startLine === undefined ? {} : { startLine: target.startLine }),
        ...(target.endLine === undefined ? {} : { endLine: target.endLine }),
        title: target.title,
        disposition: command.disposition,
        ...(command.reason === undefined ? {} : { reason: command.reason }),
        actor: comment.authorLogin,
      }),
      epochMillis: comment.createdAt === null ? -1 : DateTime.toEpochMillis(comment.createdAt),
      sourceOrder: comment.sourceOrder,
    });
  };
  const authorized = (comment: AdjudicationComment, surface: string): boolean => {
    if (AUTHORIZED_ADJUDICATION_ASSOCIATIONS.has(comment.authorAssociation)) return true;
    ignored.push(
      `${surface}: unauthorized /adjudicate from @${comment.authorLogin} (${comment.authorAssociation})`,
    );
    return false;
  };

  for (const thread of input.threads) {
    const target = threadFindingTarget(thread);
    for (const reply of thread.replies) {
      const command = parseThreadAdjudication(reply.body);
      if (command === undefined) continue;
      const surface = `inline thread ${thread.path}`;
      if (command === "malformed") {
        ignored.push(`${surface}: malformed /adjudicate command from @${reply.authorLogin}`);
        continue;
      }
      if (!authorized(reply, surface)) continue;
      if (target === undefined) {
        ignored.push(`${surface}: thread root names no parsable finding title`);
        continue;
      }
      admit(reply, command, target);
    }
  }
  for (const comment of input.issueComments) {
    const command = parseIssueAdjudication(comment.body);
    if (command === undefined) continue;
    const surface = "pull-request conversation";
    if (command === "malformed") {
      ignored.push(`${surface}: malformed /adjudicate command from @${comment.authorLogin}`);
      continue;
    }
    if (!authorized(comment, surface)) continue;
    if (command.title === undefined) {
      ignored.push(`${surface}: /adjudicate without a quoted target title`);
      continue;
    }
    admit(comment, command, { title: command.title });
  }

  const byIdentity = new Map<string, AdjudicationCandidate>();
  const ordered = [...candidates].sort(
    (left, right) => left.epochMillis - right.epochMillis || left.sourceOrder - right.sourceOrder,
  );
  for (const candidate of ordered) {
    const identity = adjudicationIdentity(candidate.adjudication);
    // Delete-then-set so a later adjudication also refreshes its recency for
    // the oldest-first drop below.
    byIdentity.delete(identity);
    byIdentity.set(identity, candidate);
  }
  const winners = [...byIdentity.values()];
  const droppedOldest = Math.max(0, winners.length - MAX_STORED_ADJUDICATIONS);
  return {
    adjudications: winners.slice(droppedOldest).map((candidate) => candidate.adjudication),
    ignored,
    droppedOldest,
  };
};

/** Later-wins merge of stored prior adjudications with freshly derived ones. */
export const mergeAdjudications = (
  prior: ReadonlyArray<StoredAdjudication>,
  fresh: ReadonlyArray<StoredAdjudication>,
): ReadonlyArray<StoredAdjudication> => {
  const byIdentity = new Map<string, StoredAdjudication>();
  for (const adjudication of [...prior, ...fresh]) {
    const identity = adjudicationIdentity(adjudication);
    byIdentity.delete(identity);
    byIdentity.set(identity, adjudication);
  }
  const merged = [...byIdentity.values()];
  return merged.slice(Math.max(0, merged.length - MAX_STORED_ADJUDICATIONS));
};

/**
 * Collect the standing maintainer adjudications: freshly derived through the
 * host, merged later-wins over the prior state's stored set. The host is a
 * visible Effect requirement; program edges that intentionally perform no
 * reads provide {@link noReviewAdjudicationHost}. Fail-open — any listing
 * fault keeps the complete prior set and never fails the review, because NOT
 * suppressing a finding is the conservative direction.
 */
export const collectReviewAdjudications = Effect.fn("collectReviewAdjudications")(function* (
  prior: ReadonlyArray<StoredAdjudication>,
) {
  const host = yield* ReviewAdjudicationHost;
  const listings = yield* Effect.all({
    threads: host.listFindingThreads,
    issueComments: host.listIssueComments,
  }).pipe(
    Effect.catch((error) =>
      Effect.logWarning(
        `Could not collect adjudications from '${error.operation}': ${error.reason}; retaining stored adjudications unchanged.`,
      ).pipe(Effect.as(undefined)),
    ),
  );
  if (listings === undefined) return prior;
  const derived = deriveAdjudications({
    threads: listings.threads,
    issueComments: listings.issueComments,
  });
  for (const note of derived.ignored) {
    yield* Effect.logDebug(`Ignored adjudication command — ${note}`);
  }
  if (derived.droppedOldest > 0) {
    yield* Effect.logWarning(
      `Dropped ${derived.droppedOldest} oldest adjudication(s) over the ${MAX_STORED_ADJUDICATIONS}-entry bound.`,
    );
  }
  return mergeAdjudications(prior, derived.adjudications);
});

// ---------------------------------------------------------------------------
// Prompt-context rendering: deterministic bounded lines the reviewer sees.
// ---------------------------------------------------------------------------

const lineRange = (startLine: number, endLine: number): string =>
  `${startLine}${endLine === startLine ? "" : `-${endLine}`}`;

/** One adjudication as a bounded reviewer-prompt context line. */
export const renderAdjudicationContextLine = (adjudication: StoredAdjudication): string => {
  const location =
    adjudication.path !== undefined &&
    adjudication.startLine !== undefined &&
    adjudication.endLine !== undefined
      ? `${adjudication.path}:${lineRange(adjudication.startLine, adjudication.endLine)}`
      : "(unanchored)";
  const reason = adjudication.reason === undefined ? "" : `: ${adjudication.reason}`;
  return `${location} "${adjudication.title}" — ${adjudication.disposition} by @${adjudication.actor}${reason}`;
};

/** One prior-round finding as a bounded reviewer-prompt context line. */
export const renderPriorFindingContextLine = (finding: StoredReviewFinding): string =>
  `${finding.path}:${lineRange(finding.startLine, finding.endLine)} [${finding.severity}] "${finding.title}" — ${finding.body.slice(0, 400)}`;

/** Prior-review context threaded into fan-out discovery briefs, per path. */
export interface PriorReviewContext {
  /** Adjudicated identities; path-free entries apply to every unit. */
  readonly adjudicated: ReadonlyArray<{
    readonly path: string | undefined;
    readonly line: string;
  }>;
  /** Prior-round findings whose paths are being re-reviewed. */
  readonly priorFindings: ReadonlyArray<{ readonly path: string; readonly line: string }>;
}

/** Build the fan-out prior-review context from the resolved continuity data. */
export const buildPriorReviewContext = (
  adjudications: ReadonlyArray<StoredAdjudication>,
  priorFindingsOnScope: ReadonlyArray<StoredReviewFinding>,
): PriorReviewContext => ({
  adjudicated: adjudications.map((adjudication) => ({
    path: adjudication.path,
    line: renderAdjudicationContextLine(adjudication),
  })),
  priorFindings: priorFindingsOnScope.map((finding) => ({
    path: finding.path,
    line: renderPriorFindingContextLine(finding),
  })),
});
