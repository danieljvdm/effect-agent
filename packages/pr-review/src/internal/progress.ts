import { Context, DateTime, Effect, Layer, Option, Ref, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN,
  GitHubApiFailure,
  GitHubReviewTarget,
} from "./github.ts";
import type { ReviewScopeMode } from "./review-state.ts";

// ---------------------------------------------------------------------------
// The sticky review-progress comment: one issue comment per pull request that
// says a review run is working the moment it starts, updated in place with
// the settled outcome. Progress reporting is cosmetic and FAIL-OPEN by
// design: it must never change what the review posts or how the check
// concludes, so every GitHub fault here is logged and swallowed. The review
// itself still publishes only through the validated ReviewPublisher after
// the run settles.
//
// Concurrency contract (honest, per the no-exactly-once rule): posting is
// at-least-once and writes are GENERATION-FENCED, never atomic. Each run
// embeds a claim marker (run token + start time) in the comment it writes and
// re-reads the comment immediately before every update, writing only when the
// current claim is its own or belongs to an older run — so a stale run cannot
// replace a newer run's status outside the read-then-write window. Runs adopt
// the newest existing claim comment and best-effort delete older duplicates,
// so duplicates left by unfenced overlapping runs self-heal on the next run.
// Strict single-comment behavior comes from workflow-level per-PR concurrency
// groups (as in the reference workflow), not from this adapter.
// ---------------------------------------------------------------------------

/** Every progress comment starts its invisible marker with this prefix. */
export const PROGRESS_COMMENT_MARKER_PREFIX = "<!-- effect-agent-pr-review progress";

/** One run's generation fence: who wrote a progress comment, and when. */
export interface ProgressClaim {
  /** Random per-run token; matching it means the comment is this run's own. */
  readonly runToken: string;
  /** Run start in epoch millis; newer runs may overwrite older claims. */
  readonly startedMillis: number;
}

const CLAIM_PATTERN = /<!-- effect-agent-pr-review progress run=([0-9A-Za-z-]+) started=(\d+) -->/g;

/** HTML comments must not contain `--`; tokens are reduced to a safe alphabet. */
const sanitizeToken = (token: string): string =>
  token.replaceAll(/[^0-9A-Za-z-]/g, "").replaceAll(/-{2,}/g, "-");

/** Render one run's claim marker (token sanitized into the safe alphabet). */
export const renderProgressClaimMarker = (claim: ProgressClaim): string =>
  `${PROGRESS_COMMENT_MARKER_PREFIX} run=${sanitizeToken(claim.runToken)} started=${Math.max(0, Math.floor(claim.startedMillis))} -->`;

/** Extract the last claim marker in one comment body, if any. */
export const parseProgressClaim = (body: string): ProgressClaim | undefined => {
  let last: ProgressClaim | undefined;
  for (const match of body.matchAll(CLAIM_PATTERN)) {
    const startedMillis = Number(match[2]);
    if (match[1] !== undefined && Number.isFinite(startedMillis)) {
      last = { runToken: match[1], startedMillis };
    }
  }
  return last;
};

/** What a starting run can honestly say before any model turn has executed. */
export interface ReviewProgressBegin {
  readonly headSha?: string | undefined;
  readonly reviewMode?: ReviewScopeMode | undefined;
  readonly reviewReason?: string | undefined;
  readonly filesInScope?: number | undefined;
  readonly modelLabel?: string | undefined;
  readonly runUrl?: string | undefined;
}

/** How the run ended: a settled (posted) review, or a failure that posted nothing. */
export type ReviewProgressSettle =
  | {
      readonly outcome: "reviewed";
      readonly conclusion: "success" | "blocking" | "incomplete";
      readonly verdict: string;
      readonly inlineComments: number;
      readonly reviewUrl?: string | undefined;
      readonly runUrl?: string | undefined;
      readonly modelLabel?: string | undefined;
    }
  | {
      readonly outcome: "failed";
      readonly runUrl?: string | undefined;
      readonly modelLabel?: string | undefined;
    };

const footerLine = (options: {
  readonly modelLabel?: string | undefined;
  readonly runUrl?: string | undefined;
}): string => {
  const parts = ["@effect-agent/pr-review"];
  if (options.modelLabel !== undefined) parts.push(options.modelLabel);
  if (options.runUrl !== undefined) parts.push(`[workflow run](${options.runUrl})`);
  return `_${parts.join(" · ")}._`;
};

const scopeSentence = (info: ReviewProgressBegin): string => {
  const subject =
    info.filesInScope === undefined ? "this pull request" : `${info.filesInScope} changed file(s)`;
  const at = info.headSha === undefined ? "" : ` at \`${info.headSha.slice(0, 7)}\``;
  const scope =
    info.reviewMode === undefined
      ? ""
      : ` — ${info.reviewMode === "incremental" ? "incremental" : "full-diff"} scope${
          info.reviewReason === undefined ? "" : `: ${info.reviewReason.slice(0, 1_000)}`
        }`;
  return `Reviewing ${subject}${at}${scope}.`;
};

/** The "a run just started" body; the outcome update replaces it in place. */
export const renderProgressBeginBody = (info: ReviewProgressBegin, claim: ProgressClaim): string =>
  [
    "> 🔍 **Code review in progress…**",
    ">",
    `> ${scopeSentence(info)}`,
    "",
    "_This comment is updated in place by each review run._",
    "",
    footerLine(info),
    renderProgressClaimMarker(claim),
  ].join("\n");

const settleCallout = (info: ReviewProgressSettle): string => {
  if (info.outcome === "failed") {
    return "> ⚠️ **Code review run failed** — nothing was posted.";
  }
  switch (info.conclusion) {
    case "success":
      return `> ✅ **Code review posted** — verdict \`${info.verdict}\`, ${info.inlineComments} inline comment(s), nothing blocking.`;
    case "blocking":
      return `> 🛑 **Code review posted** — blocking findings; the check fails until they are addressed.`;
    case "incomplete":
      return `> ⚠️ **Code review posted** — required coverage is incomplete, so the check fails.`;
  }
};

/** The settled-outcome body written over the in-progress comment. */
export const renderProgressSettleBody = (
  info: ReviewProgressSettle,
  claim: ProgressClaim,
): string => {
  const link =
    info.outcome === "reviewed" && info.reviewUrl !== undefined
      ? `See the [posted review](${info.reviewUrl}).`
      : info.runUrl !== undefined
        ? `See the [workflow run](${info.runUrl}) for details.`
        : undefined;
  return [
    settleCallout(info),
    ...(link === undefined ? [] : ["", link]),
    "",
    footerLine(info),
    renderProgressClaimMarker(claim),
  ].join("\n");
};

/**
 * Maintains the sticky progress comment. Both operations are infallible by
 * contract: implementations own their fault handling, because progress
 * reporting may never fail or delay the review run it narrates.
 */
export class ReviewProgressReporter extends Context.Service<
  ReviewProgressReporter,
  {
    readonly begin: (info: ReviewProgressBegin) => Effect.Effect<void>;
    readonly settle: (info: ReviewProgressSettle) => Effect.Effect<void>;
  }
>()("@effect-agent/pr-review/ReviewProgressReporter") {}

/** Reports nothing; the substitute for hosts without a progress surface. */
export const noopReviewProgressReporterLayer: Layer.Layer<ReviewProgressReporter> = Layer.succeed(
  ReviewProgressReporter,
  ReviewProgressReporter.of({
    begin: () => Effect.void,
    settle: () => Effect.void,
  }),
);

const GitHubIssueCommentWire = Schema.Struct({
  id: Schema.Int,
  body: Schema.optionalKey(Schema.NullOr(Schema.String)),
  user: Schema.optionalKey(
    Schema.NullOr(Schema.Struct({ login: Schema.String, type: Schema.String })),
  ),
});
const GitHubIssueCommentsPageWire = Schema.Array(GitHubIssueCommentWire);

/** Issue comments page chronologically; the sticky-comment scan stays bounded. */
const MAX_PROGRESS_LOOKUP_PAGES = 5;

interface ProgressCandidate {
  readonly id: number;
  readonly body: string;
}

/** The comment carrying the newest claim wins; unparseable claims lose ties. */
const pickNewestClaim = (
  candidates: ReadonlyArray<ProgressCandidate>,
): ProgressCandidate | undefined => {
  let newest: ProgressCandidate | undefined;
  let newestStarted = Number.NEGATIVE_INFINITY;
  for (const candidate of candidates) {
    const started = parseProgressClaim(candidate.body)?.startedMillis ?? Number.NEGATIVE_INFINITY;
    // >= keeps the LAST (chronologically newest) comment on ties.
    if (newest === undefined || started >= newestStarted) {
      newest = candidate;
      newestStarted = started;
    }
  }
  return newest;
};

/**
 * GitHub-backed progress reporter over the issue-comments API. The sticky
 * comment is found by its invisible marker AND the configured posting-bot
 * identity — a marker pasted into someone else's comment is never edited.
 * Writes are generation-fenced per the module contract above, and every
 * fault (lookup, create, update, delete, bound exhaustion) degrades to a
 * logged warning: a pull request without a progress comment is a cosmetic
 * loss, a failed review run over a cosmetic fault would not be.
 */
export const gitHubReviewProgressLayer: Layer.Layer<
  ReviewProgressReporter,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(ReviewProgressReporter)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const started = yield* DateTime.now;
    const claim: ProgressClaim = {
      runToken: globalThis.crypto.randomUUID(),
      startedMillis: DateTime.toEpochMillis(started),
    };
    const knownCommentId = yield* Ref.make(Option.none<number>());
    const authorLogin = (
      target.reviewAuthorLogin ?? DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN
    ).toLowerCase();
    const issuePrefix = `${target.apiUrl}/repos/${target.repository}/issues`;

    /** May this run overwrite a comment currently carrying `body`? */
    const canClaim = (body: string): boolean => {
      const existing = parseProgressClaim(body);
      if (existing === undefined) return true;
      return existing.runToken === claim.runToken || claim.startedMillis >= existing.startedMillis;
    };

    const withHeaders = (request: HttpClientRequest.HttpClientRequest) => {
      const base = request.pipe(
        HttpClientRequest.setHeaders({
          "X-GitHub-Api-Version": "2022-11-28",
          "User-Agent": "effect-agent-pr-review",
        }),
        HttpClientRequest.acceptJson,
      );
      return Option.isSome(target.token)
        ? base.pipe(HttpClientRequest.bearerToken(target.token.value))
        : base;
    };

    const asApiFailure =
      (operation: string) =>
      (error: { readonly _tag: string; readonly message?: string }): GitHubApiFailure =>
        GitHubApiFailure.make({
          operation,
          reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
        });

    const execute = (operation: string, request: HttpClientRequest.HttpClientRequest) =>
      HttpClient.execute(request).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.mapError(asApiFailure(operation)),
        Effect.provideService(HttpClient.HttpClient, client),
      );

    const decodeJson = <S extends Schema.Top>(schema: S, operation: string) => {
      const decode = Schema.decodeUnknownEffect(schema);
      return (response: HttpClientResponse.HttpClientResponse) =>
        response.json.pipe(
          Effect.mapError(asApiFailure(operation)),
          Effect.flatMap((payload) =>
            decode(payload).pipe(Effect.mapError(asApiFailure(operation))),
          ),
        );
    };

    const findCandidates = Effect.gen(function* () {
      const perPage = 100;
      const found: Array<ProgressCandidate> = [];
      for (let page = 1; page <= MAX_PROGRESS_LOOKUP_PAGES; page += 1) {
        const response = yield* execute(
          "listProgressComments",
          withHeaders(
            HttpClientRequest.get(`${issuePrefix}/${target.number}/comments`).pipe(
              HttpClientRequest.setUrlParams({
                per_page: String(perPage),
                page: String(page),
              }),
            ),
          ),
        );
        const wires = yield* decodeJson(
          GitHubIssueCommentsPageWire,
          "listProgressComments",
        )(response);
        for (const wire of wires) {
          if (
            wire.user?.login.toLowerCase() === authorLogin &&
            wire.user.type === "Bot" &&
            (wire.body ?? "").includes(PROGRESS_COMMENT_MARKER_PREFIX)
          ) {
            found.push({ id: wire.id, body: wire.body ?? "" });
          }
        }
        if (wires.length < perPage) return found as ReadonlyArray<ProgressCandidate>;
      }
      return yield* GitHubApiFailure.make({
        operation: "listProgressComments",
        reason: `comment history exceeds the bounded ${MAX_PROGRESS_LOOKUP_PAGES * 100}-comment lookup`,
      });
    });

    const readComment = (commentId: number) =>
      execute(
        "readProgressComment",
        withHeaders(HttpClientRequest.get(`${issuePrefix}/comments/${commentId}`)),
      ).pipe(
        Effect.flatMap(decodeJson(GitHubIssueCommentWire, "readProgressComment")),
        Effect.map((wire) => wire.body ?? ""),
      );

    const create = (body: string) =>
      execute(
        "createProgressComment",
        withHeaders(
          HttpClientRequest.post(`${issuePrefix}/${target.number}/comments`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ body }),
          ),
        ),
      ).pipe(
        Effect.flatMap(decodeJson(GitHubIssueCommentWire, "createProgressComment")),
        Effect.map((wire) => wire.id),
      );

    const update = (commentId: number, body: string) =>
      execute(
        "updateProgressComment",
        withHeaders(
          HttpClientRequest.patch(`${issuePrefix}/comments/${commentId}`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ body }),
          ),
        ),
      ).pipe(Effect.asVoid);

    const deleteComment = (commentId: number) =>
      execute(
        "deleteProgressComment",
        withHeaders(HttpClientRequest.delete(`${issuePrefix}/comments/${commentId}`)),
      ).pipe(Effect.asVoid);

    /** Re-read, fence, then write: a stale run must not replace newer status. */
    const guardedUpdate = Effect.fn("ReviewProgressReporter.guardedUpdate")(function* (
      commentId: number,
      body: string,
    ) {
      const current = yield* readComment(commentId);
      if (!canClaim(current)) {
        return yield* Effect.logDebug(
          "review progress comment is owned by a newer run; leaving it untouched",
        );
      }
      yield* update(commentId, body);
    });

    const upsert = Effect.fn("ReviewProgressReporter.upsert")(function* (body: string) {
      const cached = yield* Ref.get(knownCommentId);
      if (Option.isSome(cached)) {
        return yield* guardedUpdate(cached.value, body);
      }
      const candidates = yield* findCandidates;
      const newest = pickNewestClaim(candidates);
      if (newest === undefined) {
        const created = yield* create(body);
        yield* Ref.set(knownCommentId, Option.some(created));
        return;
      }
      // Reconcile duplicates left by unfenced overlapping runs: keep the
      // newest claim, best-effort delete the rest (each fault only logged).
      yield* Effect.forEach(
        candidates.filter((candidate) => candidate.id !== newest.id),
        (duplicate) =>
          deleteComment(duplicate.id).pipe(
            Effect.catch((failure) =>
              Effect.logWarning("duplicate review progress comment could not be deleted").pipe(
                Effect.annotateLogs({ commentId: duplicate.id, reason: failure.reason }),
              ),
            ),
          ),
        { discard: true },
      );
      yield* Ref.set(knownCommentId, Option.some(newest.id));
      if (!canClaim(newest.body)) {
        return yield* Effect.logDebug(
          "review progress comment is owned by a newer run; leaving it untouched",
        );
      }
      yield* update(newest.id, body);
    });

    const failOpen = (phase: string) => (effect: Effect.Effect<void, GitHubApiFailure>) =>
      effect.pipe(
        Effect.catch((failure) =>
          Effect.logWarning("review progress comment update failed").pipe(
            Effect.annotateLogs({
              progressPhase: phase,
              operation: failure.operation,
              reason: failure.reason,
            }),
          ),
        ),
      );

    return ReviewProgressReporter.of({
      begin: (info) => upsert(renderProgressBeginBody(info, claim)).pipe(failOpen("begin")),
      settle: (info) => upsert(renderProgressSettleBody(info, claim)).pipe(failOpen("settle")),
    });
  }),
);
