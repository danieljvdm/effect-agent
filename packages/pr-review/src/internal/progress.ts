import { Context, Effect, Layer, Option, Ref, Schema } from "effect";
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
// ---------------------------------------------------------------------------

/** Invisible marker identifying this package's sticky progress comment. */
export const PROGRESS_COMMENT_MARKER = "<!-- effect-agent-pr-review progress -->";

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
export const renderProgressBeginBody = (info: ReviewProgressBegin): string =>
  [
    "> 🔍 **Code review in progress…**",
    ">",
    `> ${scopeSentence(info)}`,
    "",
    "_This comment is updated in place by each review run._",
    "",
    footerLine(info),
    PROGRESS_COMMENT_MARKER,
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
export const renderProgressSettleBody = (info: ReviewProgressSettle): string => {
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
    PROGRESS_COMMENT_MARKER,
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

/**
 * GitHub-backed progress reporter over the issue-comments API. The sticky
 * comment is found by its invisible marker AND the configured posting-bot
 * identity — a marker pasted into someone else's comment is never edited.
 * Every fault (lookup, create, update, bound exhaustion) degrades to a logged
 * warning: a pull request without a progress comment is a cosmetic loss, a
 * failed review run over a cosmetic fault would not be.
 */
export const gitHubReviewProgressLayer: Layer.Layer<
  ReviewProgressReporter,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(ReviewProgressReporter)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const knownCommentId = yield* Ref.make(Option.none<number>());
    const authorLogin = (
      target.reviewAuthorLogin ?? DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN
    ).toLowerCase();
    const issuePrefix = `${target.apiUrl}/repos/${target.repository}/issues`;

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

    const decodeComments = Schema.decodeUnknownEffect(GitHubIssueCommentsPageWire);
    const decodeComment = Schema.decodeUnknownEffect(GitHubIssueCommentWire);

    const findExisting = Effect.gen(function* () {
      const perPage = 100;
      let found = Option.none<number>();
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
        const wires = yield* response.json.pipe(
          Effect.mapError(asApiFailure("listProgressComments")),
          Effect.flatMap((body) =>
            decodeComments(body).pipe(Effect.mapError(asApiFailure("listProgressComments"))),
          ),
        );
        for (const wire of wires) {
          if (
            wire.user?.login.toLowerCase() === authorLogin &&
            wire.user.type === "Bot" &&
            (wire.body ?? "").includes(PROGRESS_COMMENT_MARKER)
          ) {
            found = Option.some(wire.id);
          }
        }
        if (wires.length < perPage) return found;
      }
      return yield* GitHubApiFailure.make({
        operation: "listProgressComments",
        reason: `comment history exceeds the bounded ${MAX_PROGRESS_LOOKUP_PAGES * 100}-comment lookup`,
      });
    });

    const create = (body: string) =>
      execute(
        "createProgressComment",
        withHeaders(
          HttpClientRequest.post(`${issuePrefix}/${target.number}/comments`).pipe(
            HttpClientRequest.bodyJsonUnsafe({ body }),
          ),
        ),
      ).pipe(
        Effect.flatMap((response) =>
          response.json.pipe(
            Effect.mapError(asApiFailure("createProgressComment")),
            Effect.flatMap((payload) =>
              decodeComment(payload).pipe(Effect.mapError(asApiFailure("createProgressComment"))),
            ),
          ),
        ),
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

    const upsert = Effect.fn("ReviewProgressReporter.upsert")(function* (body: string) {
      const cached = yield* Ref.get(knownCommentId);
      if (Option.isSome(cached)) {
        return yield* update(cached.value, body);
      }
      const existing = yield* findExisting;
      if (Option.isSome(existing)) {
        yield* Ref.set(knownCommentId, existing);
        return yield* update(existing.value, body);
      }
      const created = yield* create(body);
      yield* Ref.set(knownCommentId, Option.some(created));
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
      begin: (info) => upsert(renderProgressBeginBody(info)).pipe(failOpen("begin")),
      settle: (info) => upsert(renderProgressSettleBody(info)).pipe(failOpen("settle")),
    });
  }),
);
