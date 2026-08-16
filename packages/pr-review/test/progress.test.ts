import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GitHubReviewTarget,
  gitHubReviewProgressLayer,
  PROGRESS_COMMENT_MARKER,
  renderProgressBeginBody,
  renderProgressSettleBody,
  ReviewProgressReporter,
} from "../src/index.ts";

const HEAD_SHA = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";
const RUN_URL = "https://github.test/acme/widgets/actions/runs/9";

describe("progress comment rendering", () => {
  it("renders the in-progress body with scope, model, and run link", () => {
    const body = renderProgressBeginBody({
      headSha: HEAD_SHA,
      reviewMode: "incremental",
      reviewReason: "changes since successfully reviewed head 1111111",
      filesInScope: 4,
      modelLabel: "openai:gpt-5.6-sol",
      runUrl: RUN_URL,
    });
    expect(body).toContain("**Code review in progress…**");
    expect(body).toContain(
      "Reviewing 4 changed file(s) at `abcdefa` — incremental scope: changes since successfully reviewed head 1111111.",
    );
    expect(body).toContain("openai:gpt-5.6-sol");
    expect(body).toContain(`[workflow run](${RUN_URL})`);
    expect(body).toContain(PROGRESS_COMMENT_MARKER);
  });

  it("renders an honest minimal in-progress body when scope is unknown", () => {
    expect(renderProgressBeginBody({})).toContain("Reviewing this pull request.");
  });

  it("renders each settled outcome with its own callout", () => {
    const base = {
      outcome: "reviewed",
      verdict: "approve",
      inlineComments: 2,
      reviewUrl: "https://github.test/acme/widgets/pull/5#pullrequestreview-1",
      runUrl: RUN_URL,
    } as const;
    const success = renderProgressSettleBody({ ...base, conclusion: "success" });
    expect(success).toContain("✅ **Code review posted**");
    expect(success).toContain("verdict `approve`, 2 inline comment(s)");
    expect(success).toContain(`[posted review](${base.reviewUrl})`);
    expect(success).toContain(PROGRESS_COMMENT_MARKER);

    expect(renderProgressSettleBody({ ...base, conclusion: "blocking" })).toContain(
      "🛑 **Code review posted** — blocking findings",
    );
    expect(renderProgressSettleBody({ ...base, conclusion: "incomplete" })).toContain(
      "required coverage is incomplete",
    );

    const failed = renderProgressSettleBody({ outcome: "failed", runUrl: RUN_URL });
    expect(failed).toContain("⚠️ **Code review run failed** — nothing was posted.");
    expect(failed).toContain(`[workflow run](${RUN_URL})`);
  });
});

// ---------------------------------------------------------------------------
// The GitHub adapter: sticky-comment lookup, upsert, and fail-open faults.
// ---------------------------------------------------------------------------

interface RecordedRequest {
  readonly method: string;
  readonly path: string;
  readonly body: string | undefined;
}

const progressHarness = (options: {
  readonly existingComments: unknown;
  readonly status?: number;
}) =>
  Effect.gen(function* () {
    const recorded = yield* Ref.make<ReadonlyArray<RecordedRequest>>([]);
    const client = HttpClient.make((request, url) =>
      Effect.gen(function* () {
        yield* Ref.update(recorded, (previous) => [
          ...previous,
          {
            method: request.method,
            path: `${url.pathname}${url.search}`,
            body:
              request.body._tag === "Uint8Array"
                ? new TextDecoder().decode(request.body.body)
                : undefined,
          },
        ]);
        const payload =
          request.method === "GET"
            ? options.existingComments
            : { id: 987, body: "", user: { login: "kommunikasie[bot]", type: "Bot" } };
        return HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(payload), {
            status: options.status ?? 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }),
    );
    const layer = gitHubReviewProgressLayer.pipe(
      Layer.provide(
        Layer.merge(
          GitHubReviewTarget.layer({
            apiUrl: "https://api.github.test",
            repository: "acme/widgets",
            number: 30,
            token: Option.some(Redacted.make("github-app-token")),
            reviewAuthorLogin: "Kommunikasie[bot]",
          }),
          Layer.succeed(HttpClient.HttpClient)(client),
        ),
      ),
    );
    return { recorded, layer };
  });

describe("gitHubReviewProgressLayer", () => {
  it.effect("creates the sticky comment on begin and patches it in place on settle", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({ existingComments: [] });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({ headSha: HEAD_SHA, filesInScope: 2 });
        yield* progress.settle({
          outcome: "reviewed",
          conclusion: "success",
          verdict: "approve",
          inlineComments: 0,
          reviewUrl: "https://github.test/acme/widgets/pull/30#pullrequestreview-1",
        });
      }).pipe(Effect.provide(harness.layer));
      const requests = yield* Ref.get(harness.recorded);
      expect(requests.map((request) => `${request.method} ${request.path}`)).toEqual([
        "GET /repos/acme/widgets/issues/30/comments?per_page=100&page=1",
        "POST /repos/acme/widgets/issues/30/comments",
        // The created id is remembered: settling needs no second lookup.
        "PATCH /repos/acme/widgets/issues/comments/987",
      ]);
      expect(requests[1]?.body).toContain("Code review in progress");
      expect(requests[1]?.body).toContain(PROGRESS_COMMENT_MARKER);
      expect(requests[2]?.body).toContain("Code review posted");
    }),
  );

  it.effect("updates only the configured bot's marker comment, never a look-alike", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({
        existingComments: [
          // A user pasting the marker must never have their comment edited.
          { id: 1, body: PROGRESS_COMMENT_MARKER, user: { login: "mallory", type: "User" } },
          { id: 2, body: PROGRESS_COMMENT_MARKER, user: { login: "other-app[bot]", type: "Bot" } },
          {
            id: 55,
            body: PROGRESS_COMMENT_MARKER,
            user: { login: "kommunikasie[bot]", type: "Bot" },
          },
        ],
      });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
      }).pipe(Effect.provide(harness.layer));
      const requests = yield* Ref.get(harness.recorded);
      expect(requests.map((request) => `${request.method} ${request.path}`)).toEqual([
        "GET /repos/acme/widgets/issues/30/comments?per_page=100&page=1",
        "PATCH /repos/acme/widgets/issues/comments/55",
      ]);
    }),
  );

  it.effect("degrades GitHub faults to a warning instead of failing the run", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({ existingComments: [], status: 500 });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
        yield* progress.settle({ outcome: "failed" });
      }).pipe(Effect.provide(harness.layer));
      const requests = yield* Ref.get(harness.recorded);
      expect(requests.length).toBeGreaterThan(0);
      expect(requests.every((request) => request.method === "GET")).toBe(true);
    }),
  );
});
