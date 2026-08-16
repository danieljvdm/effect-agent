import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GitHubReviewTarget,
  gitHubReviewProgressLayer,
  parseProgressClaim,
  PROGRESS_COMMENT_MARKER_PREFIX,
  renderProgressBeginBody,
  renderProgressClaimMarker,
  renderProgressSettleBody,
  ReviewProgressReporter,
  type ProgressClaim,
} from "../src/index.ts";

const HEAD_SHA = "abcdefabcdefabcdefabcdefabcdefabcdefabcd";
const RUN_URL = "https://github.test/acme/widgets/actions/runs/9";
const CLAIM: ProgressClaim = { runToken: "test-run", startedMillis: 1_755_000_000_000 };

describe("progress claim markers", () => {
  it("round-trips a claim through its rendered marker", () => {
    const marker = renderProgressClaimMarker(CLAIM);
    expect(marker).toBe(`${PROGRESS_COMMENT_MARKER_PREFIX} run=test-run started=1755000000000 -->`);
    expect(parseProgressClaim(`before\n${marker}\nafter`)).toEqual(CLAIM);
  });

  it("sanitizes token content and rejects bodies without a parseable claim", () => {
    const marker = renderProgressClaimMarker({ runToken: "a -->b<!--", startedMillis: 5 });
    expect(marker).toContain("run=a-b- started=5");
    expect(parseProgressClaim("no marker here")).toBeUndefined();
    expect(parseProgressClaim(`${PROGRESS_COMMENT_MARKER_PREFIX} -->`)).toBeUndefined();
  });
});

describe("progress comment rendering", () => {
  it("renders the in-progress body with scope, model, run link, and claim", () => {
    const body = renderProgressBeginBody(
      {
        headSha: HEAD_SHA,
        reviewMode: "incremental",
        reviewReason: "changes since successfully reviewed head 1111111",
        filesInScope: 4,
        modelLabel: "openai:gpt-5.6-sol",
        runUrl: RUN_URL,
      },
      CLAIM,
    );
    expect(body).toContain("**Code review in progress…**");
    expect(body).toContain(
      "Reviewing 4 changed file(s) at `abcdefa` — incremental scope: changes since successfully reviewed head 1111111.",
    );
    expect(body).toContain("openai:gpt-5.6-sol");
    expect(body).toContain(`[workflow run](${RUN_URL})`);
    expect(parseProgressClaim(body)).toEqual(CLAIM);
  });

  it("renders an honest minimal in-progress body when scope is unknown", () => {
    expect(renderProgressBeginBody({}, CLAIM)).toContain("Reviewing this pull request.");
  });

  it("renders each settled outcome with its own callout", () => {
    const base = {
      outcome: "reviewed",
      verdict: "approve",
      inlineComments: 2,
      reviewUrl: "https://github.test/acme/widgets/pull/5#pullrequestreview-1",
      runUrl: RUN_URL,
    } as const;
    const success = renderProgressSettleBody({ ...base, conclusion: "success" }, CLAIM);
    expect(success).toContain("✅ **Code review posted**");
    expect(success).toContain("verdict `approve`, 2 inline comment(s)");
    expect(success).toContain(`[posted review](${base.reviewUrl})`);
    expect(parseProgressClaim(success)).toEqual(CLAIM);

    expect(renderProgressSettleBody({ ...base, conclusion: "blocking" }, CLAIM)).toContain(
      "🛑 **Code review posted** — blocking findings",
    );
    expect(renderProgressSettleBody({ ...base, conclusion: "incomplete" }, CLAIM)).toContain(
      "required coverage is incomplete",
    );

    const failed = renderProgressSettleBody({ outcome: "failed", runUrl: RUN_URL }, CLAIM);
    expect(failed).toContain("⚠️ **Code review run failed** — nothing was posted.");
    expect(failed).toContain(`[workflow run](${RUN_URL})`);
  });
});

// ---------------------------------------------------------------------------
// The GitHub adapter over an in-memory comment store: sticky-comment lookup,
// generation-fenced upsert, duplicate reconciliation, and fail-open faults.
// ---------------------------------------------------------------------------

interface StoredComment {
  readonly id: number;
  readonly body: string;
  readonly user: { readonly login: string; readonly type: string };
}

const BOT = { login: "kommunikasie[bot]", type: "Bot" } as const;

const progressHarness = (options: {
  readonly initialComments: ReadonlyArray<StoredComment>;
  readonly status?: number;
}) =>
  Effect.gen(function* () {
    const store = yield* Ref.make(options.initialComments);
    const operations = yield* Ref.make<ReadonlyArray<string>>([]);
    const client = HttpClient.make((request, url) =>
      Effect.gen(function* () {
        yield* Ref.update(operations, (previous) => [
          ...previous,
          `${request.method} ${url.pathname}`,
        ]);
        const respond = (payload: unknown, status = options.status ?? 200) =>
          HttpClientResponse.fromWeb(
            request,
            new Response(status === 204 ? null : JSON.stringify(payload), {
              status,
              headers: { "Content-Type": "application/json" },
            }),
          );
        if ((options.status ?? 200) >= 400) return respond({});
        const requestBody =
          request.body._tag === "Uint8Array"
            ? (JSON.parse(new TextDecoder().decode(request.body.body)) as { body: string })
            : undefined;
        const single = /\/issues\/comments\/(\d+)$/.exec(url.pathname);
        if (single !== null) {
          const id = Number(single[1]);
          const comments = yield* Ref.get(store);
          const found = comments.find((comment) => comment.id === id);
          if (found === undefined) return respond({ message: "Not Found" }, 404);
          if (request.method === "GET") return respond(found);
          if (request.method === "PATCH" && requestBody !== undefined) {
            const updated = { ...found, body: requestBody.body };
            yield* Ref.update(store, (previous) =>
              previous.map((comment) => (comment.id === id ? updated : comment)),
            );
            return respond(updated);
          }
          if (request.method === "DELETE") {
            yield* Ref.update(store, (previous) => previous.filter((comment) => comment.id !== id));
            return respond(undefined, 204);
          }
        }
        if (url.pathname.endsWith("/issues/30/comments")) {
          if (request.method === "GET") return respond(yield* Ref.get(store));
          if (request.method === "POST" && requestBody !== undefined) {
            const created = { id: 1000, body: requestBody.body, user: BOT };
            yield* Ref.update(store, (previous) => [...previous, created]);
            return respond(created, 201);
          }
        }
        return respond({ message: "unexpected request" }, 500);
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
    return { store, operations, layer };
  });

const mutations = (operations: ReadonlyArray<string>): ReadonlyArray<string> =>
  operations.filter((operation) => !operation.startsWith("GET "));

describe("gitHubReviewProgressLayer", () => {
  it.effect("creates the sticky comment on begin and fences the settle update", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({ initialComments: [] });
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
      expect(yield* Ref.get(harness.operations)).toEqual([
        "GET /repos/acme/widgets/issues/30/comments",
        "POST /repos/acme/widgets/issues/30/comments",
        // The settle re-reads the remembered comment to fence the overwrite.
        "GET /repos/acme/widgets/issues/comments/1000",
        "PATCH /repos/acme/widgets/issues/comments/1000",
      ]);
      const comments = yield* Ref.get(harness.store);
      expect(comments).toHaveLength(1);
      expect(comments[0]?.body).toContain("Code review posted");
      expect(parseProgressClaim(comments[0]?.body ?? "")).toBeDefined();
    }),
  );

  it.effect("adopts only the configured bot's marker comment, never a look-alike", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({
        initialComments: [
          // A user pasting the marker must never have their comment edited.
          {
            id: 1,
            body: `${PROGRESS_COMMENT_MARKER_PREFIX} -->`,
            user: { login: "mallory", type: "User" },
          },
          {
            id: 2,
            body: `${PROGRESS_COMMENT_MARKER_PREFIX} -->`,
            user: { login: "other-app[bot]", type: "Bot" },
          },
          // A pre-claim-era marker from the right bot is adoptable.
          { id: 55, body: `${PROGRESS_COMMENT_MARKER_PREFIX} -->`, user: BOT },
        ],
      });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
      }).pipe(Effect.provide(harness.layer));
      expect(mutations(yield* Ref.get(harness.operations))).toEqual([
        "PATCH /repos/acme/widgets/issues/comments/55",
      ]);
      expect((yield* Ref.get(harness.store)).map((comment) => comment.id)).toEqual([1, 2, 55]);
    }),
  );

  it.effect("claims over an older run's comment instead of creating another", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({
        initialComments: [
          {
            id: 80,
            body: renderProgressClaimMarker({ runToken: "older-run", startedMillis: 0 }),
            user: BOT,
          },
        ],
      });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
      }).pipe(Effect.provide(harness.layer));
      expect(mutations(yield* Ref.get(harness.operations))).toEqual([
        "PATCH /repos/acme/widgets/issues/comments/80",
      ]);
    }),
  );

  it.effect("never overwrites a newer run's status: stale begin and settle are fenced", () =>
    Effect.gen(function* () {
      const newerBody = renderProgressBeginBody(
        {},
        // Far-future claim: this run is stale relative to the comment owner.
        { runToken: "newer-run", startedMillis: 9_999_999_999_999 },
      );
      const harness = yield* progressHarness({
        initialComments: [{ id: 70, body: newerBody, user: BOT }],
      });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
        yield* progress.settle({ outcome: "failed" });
      }).pipe(Effect.provide(harness.layer));
      expect(mutations(yield* Ref.get(harness.operations))).toEqual([]);
      expect((yield* Ref.get(harness.store))[0]?.body).toBe(newerBody);
    }),
  );

  it.effect("reconciles duplicate marker comments down to the newest claim", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({
        initialComments: [
          {
            id: 10,
            body: renderProgressClaimMarker({ runToken: "run-a", startedMillis: 0 }),
            user: BOT,
          },
          {
            id: 20,
            body: renderProgressClaimMarker({ runToken: "run-b", startedMillis: 0 }),
            user: BOT,
          },
        ],
      });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
      }).pipe(Effect.provide(harness.layer));
      expect(mutations(yield* Ref.get(harness.operations))).toEqual([
        "DELETE /repos/acme/widgets/issues/comments/10",
        "PATCH /repos/acme/widgets/issues/comments/20",
      ]);
      expect((yield* Ref.get(harness.store)).map((comment) => comment.id)).toEqual([20]);
    }),
  );

  it.effect("degrades GitHub faults to a warning instead of failing the run", () =>
    Effect.gen(function* () {
      const harness = yield* progressHarness({ initialComments: [], status: 500 });
      yield* Effect.gen(function* () {
        const progress = yield* ReviewProgressReporter;
        yield* progress.begin({});
        yield* progress.settle({ outcome: "failed" });
      }).pipe(Effect.provide(harness.layer));
      const operations = yield* Ref.get(harness.operations);
      expect(operations.length).toBeGreaterThan(0);
      expect(mutations(operations)).toEqual([]);
    }),
  );
});
