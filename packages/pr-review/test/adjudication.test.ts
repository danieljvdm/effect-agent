import { describe, expect, it } from "@effect/vitest";
import { DateTime, Effect } from "effect";

import {
  AdjudicableThread,
  AdjudicationComment,
  adjudicationIdentity,
  collectReviewAdjudications,
  deriveAdjudications,
  mergeAdjudications,
  parseIssueAdjudication,
  parseThreadAdjudication,
  ReviewAdjudicationFailure,
  ReviewAdjudicationHost,
  StoredAdjudication,
  threadFindingTarget,
} from "../src/index.ts";

const at = (iso: string) => DateTime.makeUnsafe(iso);

const comment = (
  body: string,
  overrides: Partial<{
    readonly authorAssociation: string;
    readonly authorLogin: string;
    readonly createdAt: DateTime.Utc | null;
  }> = {},
) =>
  AdjudicationComment.make({
    body,
    authorAssociation: overrides.authorAssociation ?? "MEMBER",
    authorLogin: overrides.authorLogin ?? "dan",
    createdAt: overrides.createdAt === undefined ? at("2026-08-01T10:00:00Z") : overrides.createdAt,
  });

const findingThread = (
  replies: ReadonlyArray<AdjudicationComment>,
  overrides: Partial<{
    readonly path: string;
    readonly startLine: number | null;
    readonly endLine: number | null;
    readonly rootBody: string;
  }> = {},
) =>
  AdjudicableThread.make({
    path: overrides.path ?? "src/api.ts",
    startLine: overrides.startLine === undefined ? 10 : overrides.startLine,
    endLine: overrides.endLine === undefined ? 12 : overrides.endLine,
    rootBody:
      overrides.rootBody ?? "**[🛑 blocking · security] Missing auth check**\n\nDetails follow.",
    replies,
  });

describe("adjudication command grammar", () => {
  it("parses every disposition with and without a reason on a thread reply", () => {
    expect(parseThreadAdjudication("/adjudicate accepted-risk")).toEqual({
      disposition: "accepted-risk",
      reason: undefined,
    });
    expect(parseThreadAdjudication("/adjudicate refuted: validated upstream")).toEqual({
      disposition: "refuted",
      reason: "validated upstream",
    });
    expect(
      parseThreadAdjudication("/adjudicate obsolete : superseded by the rewrite\nmore prose"),
    ).toEqual({ disposition: "obsolete", reason: "superseded by the rewrite" });
  });

  it("parses the quoted-title issue-comment grammar", () => {
    expect(
      parseIssueAdjudication('/adjudicate refuted "No test pins the new export": covered by e2e'),
    ).toEqual({
      disposition: "refuted",
      title: "No test pins the new export",
      reason: "covered by e2e",
    });
    expect(parseIssueAdjudication('/adjudicate accepted-risk "Known cost"')).toEqual({
      disposition: "accepted-risk",
      title: "Known cost",
      reason: undefined,
    });
  });

  it("ignores malformed commands and treats non-commands as absent", () => {
    expect(parseThreadAdjudication("looks fine to me")).toBeUndefined();
    expect(parseThreadAdjudication("/adjudicate")).toBe("malformed");
    expect(parseThreadAdjudication("/adjudicate maybe")).toBe("malformed");
    expect(parseThreadAdjudication("/adjudicated accepted-risk")).toBe("malformed");
    expect(parseIssueAdjudication("/adjudicate refuted no quoted title")).toBe("malformed");
    expect(parseIssueAdjudication(`/adjudicate refuted "${"t".repeat(121)}"`)).toBe("malformed");
    expect(parseIssueAdjudication("plain conversation")).toBeUndefined();
  });

  it("names the thread target from the root comment's finding title line", () => {
    expect(threadFindingTarget(findingThread([]))).toEqual({
      path: "src/api.ts",
      startLine: 10,
      endLine: 12,
      title: "Missing auth check",
    });
    expect(threadFindingTarget(findingThread([], { rootBody: "a human comment" }))).toBeUndefined();
    expect(threadFindingTarget(findingThread([], { startLine: null }))).toBeUndefined();
  });
});

describe("adjudication derivation", () => {
  it("authorizes fail-closed: only OWNER, MEMBER, and COLLABORATOR adjudicate", () => {
    const derived = deriveAdjudications({
      threads: [
        findingThread([
          comment("/adjudicate refuted: drive-by", { authorAssociation: "NONE" }),
          comment("/adjudicate refuted: contributor", { authorAssociation: "CONTRIBUTOR" }),
          comment("/adjudicate refuted: first-timer", {
            authorAssociation: "FIRST_TIME_CONTRIBUTOR",
          }),
        ]),
      ],
      issueComments: [
        comment('/adjudicate obsolete "Open question"', { authorAssociation: "COLLABORATOR" }),
      ],
    });
    expect(derived.adjudications).toHaveLength(1);
    expect(derived.adjudications[0]?.title).toBe("Open question");
    expect(derived.ignored).toHaveLength(3);
    expect(derived.ignored.join("\n")).toContain("unauthorized");
  });

  it("ignores malformed command bodies and threads without a parsable title", () => {
    const derived = deriveAdjudications({
      threads: [
        findingThread([comment("/adjudicate perhaps: not a disposition")]),
        findingThread([comment("/adjudicate refuted")], { rootBody: "no finding title here" }),
      ],
      issueComments: [comment("/adjudicate refuted missing quotes")],
    });
    expect(derived.adjudications).toHaveLength(0);
    expect(derived.ignored).toHaveLength(3);
    expect(derived.ignored.join("\n")).toContain("malformed");
    expect(derived.ignored.join("\n")).toContain("no parsable finding title");
  });

  it("lets the later adjudication of one identity win by creation order", () => {
    const derived = deriveAdjudications({
      threads: [
        findingThread([
          comment("/adjudicate accepted-risk: first", { createdAt: at("2026-08-01T10:00:00Z") }),
          comment("/adjudicate refuted: second", { createdAt: at("2026-08-02T10:00:00Z") }),
        ]),
      ],
      issueComments: [],
    });
    expect(derived.adjudications).toHaveLength(1);
    expect(derived.adjudications[0]?.disposition).toBe("refuted");
    expect(derived.adjudications[0]?.reason).toBe("second");

    // Listing order must not beat creation time: the dated later comment wins
    // even when it arrives earlier in the listing, and an undated comment
    // loses to any dated one.
    const reordered = deriveAdjudications({
      threads: [],
      issueComments: [
        comment('/adjudicate refuted "Open question": late', {
          createdAt: at("2026-08-03T10:00:00Z"),
        }),
        comment('/adjudicate accepted-risk "Open question": early', {
          createdAt: at("2026-08-01T10:00:00Z"),
        }),
        comment('/adjudicate obsolete "Open question": undated', { createdAt: null }),
      ],
    });
    expect(reordered.adjudications).toHaveLength(1);
    expect(reordered.adjudications[0]?.disposition).toBe("refuted");
  });

  it("caps stored adjudications at the schema bound dropping the oldest", () => {
    const derived = deriveAdjudications({
      threads: [],
      issueComments: Array.from({ length: 22 }, (_, index) =>
        comment(`/adjudicate refuted "Concern ${String(index).padStart(2, "0")}"`, {
          createdAt: at(`2026-08-01T10:${String(index).padStart(2, "0")}:00Z`),
        }),
      ),
    });
    expect(derived.adjudications).toHaveLength(20);
    expect(derived.droppedOldest).toBe(2);
    expect(derived.adjudications[0]?.title).toBe("Concern 02");
    expect(derived.adjudications[19]?.title).toBe("Concern 21");
  });
});

describe("adjudication collection", () => {
  const priorAdjudication = StoredAdjudication.make({
    path: "src/api.ts",
    startLine: 10,
    endLine: 12,
    title: "Missing auth check",
    disposition: "accepted-risk",
    reason: "stored from the prior round",
    actor: "dan",
  });

  it.effect("merges fresh host adjudications later-wins over the stored prior set", () =>
    Effect.gen(function* () {
      const host = ReviewAdjudicationHost.of({
        listFindingThreads: Effect.succeed([
          findingThread([comment("/adjudicate refuted: fresh verdict", { authorLogin: "maude" })]),
        ]),
        listIssueComments: Effect.succeed([comment('/adjudicate obsolete "Open question"')]),
      });
      const merged = yield* collectReviewAdjudications([priorAdjudication]).pipe(
        Effect.provideService(ReviewAdjudicationHost, host),
      );
      expect(merged).toHaveLength(2);
      const byIdentity = new Map(merged.map((entry) => [adjudicationIdentity(entry), entry]));
      const refreshed = byIdentity.get(adjudicationIdentity(priorAdjudication));
      expect(refreshed?.disposition).toBe("refuted");
      expect(refreshed?.actor).toBe("maude");
      expect(byIdentity.get("Open question")?.disposition).toBe("obsolete");
    }),
  );

  it.effect("fails open: a listing fault keeps the stored prior adjudications", () =>
    Effect.gen(function* () {
      const failure = ReviewAdjudicationFailure.make({
        operation: "listReviewCommentsForAdjudication",
        reason: "scripted failure",
      });
      const host = ReviewAdjudicationHost.of({
        listFindingThreads: Effect.fail(failure),
        listIssueComments: Effect.fail(failure),
      });
      const kept = yield* collectReviewAdjudications([priorAdjudication]).pipe(
        Effect.provideService(ReviewAdjudicationHost, host),
      );
      expect(kept).toEqual([priorAdjudication]);
      // Without a host at all, the stored set stands unchanged.
      const withoutHost = yield* collectReviewAdjudications([priorAdjudication]);
      expect(withoutHost).toEqual([priorAdjudication]);
    }),
  );

  it("merge is later-wins by identity and bounded", () => {
    const fresh = StoredAdjudication.make({
      path: priorAdjudication.path,
      startLine: priorAdjudication.startLine,
      endLine: priorAdjudication.endLine,
      title: priorAdjudication.title,
      disposition: "refuted",
      actor: "maude",
    });
    expect(mergeAdjudications([priorAdjudication], [fresh])).toEqual([fresh]);
    const many = Array.from({ length: 25 }, (_, index) =>
      StoredAdjudication.make({
        title: `Concern ${String(index).padStart(2, "0")}`,
        disposition: "refuted",
        actor: "dan",
      }),
    );
    const bounded = mergeAdjudications(many, []);
    expect(bounded).toHaveLength(20);
    expect(bounded[0]?.title).toBe("Concern 05");
  });
});
