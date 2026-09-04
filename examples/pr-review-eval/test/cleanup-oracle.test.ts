import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Clock, Context, Effect, FileSystem, Path, Ref } from "effect";

import { loadEvalSuite } from "../src/corpus.ts";
import { makeCleanup as base } from "./fixtures/cleanup-base.ts";
import { makeCleanup as cached } from "./fixtures/cleanup-cached.ts";
import { makeCleanup as original } from "./fixtures/cleanup-original.ts";

it.effect.each([
  { name: "base", make: base, late: "confirmed", closed: ["late-session"], after: "confirmed" },
  {
    name: "original",
    make: original,
    late: "confirmed",
    closed: ["late-session"],
    after: "unconfirmed",
  },
  { name: "cached", make: cached, late: "unconfirmed", closed: [], after: "unconfirmed" },
])("proves the pinned Effect cleanup oracle: $name", ({ make, late, closed, after }) =>
  Effect.gen(function* () {
    const closedSessions = yield* Ref.make<ReadonlyArray<string>>([]);
    const early = yield* Ref.make<ReadonlyArray<string>>([]);
    const cleanup = yield* make((id) => Ref.update(closedSessions, (ids) => [...ids, id]));
    const runCleanup = Effect.runPromiseWith(Context.make(Clock.Clock, yield* Clock.Clock));

    yield* Effect.addFinalizer(() =>
      cleanup.close.pipe(
        Effect.flatMap((outcome) => Ref.update(early, (outcomes) => [...outcomes, outcome])),
      ),
    ).pipe(Effect.scoped);

    expect(yield* Ref.get(early)).toEqual(["unconfirmed"]);
    expect(yield* Ref.get(closedSessions)).toEqual([]);
    expect(yield* Effect.promise(() => runCleanup(cleanup.acquired("late-session")))).toBe(late);
    expect(yield* Ref.get(closedSessions)).toEqual(closed);
    expect(yield* cleanup.close).toBe(after);
  }),
);

it.effect(
  "binds the scored cleanup source to the executed proof and excludes the operational replay",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const suite = yield* loadEvalSuite(
        yield* path.fromFileUrl(new URL("../fixtures/verification-corpus.json", import.meta.url)),
      );

      const baseSource = yield* fs.readFileString(
        yield* path.fromFileUrl(new URL("./fixtures/cleanup-base.ts", import.meta.url)),
      );

      for (const variant of ["original", "cached"] as const) {
        const evalCase = suite.cases.find(({ id }) => id === `cleanup-${variant}`);

        const headSource = yield* fs.readFileString(
          yield* path.fromFileUrl(new URL(`./fixtures/cleanup-${variant}.ts`, import.meta.url)),
        );

        expect(
          evalCase?.repository?.files.find(
            ({ path, revision }) => path === "src/cleanup.ts" && revision === "base",
          )?.content,
        ).toBe(baseSource);
        expect(
          evalCase?.repository?.files.find(
            ({ path, revision }) => path === "src/cleanup.ts" && revision === "head",
          )?.content,
        ).toBe(headSource);
        expect(evalCase?.kind).toBe(variant === "original" ? "clean-control" : "known-defects");
        expect(evalCase?.split).toBe("development");
        expect(evalCase?.relatedGroup).toBe("late-cleanup");
      }

      const replay = suite.cases.find(({ id }) => id === "effect-agent-287-9cae87d");

      expect(replay?.kind).toBe("unadjudicated");
      expect(replay?.expectedDefects).toEqual([]);
      expect(replay?.request.headRevision).toBe("9cae87d6cc0234867136137b23a03ba2471265f3");
      expect(replay?.request.changes).toHaveLength(27);
    }).pipe(Effect.provide(NodeServices.layer)),
);
