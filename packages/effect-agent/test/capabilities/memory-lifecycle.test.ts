import { describe, expect, it } from "@effect/vitest";
import { Schema as NamespaceSchema, Effect, Ref } from "effect";
import * as Memory from "effect-agent/memory";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import {
  MemoryAttribution,
  MemoryContent,
  type MemoryLookup,
  MemoryPassage,
  MemoryRecallLimits,
} from "effect-agent/memory-reference";
import { MemoryAccess, revalidateMemoryLookup } from "effect-agent/memory-revalidation";
import {
  MemoryScope,
  ActiveMemoryDocument,
  MemoryKey,
  MemoryReader,
} from "effect-agent/memory-store";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const key = MemoryKey.make({ namespace: TestNamespace.make("team-a"), id: "queue-discussion" });

const access = MemoryAccess.make({
  namespace: TestNamespace.make("team-a"),
  scope: MemoryScope.make("participating-channels"),
});

const document = ActiveMemoryDocument.make({
  version: 1,
  key,
  source: { id: key.id, locator: "chat://engineering/42", revision: "1" },
  generation: 1,
  predecessor: null,
  modifiedAt: 30,
  scopes: [access.scope],
  content: MemoryContent.make({
    text: "Dan proposes a queue. No decision yet.",
    attributions: [
      MemoryAttribution.make({
        originId: "engineering:42",
        speaker: "Dan",
        observers: ["Chad"],
        locator: "chat://engineering/42",
        activityAt: 10,
        interpretation: "proposal",
      }),
    ],
    metadata: { topic: "delivery" },
    recordedAt: 20,
    extractedAt: 25,
  }),
});

const candidate = MemoryPassage.make({
  version: 1,
  source: document.source,
  passageId: "claim",
  content: document.content,
});

const candidates: MemoryLookup = { _tag: "Found", passages: [candidate] };

const limits = MemoryRecallLimits.make({
  maxSources: 4,
  maxItems: 8,
  maxBytes: 16_384,
  maxTokens: 16_384,
  timeoutMillis: 100,
});

const recall = (lookup = candidates) =>
  Memory.recall(
    [
      {
        id: "stale-cache",
        essential: true,
        read: revalidateMemoryLookup(lookup, access),
      },
    ],
    limits,
  );

describe("authoritative memory validation", () => {
  it.effect("preserves independently authorized namespaces through recall composition", () =>
    Effect.gen(function* () {
      const accesses = [
        access,
        MemoryAccess.make({ ...access, namespace: TestNamespace.make("team-b") }),
      ];

      const lookup: MemoryLookup = {
        _tag: "Found",
        passages: [
          MemoryPassage.make({
            ...candidate,
            source: { ...candidate.source, id: "profile" },
            passageId: "document",
            authority: "forged-candidate-authority",
          }),
        ],
      };

      {
        const projectText = "Different project preference";

        const current = accesses.map((bound, index) =>
          ActiveMemoryDocument.make({
            ...document,
            key: { namespace: bound.namespace, id: "profile" },
            source: { ...document.source, id: "profile" },
            content: {
              ...document.content,
              text: index === 0 ? document.content.text : projectText,
            },
          }),
        );

        const result = yield* Memory.recall(
          accesses.map((bound, index) => ({
            id: `reader-${index}`,
            essential: true,
            read: revalidateMemoryLookup(lookup, bound).pipe(
              Effect.provideService(
                MemoryReader,
                MemoryReader.fromAdapter({
                  get: () => Effect.succeed(current[index] ?? null),
                }),
              ),
            ),
          })),
          limits,
        );

        expect(result.passages.map((passage) => passage.authority)).toEqual(
          accesses.map((bound) => bound.namespace.address),
        );
        expect(result.passages.map((passage) => passage.content.text)).toEqual([
          document.content.text,
          projectText,
        ]);
        expect(result.text).not.toContain("forged-candidate-authority");
        expect(result.text).not.toContain("team-a");
        expect(result.text).not.toContain("team-b");
      }
    }),
  );

  it.effect(
    "checks every recall again and replaces forged provenance and nonexistent excerpts",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make(0);

        const reader = MemoryReader.fromAdapter({
          get: () =>
            Ref.getAndUpdate(reads, (n) => n + 1).pipe(
              Effect.map((n) => (n === 0 ? document : null)),
            ),
        });

        const forged = MemoryPassage.make({
          ...candidate,
          content: {
            ...candidate.content,
            text: "An invented agreement",
            attributions: [{ ...candidate.content.attributions[0], speaker: "Adam" }],
          },
        });

        const first = yield* recall({ _tag: "Found", passages: [forged, forged] }).pipe(
          Effect.provideService(MemoryReader, reader),
        );

        expect(first.passages).toHaveLength(1);
        expect(first.passages[0]?.content).toEqual(document.content);
        expect(yield* Ref.get(reads)).toBe(1);
        const second = yield* recall().pipe(Effect.provideService(MemoryReader, reader));

        expect(second.passages).toEqual([]);
        expect(yield* Ref.get(reads)).toBe(2);
      }),
  );
});
