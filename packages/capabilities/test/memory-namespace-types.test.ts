import {
  type MemoryDocument,
  type SemanticMemoryProfile,
  ActiveMemoryDocument,
  applyMemoryWrite,
  MemoryIndexCandidate,
  MemoryIndexQuery,
  MemoryIndexReplacement,
  MemoryIndexSearch,
  MemoryIndexSource,
  MemoryKey,
  MemoryNamespace,
  MemoryScope,
  type MemoryNamespaceError,
  MemoryReader,
  type MemoryStorageError,
  MemoryWrite,
  type MemoryWriteError,
  MemoryWriter,
  SemanticMemoryIndex,
} from "@effect-agent/core";
import { Effect, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { MemoryAccess, indexMemorySource, SemanticIndexLimits } from "../src/index.ts";

const TenantId = Schema.NonEmptyString.pipe(Schema.brand("app/TenantId"));
const UserId = Schema.NonEmptyString.pipe(Schema.brand("app/UserId"));
const identity = Schema.Struct({ tenantId: TenantId, userId: UserId });
const Conversations = MemoryNamespace.define({ name: "app/conversations", version: 1, identity });
const Other = MemoryNamespace.define({ name: "app/other", version: 1, identity });
const V2 = MemoryNamespace.define({ name: "app/conversations", version: 2, identity });
const tenantId = TenantId.make("tenant");
const userId = UserId.make("user");
const namespace = Conversations.make({ tenantId, userId });

type Namespace = typeof namespace;
const key = MemoryKey.make({ namespace, id: "source" });
const access = MemoryAccess.make({ namespace, scope: MemoryScope.make("private") });

const content = {
  text: "memory",
  attributions: [
    {
      originId: "origin",
      speaker: "host",
      observers: [],
      locator: "memory://source",
      activityAt: 0,
      interpretation: "statement",
    },
  ],
  metadata: {},
  recordedAt: 0,
  extractedAt: 0,
};

const write = MemoryWrite.make({
  _tag: "Put",
  key,
  operationId: "operation",
  expectedRevision: null,
  locator: "memory://source",
  content,
  scopes: [access.scope],
});

const document = ActiveMemoryDocument.make({
  version: 1,
  key,
  source: { id: key.id, locator: "memory://source", revision: "1" },
  generation: 1,
  predecessor: null,
  modifiedAt: 0,
  content,
  scopes: [access.scope],
});

const source = MemoryIndexSource.make({ key, source: document.source, sourceGeneration: 1 });

const query = MemoryIndexQuery.make({
  namespace,
  vector: [1],
  limit: 1,
  minScore: 0,
  maxScannedChunks: 1,
});

const candidate = MemoryIndexCandidate.make({
  ...source,
  passageId: "passage",
  ordinal: 0,
  startByte: 0,
  endByte: 6,
  text: "memory",
  score: 1,
  indexedAt: 0,
});

const search = MemoryIndexSearch.make({ candidates: [candidate], scannedChunks: 1 });

const replacement = (profile: SemanticMemoryProfile) =>
  MemoryIndexReplacement.make({
    source,
    profile,
    chunks: [
      { passageId: "passage", ordinal: 0, startByte: 0, endByte: 6, text: "memory", vector: [1] },
    ],
  });

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Assert<Value extends true> = Value;

const workflow = Effect.gen(function* () {
  const reader = yield* MemoryReader;
  const writer = yield* MemoryWriter;
  const index = yield* SemanticMemoryIndex;
  const read = reader.get(key);
  const changed = writer.change(write);
  const found = index.search(query);

  const indexed = indexMemorySource(
    key,
    SemanticIndexLimits.make({ maxSourceBytes: 100, maxChunks: 1, timeoutMillis: 100 }),
  );

  const changedDocument = yield* changed;
  const foundCandidates = yield* found;

  return { read, changed, found, indexed, changedDocument, foundCandidates };
});

type Workflow = Effect.Success<typeof workflow>;
type Proofs = [
  Assert<Equal<typeof access.scope, MemoryScope>>,
  Assert<Equal<(typeof document.scopes)[number], MemoryScope>>,
  Assert<Equal<Extract<MemoryWrite, { readonly _tag: "Put" }>["scopes"][number], MemoryScope>>,
  Assert<Equal<typeof key.namespace, Namespace>>,
  Assert<Equal<typeof access.namespace, Namespace>>,
  Assert<Equal<typeof write.key.namespace, Namespace>>,
  Assert<Equal<typeof document.key.namespace, Namespace>>,
  Assert<Equal<typeof source.key.namespace, Namespace>>,
  Assert<Equal<typeof candidate.key.namespace, Namespace>>,
  Assert<Equal<(typeof search.candidates)[number]["key"]["namespace"], Namespace>>,
  Assert<Equal<ReturnType<typeof replacement>["source"]["key"]["namespace"], Namespace>>,
  Assert<Equal<Workflow["changedDocument"]["key"]["namespace"], Namespace>>,
  Assert<Equal<Workflow["foundCandidates"]["candidates"][number]["key"]["namespace"], Namespace>>,
  Assert<Equal<Effect.Success<Workflow["indexed"]>["key"]["namespace"], Namespace>>,
  Assert<Equal<Effect.Error<Workflow["read"]>, MemoryStorageError>>,
  Assert<Equal<Effect.Error<Workflow["changed"]>, MemoryWriteError>>,
  Assert<
    Equal<Effect.Services<typeof workflow>, MemoryReader | MemoryWriter | SemanticMemoryIndex>
  >,
  Assert<Equal<Effect.Error<ReturnType<typeof Conversations.decode>>, MemoryNamespaceError>>,
  Assert<Equal<Effect.Services<ReturnType<typeof Conversations.decode>>, never>>,
];

const negativeCases = () => {
  // @ts-expect-error Missing branded user identity.
  Conversations.make({ tenantId });
  // @ts-expect-error Unbranded identities are not authenticated typed inputs.
  Conversations.make({ tenantId: "tenant", userId: "user" });
  // @ts-expect-error Brands distinguish identity categories.
  Conversations.make({ tenantId: userId, userId: tenantId });
  // @ts-expect-error Raw namespace strings are not namespace values.
  MemoryKey.make({ namespace: "tenant", id: "source" });
  // @ts-expect-error Access constructors reject strings too.
  MemoryAccess.make({ namespace: "tenant", scope: access.scope });
  // @ts-expect-error A raw string cannot stand in for a host-bound scope.
  MemoryAccess.make({ namespace, scope: "private" });
  // @ts-expect-error Unrelated branded identities cannot become memory scopes.
  MemoryAccess.make({ namespace, scope: userId });
  // @ts-expect-error Persisted document construction requires branded scopes too.
  ActiveMemoryDocument.make({ ...document, scopes: ["private"] });
  const other = Other.make({ tenantId, userId });
  const newer = V2.make({ tenantId, userId });
  const acceptKey = (_key: MemoryKey<Namespace>) => undefined;

  // @ts-expect-error Same identity shape does not make families interchangeable.
  acceptKey(MemoryKey.make({ namespace: other, id: "source" }));
  // @ts-expect-error Definition versions select different namespace families.
  acceptKey(MemoryKey.make({ namespace: newer, id: "source" }));
  // @ts-expect-error Namespace parameters are invariant, not a covariant phantom.
  const widened: MemoryNamespace.Value<string, number, unknown> = namespace;
  const acceptWrite = (_write: MemoryWrite<Namespace>) => undefined;
  const otherKey = MemoryKey.make({ namespace: other, id: key.id });
  const acceptAccess = (_access: MemoryAccess<Namespace>) => undefined;

  // @ts-expect-error Access construction retains the incompatible family.
  acceptAccess(MemoryAccess.make({ namespace: other, scope: access.scope }));
  const acceptQuery = (_query: MemoryIndexQuery<Namespace>) => undefined;

  // @ts-expect-error Query construction retains the incompatible version.
  acceptQuery(MemoryIndexQuery.make({ ...query, namespace: newer }));
  const otherWrite = MemoryWrite.make({ ...write, key: otherKey });

  // @ts-expect-error Write construction preserves the incompatible namespace.
  acceptWrite(otherWrite);

  const transition = applyMemoryWrite(
    // @ts-expect-error Paired transition arguments do not infer a union namespace.
    document,
    otherWrite,
    1,
  );

  const acceptSource = (_source: MemoryIndexSource<Namespace>) => undefined;
  const otherSource = MemoryIndexSource.make({ ...source, key: otherKey });

  // @ts-expect-error Index constructors must not erase a different family.
  acceptSource(otherSource);
  const acceptDocument = (_document: MemoryDocument<Namespace>) => undefined;

  const newerDocument = ActiveMemoryDocument.make({
    ...document,
    key: MemoryKey.make({ namespace: newer, id: key.id }),
  });

  // @ts-expect-error Document constructors preserve their actual namespace.
  acceptDocument(newerDocument);

  return { widened, transition };
};

it("retains namespace types through public construction and operations", () => {
  const proofs: Proofs = [
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
    true,
  ];

  expect(proofs.every(Boolean)).toBe(true);
  expect(typeof negativeCases).toBe("function");
  for (const value of ["private", "x".repeat(1_024)]) {
    const decodedAccess = Schema.decodeUnknownSync(MemoryAccess.Wire)({ ...access, scope: value });

    expect(Schema.encodeSync(MemoryAccess.Wire)(decodedAccess).scope).toBe(value);

    const decodedDocument = Schema.decodeUnknownSync(ActiveMemoryDocument.Wire)({
      ...document,
      scopes: [value],
    });

    expect(Schema.encodeSync(ActiveMemoryDocument.Wire)(decodedDocument).scopes).toEqual([value]);
  }
  for (const value of ["", "x".repeat(1_025)]) {
    expect(() => Schema.decodeUnknownSync(MemoryAccess.Wire)({ ...access, scope: value })).toThrow(
      /Expected/,
    );
    expect(() =>
      Schema.decodeUnknownSync(ActiveMemoryDocument.Wire)({ ...document, scopes: [value] }),
    ).toThrow(/Expected/);
    expect(() => Schema.decodeUnknownSync(MemoryWrite.Wire)({ ...write, scopes: [value] })).toThrow(
      /Expected/,
    );
  }
});
