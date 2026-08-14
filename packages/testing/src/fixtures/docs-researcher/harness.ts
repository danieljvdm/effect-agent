import { Crypto, Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import {
  connectMcp,
  Redactor,
  SubagentReservationsMemoryLive,
  type McpDiscovery,
  type RedactedPreview,
  type RedactionError,
} from "@effect-agent/capabilities";
import { Agent, type ConversationId } from "@effect-agent/core";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableWorkerBinding,
  Principal,
  ProducerId,
  type DurableSubmitOptions,
  type IdempotencyKey,
  type ResolvedBinding,
} from "@effect-agent/session";

import { DeterministicIdGeneratorLayer } from "../travel-planner/deterministic-layers.ts";
import {
  DocsResearcher,
  DocSummarizer,
  DocumentLibrary,
  docContentToolkitLayer,
  docsSummaryHandlersLayer,
  encodedDocumentSummary,
  expectedResearchDigest,
  ResearchDigest,
  ResearchDocument,
  researchCorpusDocumentIds,
  researchDocumentFor,
  researchDocumentLookup,
} from "./definition.ts";
import {
  assertDiscoveryMatchesAuthoredToolkit,
  docsMcpConnectorLayer,
  docsMcpRequest,
} from "./mcp.ts";

// ---------------------------------------------------------------------------
// DN durable harness for the docs-researcher (P7 plan §6 agent #3), following
// `makeDurableResearchHarness` conventions: invocation counters and captured
// prompts live OUTSIDE the Model Layers so they survive Layer rebuilds across
// Attempts and separate runtime handles over the same SQLite file.
// ---------------------------------------------------------------------------

export const docsResearcherDeploymentId = Schema.decodeSync(DeploymentId)(
  "docs-researcher-p7-deployment",
);
export const docsResearcherProducerId = Schema.decodeSync(ProducerId)(
  "docs-researcher-p7-producer",
);
export const docsResearcherPrincipal = Schema.decodeSync(Principal)("docs-researcher-p7-principal");

const digestOf = (pair: string) => Schema.decodeSync(Digest)(pair.repeat(32));

/** Redacted, deterministic coordinator definition digests for this fixture version. */
export const docsCoordinatorDigests = DefinitionDigests.make({
  agent: digestOf("40"),
  model: digestOf("41"),
  tools: digestOf("42"),
});

/** The child registration digests — byte-equal to `docsSummarizerDigestStrings` (SUB-023). */
export const docsSummarizerDigests = DefinitionDigests.make({
  agent: digestOf("50"),
  model: digestOf("51"),
  tools: digestOf("52"),
});

/** Durable admission options for one docs-researcher Submission on one mission lane. */
export const docsResearcherSubmitOptions = (
  conversationId: ConversationId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  conversationId,
  principal: docsResearcherPrincipal,
  idempotencyKey,
  definitions: docsCoordinatorDigests,
});

/** The structural submit slice of the coordinator Binding (`DurableAgentRuntime.submit`). */
export const docsResearcherSubmitAgent = {
  definition: { id: DocsResearcher.id, input: DocsResearcher.input },
} as const;

/** The deterministic delegation Tool Call identity for one document. */
export const summarizeCallId = (documentId: string): string => `summarize-${documentId}`;

/** The child's own scripted fetch Tool Call identity for one document. */
export const fetchCallId = (documentId: string): string => `fetch-${documentId}`;

const scriptedUsage = { inputTokens: { total: 96 }, outputTokens: { total: 64 } };

const summaryDelegationParts = (
  documentIds: ReadonlyArray<string>,
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...documentIds.map(
    (documentId): Response.StreamPartEncoded => ({
      type: "tool-call",
      id: summarizeCallId(documentId),
      name: "delegate_document_summary",
      params: { documentId },
      providerExecuted: false,
    }),
  ),
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const digestParts = (
  documentIds: ReadonlyArray<string>,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "digest" },
  {
    type: "text-delta",
    id: "digest",
    delta: JSON.stringify(Schema.encodeSync(ResearchDigest)(expectedResearchDigest(documentIds))),
  },
  { type: "text-end", id: "digest" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const fetchParts = (documentId: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: fetchCallId(documentId),
    name: "fetch_document",
    params: { documentId },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const summaryParts = (documentId: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "document-summary" },
  { type: "text-delta", id: "document-summary", delta: encodedDocumentSummary(documentId) },
  { type: "text-end", id: "document-summary" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/**
 * One prompt-aware scripted model with externally observable counters. A DN
 * Attempt may resume on a fresh Layer build, so responses derive from the
 * committed history in the prompt — never from an in-Layer turn counter.
 */
const makeCountingModel = (
  name: string,
  decide: (promptJson: string) => Effect.Effect<ReadonlyArray<Response.StreamPartEncoded>>,
) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
    const model = Model.make(
      "scripted",
      name,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Effect.gen(function* () {
                yield* Ref.update(calls, (value) => value + 1);
                const promptJson = JSON.stringify(request.prompt);
                yield* Ref.update(prompts, (previous) => [...previous, promptJson]);
                return Stream.fromIterable(yield* decide(promptJson));
              }),
            ),
        }),
      ),
    );
    return { model, calls: Ref.get(calls), prompts: Ref.get(prompts) };
  });

/** Optional overrides for one docs-researcher harness. */
export interface DocsResearcherHarnessOptions {
  /** Documents to research; defaults to the full two-document corpus. */
  readonly documentIds?: ReadonlyArray<string> | undefined;
}

/** One durable coordinator/summarizer pair with observable counters and MCP evidence. */
export interface DocsResearcherHarness {
  /** Host registrations for `NodeDurableRuntimeOptions.bindings` (parent + child). */
  readonly bindings: ReadonlyArray<ResolvedBinding>;
  /** The validated MCP discovery the child toolkit registration was gated on. */
  readonly discovery: McpDiscovery;
  /** Total coordinator model invocations across every Attempt and runtime handle. */
  readonly parentModelCalls: Effect.Effect<number>;
  /** JSON-encoded coordinator prompts in request order. */
  readonly parentPrompts: Effect.Effect<ReadonlyArray<string>>;
  /** Total summarizer model invocations across every Attempt and runtime handle. */
  readonly childModelCalls: Effect.Effect<number>;
  /** JSON-encoded summarizer prompts in request order (context-isolation evidence). */
  readonly childPrompts: Effect.Effect<ReadonlyArray<string>>;
  /** MCP content-tool handler executions for one document (external side-effect record). */
  readonly fetchInvocations: (documentId: string) => Effect.Effect<number>;
}

/**
 * Build the docs-researcher harness. Order matters and is the point: the
 * child's content toolkit is only registered as a worker Binding AFTER the
 * MCP connector's bounded discovery validated the authored toolkit
 * byte-for-byte (`connectMcp` + `assertDiscoveryMatchesAuthoredToolkit`), so
 * "the tools the summarizer runs are the tools discovery served" is enforced
 * at assembly, not assumed. Content-tool execution then flows through the
 * counting `DocumentLibrary` — the scripted MCP server's content store.
 */
export const makeDocsResearcherHarness = (options?: DocsResearcherHarnessOptions) =>
  Effect.gen(function* () {
    const documentIds = options?.documentIds ?? researchCorpusDocumentIds;

    // MCP discovery gate (CAP-009): bounded, digest-verified, fail-closed.
    const discovery = yield* Effect.scoped(
      Effect.gen(function* () {
        const connection = yield* connectMcp(docsMcpRequest);
        yield* assertDiscoveryMatchesAuthoredToolkit(connection);
        return connection.discovery;
      }),
    ).pipe(Effect.provide(docsMcpConnectorLayer));

    const fetchCounts = yield* Ref.make<ReadonlyMap<string, number>>(new Map());
    const libraryLayer = Layer.succeed(
      DocumentLibrary,
      DocumentLibrary.of({
        fetch: (query) =>
          Ref.update(fetchCounts, (current) =>
            new Map(current).set(query.documentId, (current.get(query.documentId) ?? 0) + 1),
          ).pipe(Effect.andThen(researchDocumentLookup(query))),
      }),
    );
    const childToolkitLayer = docContentToolkitLayer.pipe(Layer.provideMerge(libraryLayer));

    const childModel = yield* makeCountingModel("doc-summarizer-p7", (promptJson) =>
      Effect.suspend(() => {
        const documentId = documentIds.find((candidate) => promptJson.includes(candidate));
        if (documentId === undefined) {
          return Effect.die(new Error("The summarizer prompt names no corpus document"));
        }
        return Effect.succeed(
          promptJson.includes(fetchCallId(documentId))
            ? summaryParts(documentId)
            : fetchParts(documentId),
        );
      }),
    );
    const childBinding = Agent.withModel(DocSummarizer, childModel.model);

    const firstCallId = summarizeCallId(documentIds[0] ?? "durability-notes");
    const parentModel = yield* makeCountingModel("docs-researcher-p7", (promptJson) =>
      Effect.succeed(
        promptJson.includes(firstCallId)
          ? digestParts(documentIds)
          : summaryDelegationParts(documentIds),
      ),
    );
    const parentBinding = Agent.withModel(DocsResearcher, parentModel.model);

    const delegationLayer = docsSummaryHandlersLayer(childBinding).pipe(
      Layer.provide(
        Layer.mergeAll(
          childToolkitLayer,
          SubagentReservationsMemoryLive,
          DeterministicIdGeneratorLayer,
        ),
      ),
    );

    const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      parentBinding,
      docsCoordinatorDigests,
    ).pipe(Effect.provide(delegationLayer));
    const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      docsSummarizerDigests,
    ).pipe(Effect.provide(childToolkitLayer));

    const harness: DocsResearcherHarness = {
      bindings: [parentResolved, childResolved],
      discovery,
      parentModelCalls: parentModel.calls,
      parentPrompts: parentModel.prompts,
      childModelCalls: childModel.calls,
      childPrompts: childModel.prompts,
      fetchInvocations: (documentId) =>
        Ref.get(fetchCounts).pipe(Effect.map((current) => current.get(documentId) ?? 0)),
    };
    return harness;
  });

const encodeResearchDocument = Schema.encodeEffect(ResearchDocument);

/**
 * The audit-surface preview of one fetched document: the raw document —
 * secret marker and all — passes through the configured structural `Redactor`
 * before anything may quote it outside the child Conversation (SEC-008,
 * CAP-013). Tests assert the preview keeps shape but no scalar content.
 */
export const redactedDocumentPreview = Effect.fn("DocsResearcher.redactedDocumentPreview")(
  function* (documentId: string): Effect.fn.Return<RedactedPreview, RedactionError, Redactor> {
    const redactor = yield* Redactor;
    const encoded = yield* encodeResearchDocument(researchDocumentFor(documentId)).pipe(
      Effect.orDie,
    );
    return yield* redactor.redact(encoded);
  },
);

// Crypto is deliberately in the harness requirements (`connectMcp` digests
// discovery): callers provide a platform Crypto Layer, keeping this fixture
// platform-neutral.
export type DocsResearcherHarnessRequirements = Crypto.Crypto;
