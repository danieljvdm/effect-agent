import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentPolicy, SubagentRuntime } from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { type RuntimeBinding } from "@effect-agent/engine/AgentRuntime";
import { Context, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

// ---------------------------------------------------------------------------
// Docs Researcher (P7 internal agent #3, plan §6): a coordinator that
// delegates per-document summarization to a doc-summarizer child through the
// S2 durable delegation surface, with the child's content tools served —
// and validated — through the MCP connector against a scripted MCP fixture.
// The corpus, tools, and both Agent Definitions are deterministic fixtures in
// the travel-planner style so DN tests (and any later DC assembly) reuse them.
// ---------------------------------------------------------------------------

export const ResearchDocumentId = Schema.NonEmptyString.check(Schema.isMaxLength(64)).pipe(
  Schema.brand("@effect-agent/testing/docs-researcher/ResearchDocumentId"),
);

export type ResearchDocumentId = typeof ResearchDocumentId.Type;

const BoundedTitle = Schema.NonEmptyString.check(Schema.isMaxLength(120));
const BoundedBody = Schema.NonEmptyString.check(Schema.isMaxLength(16 * 1024));

/** Bounded summary text: the ONLY child-derived text that may cross to the parent. */
export const BoundedSummary = Schema.NonEmptyString.check(Schema.isMaxLength(240));

export class DocumentQuery extends Schema.Class<DocumentQuery>("DocumentQuery")({
  documentId: ResearchDocumentId,
}) {}

/** One bounded research document as the MCP content server exposes it. */
export class ResearchDocument extends Schema.Class<ResearchDocument>("ResearchDocument")({
  documentId: ResearchDocumentId,
  title: BoundedTitle,
  body: BoundedBody,
}) {}

export class DocumentUnavailable extends Schema.TaggedError<DocumentUnavailable>()(
  "DocumentUnavailable",
  {
    documentId: ResearchDocumentId,
    message: Schema.String,
  },
) {}

/** The content store behind the scripted MCP server. */
export class DocumentLibrary extends Context.Service<
  DocumentLibrary,
  {
    readonly fetch: (query: DocumentQuery) => Effect.Effect<ResearchDocument, DocumentUnavailable>;
  }
>()("@effect-agent/testing/docs-researcher/DocumentLibrary") {}

/**
 * The one content tool the doc-summarizer child uses. Its authored JSON
 * schema is what MCP discovery must serve byte-for-byte: the scripted MCP
 * fixture derives its discovery entry from `Tool.getJsonSchema(FetchDocument)`
 * and `validateMcpDiscovery` re-derives and digests both sides (CAP-009).
 */
export const FetchDocument = Tool.make("fetch_document", {
  description: "Fetch one bounded research document by its identifier.",
  parameters: DocumentQuery,
  success: ResearchDocument,
  failure: DocumentUnavailable,
  failureMode: "error",
  dependencies: [DocumentLibrary],
});

export const DocContentToolkit = Toolkit.make(FetchDocument);

export const docContentToolkitLayer = DocContentToolkit.toLayer({
  fetch_document: (query) => Effect.flatMap(DocumentLibrary, (library) => library.fetch(query)),
});

// ---------------------------------------------------------------------------
// Deterministic corpus. Every body deliberately embeds BOTH a secret marker
// and a distinctive body phrase: the tests assert that neither ever reaches
// the parent Thread, the parent prompts, or a redacted preview — only
// the bounded summary crosses the delegation boundary (SUB-015, SEC-008).
// ---------------------------------------------------------------------------

/** Never allowed outside a child Thread or an unredacted fixture value. */
export const docsDocumentBodySecret = "docs-vault-secret-771";

const decodeDocumentId = Schema.decodeSync(ResearchDocumentId);

interface CorpusEntry {
  readonly document: ResearchDocument;
  readonly bodyPhrase: string;
  readonly summary: string;
}

const corpusEntries = new Map<string, CorpusEntry>(
  [
    {
      documentId: "durability-notes",
      title: "Durability protocol notes",
      bodyPhrase: "amber-ledger-passage",
      summary:
        "Settlement results are recorded exactly once while external side effects stay at-least-once.",
    },
    {
      documentId: "subagent-notes",
      title: "Subagent join notes",
      bodyPhrase: "cobalt-join-corridor",
      summary: "A parent joins only the verified settlement of its own established child.",
    },
  ].map((entry) => [
    entry.documentId,
    {
      document: ResearchDocument.make({
        documentId: decodeDocumentId(entry.documentId),
        title: entry.title,
        body: `${entry.bodyPhrase}: internal working notes. ${docsDocumentBodySecret}. ${entry.summary} Raw notes stay inside the child Thread.`,
      }),
      bodyPhrase: entry.bodyPhrase,
      summary: entry.summary,
    },
  ]),
);

/** The corpus document ids in canonical fixture order. */
export const researchCorpusDocumentIds: ReadonlyArray<ResearchDocumentId> = [
  decodeDocumentId("durability-notes"),
  decodeDocumentId("subagent-notes"),
];

const requireCorpusEntry = (documentId: string): CorpusEntry => {
  const entry = corpusEntries.get(documentId);

  if (entry === undefined) {
    throw new Error(`No deterministic corpus entry exists for document ${documentId}`);
  }

  return entry;
};

/** Deterministic library lookup shared by the scripted MCP content handlers. */
export const researchDocumentLookup = (
  query: DocumentQuery,
): Effect.Effect<ResearchDocument, DocumentUnavailable> => {
  const entry = corpusEntries.get(query.documentId);

  return entry === undefined
    ? Effect.fail(
        DocumentUnavailable.make({
          documentId: query.documentId,
          message: "No deterministic corpus entry exists for this document.",
        }),
      )
    : Effect.succeed(entry.document);
};

/** The full fixture document (body includes the secret marker — child-side only). */
export const researchDocumentFor = (documentId: string): ResearchDocument =>
  requireCorpusEntry(documentId).document;

/** The distinctive body phrase used by context-isolation assertions. */
export const documentBodyPhrase = (documentId: string): string =>
  requireCorpusEntry(documentId).bodyPhrase;

// ---------------------------------------------------------------------------
// Doc Summarizer: the child Agent Definition. Its toolkit is the authored
// `DocContentToolkit`; the harness registers its worker Binding only after
// MCP discovery validates that exact toolkit (mcp.ts).
// ---------------------------------------------------------------------------

export class SummaryBrief extends Schema.Class<SummaryBrief>("SummaryBrief")({
  documentId: ResearchDocumentId,
  focus: Schema.NonEmptyString,
}) {}

export class DocumentSummary extends Schema.Class<DocumentSummary>("DocumentSummary")({
  documentId: ResearchDocumentId,
  summary: BoundedSummary,
}) {}

/** The summary the scripted child writes after fetching the document. */
export const documentSummaryFor = (documentId: string): DocumentSummary =>
  DocumentSummary.make({
    documentId: requireCorpusEntry(documentId).document.documentId,
    summary: requireCorpusEntry(documentId).summary,
  });

export const encodedDocumentSummary = (documentId: string): string =>
  JSON.stringify(Schema.encodeSync(DocumentSummary)(documentSummaryFor(documentId)));

export const DocSummarizer = Agent.make("doc-summarizer", {
  input: SummaryBrief,
  output: DocumentSummary,
  instructions:
    "Fetch the briefed document with fetch_document exactly once, then return only a JSON document summary. Never copy raw notes or secrets into the summary.",
  toolkit: DocContentToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  description: "Summarize one bounded research document fetched through MCP content tools.",
  metadata: { deploymentClass: "DN", phase: "P7" },
});

// ---------------------------------------------------------------------------
// Delegation Definition: the coordinator sees exactly one Tool with explicit
// projections and finite bounds. `projectResult` is the declassification
// boundary (SUB-015): only the bounded summary crosses; the fetched body —
// secret marker included — stays in the child Thread.
// ---------------------------------------------------------------------------

export class SummaryRequest extends Schema.Class<SummaryRequest>("SummaryRequest")({
  documentId: ResearchDocumentId,
}) {}

export class SummaryFinding extends Schema.Class<SummaryFinding>("SummaryFinding")({
  documentId: ResearchDocumentId,
  summary: BoundedSummary,
}) {}

export class DocumentSummaryFailed extends Schema.TaggedError<DocumentSummaryFailed>()(
  "DocumentSummaryFailed",
  {
    childErrorTag: Schema.NonEmptyString,
  },
) {}

/** Finite per-invocation bounds (SUB-009): one fetch per child, two children per Run. */
export const documentSummaryPolicy = SubagentPolicy.make({
  maxChildren: 2,
  maxConcurrency: 2,
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "10 seconds",
});

export const delegateDocumentSummary = Subagent.define("delegate_document_summary", {
  description:
    "Summarize one research document through the doc-summarizer child and return a bounded finding.",
  target: DocSummarizer,
  parameters: SummaryRequest,
  success: SummaryFinding,
  failure: DocumentSummaryFailed,
  prepareInput: (request) =>
    Effect.succeed(
      SummaryBrief.make({
        documentId: request.documentId,
        focus: "summarize:durability-claims",
      }),
    ),
  projectResult: (summary) =>
    Effect.succeed(
      SummaryFinding.make({
        documentId: summary.documentId,
        summary: summary.summary,
      }),
    ),
  policy: documentSummaryPolicy,
});

/** Total mapping from every expected child Run failure to the declared Tool failure (SUB-028). */
export const mapSummaryChildFailure = (failure: { readonly _tag: string }): DocumentSummaryFailed =>
  DocumentSummaryFailed.make({ childErrorTag: failure._tag });

/** The exact digest strings the durable declaration AND host registration must share (SUB-023). */
export const docsSummarizerDigestStrings = {
  agent: "50".repeat(32),
  model: "51".repeat(32),
  tools: "52".repeat(32),
} as const;

/** Runtime wiring: the immutable delegation plus one explicit child Binding (S2 declaration). */
export const docsSummaryHandlersLayer = <Provider, ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof SummaryBrief,
    typeof DocumentSummary,
    string,
    Toolkit.Tools<typeof DocContentToolkit>,
    Provider,
    ModelProvides,
    ModelRequires
  >,
) =>
  SubagentRuntime.layer(delegateDocumentSummary, childBinding, {
    mapChildFailure: mapSummaryChildFailure,
    durable: { targetDigests: docsSummarizerDigestStrings },
  });

// ---------------------------------------------------------------------------
// Docs Researcher: the parent Agent Definition.
// ---------------------------------------------------------------------------

export class ResearchRequest extends Schema.Class<ResearchRequest>("ResearchRequest")({
  question: Schema.NonEmptyString,
  documentIds: Schema.Array(ResearchDocumentId).check(Schema.isMinLength(1)),
}) {}

export class ResearchDigest extends Schema.Class<ResearchDigest>("ResearchDigest")({
  findings: Schema.Array(SummaryFinding),
  nextAction: Schema.Literal("review"),
}) {}

/** Parent-only transcript markers proving child context isolation (SUB-006/SUB-015). */
export const docsCoordinatorConfidentialMarker = "docs-coordinator-vault-19x";
export const docsMissionConfidentialMarker = "docs-mission-dossier-42f";

export const DocsResearcherToolkit = Toolkit.make(delegateDocumentSummary.tool);

export const DocsResearcher = Agent.make("docs-researcher", {
  input: ResearchRequest,
  output: ResearchDigest,
  instructions: [
    "You are the Effect Agent P7 docs-researcher coordinator.",
    `Coordinator-only context: ${docsCoordinatorConfidentialMarker}.`,
    "Call delegate_document_summary once per requested document in one Tool batch.",
    "Return only a JSON digest built from the delegated findings. This is read-only research.",
  ].join("\n"),
  toolkit: DocsResearcherToolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
  description: "Coordinate per-document summarization through one declared delegation Tool.",
  metadata: { deploymentClass: "DN", phase: "P7" },
});

/** The default two-document research mission. */
export const researchMissionRequest = ResearchRequest.make({
  question: `Summarize the durability and subagent notes; keep ${docsMissionConfidentialMarker} inside the coordinator thread.`,
  documentIds: researchCorpusDocumentIds,
});

/** The coordinator's expected final digest for the given documents. */
export const expectedResearchDigest = (
  documentIds: ReadonlyArray<string> = researchCorpusDocumentIds,
): ResearchDigest =>
  ResearchDigest.make({
    findings: documentIds.map((documentId) => {
      const summary = documentSummaryFor(documentId);

      return SummaryFinding.make({
        documentId: summary.documentId,
        summary: summary.summary,
      });
    }),
    nextAction: "review",
  });
