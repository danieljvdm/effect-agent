---
title: Context management
description: Limit model context, compact history, and track token usage.
---

# Context management

Each model request resends its context. Long runs can spend most of their budget rereading old
tool results or exceed the model's context window. Effect Agent bounds tool output, reports the
remaining budget, and compacts old context. Final-answer policy can grant one constrained turn
after turn, tool call, or token exhaustion.

Set these limits from the model and workload. The provider does not supply them:

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,

  tokenBudget: 200_000,
  completionReserveTokens: 32_000,
  costBudgetMicrousd: 2_000_000,
  contextTokenLimit: 150_000,

  toolResultBounds: ToolResultBounds.make({ maxBytes: 50 * 1024 }),
  runStatus: "appended",
  compaction: CompactionPolicy.make({ keepRecentTokens: 20_000 }),
  onExhaustion: "final-answer",
});
```

`tokenBudget` counts cumulative input and output across the run. `costBudgetMicrousd` uses the
installed cost estimator and cache-split usage. `contextTokenLimit` bounds the live context for one
call. Set it below the model window so output and compaction have room.

## Prompt preparation order

At run start, the runtime evaluates instructions and the definition's optional
[`inputPrompt`](/guide/agents#choose-model-visible-input). Without `inputPrompt`, the model receives
the full encoded input as a JSON user message.

Before each turn, `RunContextPreparation.hook.prepare` transforms the source prompt. The engine
compacts the prepared history, then loads optional references through
`RunContextPreparation.transientContext.load`. If the references exceed the remaining budget,
the engine can compact canonical history further while keeping the same reference snapshot.
It appends the references and derived run status to the compacted view and adds the output
contract. Compaction summaries never receive transient references. Durable
recovery rebuilds the committed model view before applying prompt preparation; a transient loader
receives the current Attempt's official source, Thread ID, Run ID, Turn ID, and Turn number.

To add application instructions to each request:

```ts twoslash
import { RunContextPreparation, type RunContextHook } from "@effect-agent/engine";
import { Effect, Layer } from "effect";
import { Prompt } from "effect/unstable/ai";

export const metricContext: RunContextHook = {
  prepare: ({ source }) =>
    Effect.succeed({
      prompt: Prompt.concat(
        Prompt.make([{ role: "system", content: "Use metric units in your answer." }]),
        source,
      ),
    }),
};

export const MetricContextLive = Layer.succeed(RunContextPreparation, { hook: metricContext });
```

Provide `MetricContextLive` to `AgentRuntime.run` or `start` with `Effect.provide`, or to
`AgentRuntime.stream` with `Stream.provide`.
With `start`, provide the Layer around the whole scoped workflow, including awaiting the handle,
so its resources remain available until the detached Run finishes.
For durable execution, install `MetricContextLive` when [configuring the runtime](./run-agents#assemble-a-custom-durable-runtime).
Transforms change the model prompt, not stored input or history. Use
[`inputPrompt`](./agents#choose-model-visible-input) to choose which input fields the model sees.

## Recall application-owned sources {#recall-memory}

`recallMemory` turns readable, application-selected sources into a bounded transient model view.
The framework does not own a memory database, write recalled material, or build an embedding
index. An Agent that does not need context loading requires no context service, memory reader,
or store. `RunContextPreparationPassthrough` remains available to explicitly disable inherited
context preparation.

Each reader returns `MemoryLookup`. `Found` carries ranked `MemoryPassage` values. `NoMatch` is a
successful empty result. `Unavailable` and `InsufficientFreshness` remain distinct in the returned
source outcomes. An unavailable or stale `essential: true` source fails recall; an essential source
whose matching passages cannot fit also fails. `NoMatch` remains successful even for an essential
source. Expected reader failures propagate unless the reader deliberately maps them to one of the
lookup outcomes.

A passage points back to its authoritative source ID, locator, and known revision. Its attribution
records the speaker, observers, original activity time, and the application's interpretation.
`recordedAt` says when the application recorded the content, while `extractedAt` says when it made
this passage. Neither substitutes for `activityAt`; use `null` when the original activity time is
unknown.

### Recall a Markdown passage directly

This source reads a known Markdown note without a store or adapter:

```ts twoslash
import { recallMemory } from "@effect-agent/capabilities";
import {
  MemoryAttribution,
  MemoryContent,
  type MemoryLookup,
  MemoryPassage,
  MemoryRecallError,
  MemoryRecallLimits,
  MemorySourceReference,
} from "@effect-agent/core";
import { RunContextPreparation, type RunTransientContextHook } from "@effect-agent/engine";
import { Effect, Layer } from "effect";
import { Prompt } from "effect/unstable/ai";

const limits = MemoryRecallLimits.make({
  maxSources: 1,
  maxItems: 4,
  maxBytes: 16_384,
  maxTokens: 4_096,
  timeoutMillis: 1_000,
});

const note = MemoryPassage.make({
  version: 1,
  source: MemorySourceReference.make({
    id: "project-notes",
    locator: "file:///workspace/notes/queue.md",
    revision: "sha256:8d31",
  }),
  passageId: "retry-policy",
  content: MemoryContent.make({
    text: "# Retry policy\nUse a bounded queue and preserve failed work.",
    attributions: [
      MemoryAttribution.make({
        originId: "meeting:2026-08-28:queue",
        speaker: "Dan",
        observers: ["Chad"],
        locator: "meeting://engineering/2026-08-28#queue",
        activityAt: 1_777_334_400_000,
        interpretation: "proposal awaiting review",
      }),
    ],
    metadata: { format: "markdown" },
    recordedAt: 1_777_420_800_000,
    extractedAt: 1_777_420_801_000,
  }),
});

const lookup: MemoryLookup = { _tag: "Found", passages: [note] };

export const projectNotes: RunTransientContextHook<MemoryRecallError> = {
  load: () =>
    recallMemory(
      [{ id: "project-notes", essential: true, read: Effect.succeed(lookup) }],
      limits,
    ).pipe(
      Effect.map((recalled) =>
        recalled.text === ""
          ? Prompt.empty
          : Prompt.make([{ role: "user", content: recalled.text }]),
      ),
    ),
};

export const ProjectContextLive = Layer.succeed(RunContextPreparation, {
  transientContext: projectNotes,
});
```

Provide `ProjectContextLive` to the run. With an existing agent and input:

```ts
const runnable = AgentRuntime.run(agent, input).pipe(
  Effect.provide(ProjectContextLive),
  Effect.catchTags({
    MemoryRecallError: handleRecallFailure,
    CompactionError: handleCompactionFailure,
  }),
);
```

The service declares `AgentInputError | MemoryRecallError | CompactionError`; method failures
retain their original tags and fields. `RunContextPreparationError` names that type union, not a
wrapper class. `Effect.provide` adds any errors from acquiring the Layer separately. Adapters
handle backend-specific failures or translate them into this contract. Providing a Layer does not
add undeclared service-method errors to a program's error type.

For an application-specific error or requirement channel, the existing generic `RunOptions.context`
and `RunOptions.transientContext` hooks override the corresponding service fields for that Run.
Other service fields remain active. Attached children inherit the host context service, not the
parent's per-Run overrides.

`recallMemory` renders positional `memory:N` reference IDs and provenance for that one result.
`RecalledMemory.outcomes` separately reports what happened at each source. The rendered envelope
and every citation remain untrusted model input. Validate model claims against
`RecalledMemory.passages` before presenting them as sourced facts.

Source IDs are local to an authority. A host can set `MemoryPassage.authority` to share that
authority across readers; passages without it are scoped to their reader declaration's `id`.
Deduplication, conflict detection, and the selected-source limit use authority-qualified IDs.
Two independent authorities can therefore both contain `profile` at revision `1` without being
merged or rejected. Direct readers must explicitly share an authority to deduplicate across
reader declarations. Authority is an identity boundary, not an authorization grant.

The rendered text substitutes positional `memory-authority:N` labels for private authority
values. These labels are local to the result and qualify its evidence origins; different labels
alone do not prove independent corroboration. `RecalledMemory.passages` retains explicit authority
for host-side composition and validation. Use `RecalledMemory.text` for model input, not a raw
serialization of those host passages.

`MemoryRecallLimits.maxInputBytes` separately bounds the aggregate UTF-8 JSON passage encodings
considered by one call, including omitted and duplicate candidates. It defaults to 16 MiB and can
be set up to 64 MiB. Exceeding it returns a typed `budget` error before retaining that candidate's
identity or encoding, even when the source is optional. Reader allocation, result decoding, and
one candidate's serialization occur before this check; it is not a whole-process heap limit.
Input-budget exhaustion stops validation and returns no partial context. Within admitted input,
conflicting known-revision identities fail even when an earlier passage exceeds the output budget.
Identity and conflict checks ignore JSON object member order, including nested metadata, while
preserving array order. Equivalent unknown-revision passages within one authority share one citation.

### Read an external corpus through an Effect service

Keep authorization, credentials, and query policy inside an application service. This contract has
one read method; it does not create a durable copy, write to the corpus, or create embeddings:

```ts twoslash
import { recallMemory } from "@effect-agent/capabilities";
import { type MemoryLookup, MemoryRecallError, MemoryRecallLimits } from "@effect-agent/core";
import { RunContextPreparation, type RunTransientContextHook } from "@effect-agent/engine";
import { Context, Effect, Layer } from "effect";
import { Prompt } from "effect/unstable/ai";

class ExternalCorpus extends Context.Service<
  ExternalCorpus,
  {
    readonly search: (query: string) => Effect.Effect<MemoryLookup, MemoryRecallError>;
  }
>()("app/ExternalCorpus") {}

const limits = MemoryRecallLimits.make({
  maxSources: 8,
  maxItems: 8,
  maxBytes: 32_768,
  maxTokens: 8_192,
  timeoutMillis: 2_000,
});

export const ExternalCorpusMemoryLive = Layer.effect(
  RunContextPreparation,
  Effect.gen(function* () {
    const corpus = yield* ExternalCorpus;
    const transientContext: RunTransientContextHook<MemoryRecallError> = {
      load: () =>
        recallMemory(
          [
            {
              id: "team-corpus",
              essential: false,
              read: corpus.search("current queue design"),
            },
          ],
          limits,
        ).pipe(
          Effect.map((recalled) =>
            recalled.text === ""
              ? Prompt.empty
              : Prompt.make([{ role: "user", content: recalled.text }]),
          ),
        ),
    };
    return RunContextPreparation.of({ transientContext });
  }),
);
```

Provide the application's `ExternalCorpus` Layer to `ExternalCorpusMemoryLive`, then provide that
closed Layer to an ephemeral Run or to `DurableAgentRuntime.layerWithServices` with
`RunToolAuthorization`. The durable runtime captures `RunContextPreparation` in its Scope.
Put `hook`, `transientContext`, and
`compactor` in the same service value when using all three.

The engine reloads transient context in every normal or grace Turn and after durable recovery,
after canonical context preparation and initial compaction succeed. Failure in that initial
phase does not read transient sources. Post-load admission can compact canonical history further
to make room for the references. This pass and a same-Turn provider-overflow retry reuse the
loaded snapshot. Each provider call still
has to fit `contextTokenLimit`; transient text participates in output-contract, run-status, token
budget, and completion-reserve admission. Oversized context fails before provider I/O.

Transient references never enter Thread history, canonical records, compaction coverage, or a
compaction summary request. Recovery reads the source again instead of replaying an earlier
snapshot. `RunContextRequest.source` is the current Attempt's official pre-preparation history;
use its stable identities or an application-owned query when retrieval requires canonical durable
state.

### Correct or withdraw a remembered source {#memory-lifecycle}

Define namespaces with an application-owned identity Schema. Constructor inputs retain brands;
keys, writes, documents, access values, and index results retain the definition's name, version,
and identity type. Definitions with the same identity fields but different names or versions are
not interchangeable.

```ts twoslash
import { MemoryKey, MemoryNamespace } from "@effect-agent/core";
import { MemoryAccess } from "@effect-agent/capabilities";
import { Schema } from "effect";

const TenantId = Schema.NonEmptyString.pipe(Schema.brand("app/TenantId"));
const UserId = Schema.NonEmptyString.pipe(Schema.brand("app/UserId"));
const UserConversations = MemoryNamespace.define({
  name: "app/user-conversations",
  version: 1,
  identity: Schema.Struct({ tenantId: TenantId, userId: UserId }),
});

declare const session: {
  readonly tenantId: typeof TenantId.Type;
  readonly userId: typeof UserId.Type;
};
const conversations = UserConversations.make(session);
const key = MemoryKey.make({ namespace: conversations, id: "conversation-42" });
const access = MemoryAccess.make({ namespace: conversations, scope: "private" });
```

`make` takes decoded identity values and throws on invalid construction. `decode(unknown)`
decodes external identity input as an Effect with `MemoryNamespaceError` and no service
requirements. `restore(address)` validates a stored address against the specific definition,
including its identity codec, name, and version. It rejects noncanonical addresses and reports
wrong definitions or unsupported address formats separately. Validation does not authenticate
a principal. Two tenants still share the same `TenantId` type. The host must establish session
identity and authorization before constructing namespaces or access values. `"private"` is only
an application-defined scope name, not a built-in privacy policy.

#### Namespace encoding and adapter boundaries

Every adapter uses `namespace.address`, a branded, Schema-validated string. Its format is compact
JSON `[1, definitionName, definitionVersion, encodedIdentity]`. Object keys sort recursively in
UTF-16 code-unit order, including numeric-looking keys. Array order is preserved. Strings use
JSON escaping without Unicode normalization. JSON number serialization normalizes negative zero
to zero. Separator characters cannot join distinct identity fields into the same address.

Identity codecs must be deterministic, synchronous, service-free, and encode to JSON. Branded
Structs, records, arrays, and codecs such as `Schema.DateFromString` are supported. Encoding then
decoding normalizes constructor values through the identity codec. Another round trip must leave
the encoded identity unchanged. Schema-defined normalization, such as ignored excess Struct
fields, deliberately selects the same address. Non-JSON values such as undefined, non-finite
numbers, and bigint must be converted by the codec or are rejected.

Names contain 1–256 UTF-16 code units; definition versions are positive safe integers. The full
address is at most 4,096 UTF-8 bytes. Encoded identities allow at most 16 nested container levels
and 128 entries per container. These limits apply before storage or indexing. Changing a
definition version selects a distinct namespace, even for the same identity. It never interprets
old memory under a new Schema. Document revisions and SQLite's storage-format version are separate.

`MemoryKey.Wire`, `MemoryDocument.Wire`, `MemoryWrite.Wire`, and the access/index `.Wire` Schemas
are explicitly heterogeneous transport representations. Their namespaces contain only the
canonical address, not a recovered application identity type. Adapter authors use
`MemoryReader.fromAdapter`, `MemoryWriter.fromAdapter`, and `SemanticMemoryIndex.fromAdapter`
to validate results and restore the caller's namespace type. For persisted documents outside a
port, restore a namespace through its definition, then call `MemoryDocument.restore(namespace, input)`.
Never assert a wire value into an application namespace type. Generic types without an explicit
namespace parameter describe heterogeneous values; use `MemoryKey<typeof conversations>` and
the equivalent document/write/access/index types for family-specific application APIs.

Namespace identities and addresses can contain sensitive identifiers. They are not retrieval
parameters for the model and are not automatically added to recall text, logs, or telemetry.
The framework does not supply a registry, tenant membership checks, wildcard search, or a fixed
memory taxonomy.

This changes SQLite memory storage to format 2. Format-1 memory data and old string-namespace
prepared activity outputs are incompatible and fail decoding. Reset affected development memory
and processor data before reusing it. There is no migration or raw-string fallback. Existing
Thread history is a separate retention concern.

`MemoryReader` and `MemoryWriter` are separate optional capabilities. Use a reader to validate
search or cache candidates against the current source. A writer is appropriate only when its
adapter can atomically check an expected revision and retain idempotency receipts. A read-only
corpus needs no writer. An external memory service can implement these ports without copying its
corpus into a framework store.

The host selects a namespace and access scope. A document explicitly lists the scopes allowed to
recall it; an empty list grants none. No scope name, shared persona, channel, or DM is enabled by
default. Call `revalidateMemoryLookup(candidates, access, limits)` inside the reader supplied to
`recallMemory`, immediately before composition. It requires only `MemoryReader`. The optional
third argument accepts `maxInputBytes` from the recall limits, defaulting to 16 MiB and capped
at 64 MiB. Revalidation reads one authoritative source at a time and checks aggregate UTF-8
replacement JSON before retaining it, including duplicate passages. Exceeding this bound fails
with `MemoryRecallError` reason `budget` before reading later source groups. A single reader
result and its schema decoding precede the aggregate retention bound.

Validation binds each returned passage's private authority to the host-selected namespace,
replacing any candidate-supplied authority. Combining independently authorized namespaces
therefore preserves their distinct source identities without exposing namespace values in model text.
Validation reloads each candidate's source, excludes missing, withdrawn, or access-revoked
documents, and replaces stale text with the current document. Even a same-revision excerpt gets
its attribution and metadata from the current source. It survives only if its text occurs there.
The usual recall budget can omit a replacement that no longer fits. Source failures stay typed;
the consumer must explicitly choose any optional fallback.

```ts twoslash
import { MemoryContent, MemoryKey, MemoryNamespace, MemoryWriter } from "@effect-agent/core";
import { Effect, Schema } from "effect";

const TeamMemory = MemoryNamespace.define({
  name: "app/team-memory",
  version: 1,
  identity: Schema.String,
});
const key = MemoryKey.make({ namespace: TeamMemory.make("team-a"), id: "queue-discussion" });

export const correctDiscussion = Effect.fn("correctDiscussion")(function* (
  content: MemoryContent,
  expectedRevision: string,
  operationId: string,
) {
  const writer = yield* MemoryWriter;
  return yield* writer.change({
    _tag: "Put",
    key,
    operationId,
    expectedRevision,
    locator: "chat://engineering/42",
    content,
    scopes: ["participating-channels"],
  });
});

export const withdrawDiscussion = Effect.fn("withdrawDiscussion")(function* (
  expectedRevision: string,
  operationId: string,
) {
  const writer = yield* MemoryWriter;
  return yield* writer.change({
    _tag: "Withdraw",
    key,
    operationId,
    expectedRevision,
    reason: "Withdrawn by the source owner",
  });
});
```

Create a source with `Put` and `expectedRevision: null`. Later writes use the current revision;
a competing edit returns `MemoryConflict` without discarding the winning edit. Every replacement
records its predecessor reference. `modifiedAt` tracks the update, while the caller's original
activity, recording, and extraction times remain separate. Applications decide how to correct
attribution, resolve conflicting claims, and age discussions or commitments. Timestamps alone
never choose a winning claim.

Retry an uncertain write with its original `operationId` and exactly the same Schema-encoded
command. A successful replay returns the original receipt's document and does not undo later
edits or withdrawal. Reusing an operation ID for different content returns
`MemoryOperationConflict`. Revalidate the returned document before recall because a receipt can
describe an older revision.

Withdrawal is terminal for that source ID within its namespace. A committed withdrawal excludes the
source from validation checks begun afterward. The same rule applies after access revocation.
An already captured view, including a same-Turn provider retry, may finish. This guarantee needs
an authoritative reader; an eventually consistent service must refuse a view it cannot validate.
Do not run the SQLite reader inside a caller-owned stale snapshot transaction.

Withdrawal governs future recall. Original Thread records, past model outputs, idempotency
receipts, and backups have separate retention policies. Sensitive-information screening is best
effort and does not authorize sharing or guarantee privacy.

For a local persistent source, install the optional SQLite adapter:

```ts twoslash
import { memoryStoreLayer } from "@effect-agent/storage-sqlite";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer } from "effect";

export const MemoryLive = memoryStoreLayer.pipe(
  Layer.provide(SqliteClient.layer({ filename: "memory.sqlite" })),
);
```

`memoryStoreLayer` provides both ports and creates only its own memory tables. It does not
initialize Thread history or a Submission Ledger. `memoryReaderLayer` checks an existing schema
without creating tables or starting a write transaction, and supports `SqliteClient.layer({
filename, readonly: true })`. It fails with `MemoryStorageError` when the schema is absent or
incompatible; initialize it through `memoryStoreLayer` in the writer process first.
The connection belongs to its Layer's Scope. The adapter rejects a change before writing when
its canonical command, document, or receipt-result JSON exceeds 16,777,216 JavaScript string code
units. `memoryStoreLayerWithFailpoints` accepts a `MemoryMutationFailpoint` service for transaction
and lost-acknowledgement tests. Replayed receipts must match their original command's predecessor,
result kind, content, locator, scopes, and withdrawal reason; mismatches fail as corrupt data.
Recall and validation helpers add named Effect spans without source text or metadata annotations.
`RecalledMemory.outcomes` reports source availability and selected, deduplicated, and omitted counts;
the host decides which diagnostics to retain and who can inspect them.

<a id="tool-results-are-bounded-at-the-source"></a>

### Process committed Thread activity {#committed-memory}

`processCommittedActivity` is an optional, finite pass over one application-selected Thread.
The host chooses the processor ID and version, eligible records, extractor, destination, sharing
scope, invocation schedule, and Thread discovery. No background worker starts when the package
is imported. A read-only corpus or direct Markdown recall needs none of these services.

The pass claims its own processor lease and receives any pending output atomically with that
claim. It captures `ThreadStore.inspectTail` once and reads
bounded, contiguous pages through that prefix. Records appended afterward belong to a later
pass. Pending work beyond the captured tail fails as `noncontiguous`, including when progress and
Thread storage were restored to different points. `PersistentHistory` exposes the batch from a
successful Run together; durable Runs expose
incremental committed records. A record's presence is evidence of its commit, not evidence that
the whole Run succeeded. Eligibility rules must account for that distinction.

For each record the processor saves a Schema-encoded extraction, applies that saved output, and
then advances its separate cursor. The work ID depends on processor, version, Thread, and
sequence, never the worker or clock. Advancing clears pending output in the same transaction;
there is no separate pending-work read per record. A saved output wins over re-extraction after restart. Its
canonical record digest excludes the adapter's opaque observation cursor. Another processor
version has independent progress; changing a version is an application decision about
reprocessing and destination identities.

The destination must reconcile the work ID durably before returning. If application and progress
storage are separate, application can repeat after a lost acknowledgment. Keep its receipts for
as long as pending output can return, including supported backup/restore windows. Conditional
writes must prevent delayed work from overwriting later corrections or withdrawals. The SQLite
memory writer supplies those properties; an ordinary non-idempotent external effect does not.

This example opts a structured Dan–Chad discussion into a scope the host also grants Tim. Its
extraction policy accepts only original user statements. An assistant repeating the statement
does not become another witness. Applications that extract assistant references should retain
the original `originId` instead of assigning independent evidence identity.

```ts twoslash
import {
  MemoryContent,
  MemoryKey,
  MemoryNamespace,
  MemoryWrite,
  MemoryWriter,
  ThreadId,
} from "@effect-agent/core";
import {
  ActivityPassLimits,
  ActivityProcessorKey,
  processCommittedActivity,
  type CanonicalRecordEnvelope,
  type PreparedActivity,
} from "@effect-agent/thread";
import { Clock, DateTime, Effect, Schema } from "effect";

// The application owns this message format and which Threads use it.
const Statement = Schema.Struct({
  speaker: Schema.NonEmptyString,
  observer: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
  activityAt: Schema.NullOr(Schema.Finite),
  interpretation: Schema.NonEmptyString,
});

const extract = Effect.fn("extractDiscussion")(function* (entry: CanonicalRecordEnvelope) {
  if (entry.record.payload._tag !== "UserInputRecorded") return null;
  const statement = yield* Schema.decodeUnknownEffect(Statement)(entry.record.payload.input);
  const locator = `thread://${entry.threadId}/records/${entry.record.recordId}`;
  const content = yield* MemoryContent.makeEffect({
    text: statement.text,
    attributions: [
      {
        originId: `${entry.threadId}:${entry.record.recordId}`,
        speaker: statement.speaker,
        observers: [statement.observer],
        locator,
        activityAt: statement.activityAt,
        interpretation: statement.interpretation,
      },
    ],
    metadata: {
      threadId: entry.threadId,
      recordId: entry.record.recordId,
      recordSchemaVersion: entry.record.schemaVersion,
      sequence: entry.sequence,
    },
    recordedAt: DateTime.toEpochMillis(entry.record.createdAt),
    extractedAt: yield* Clock.currentTimeMillis,
  });
  return yield* Schema.encodeEffect(MemoryContent)(content);
});

const apply = Effect.fn("applyDiscussion")(function* (work: PreparedActivity) {
  const content = yield* Schema.decodeUnknownEffect(Schema.NullOr(MemoryContent))(work.output);
  if (content === null) return;
  const writer = yield* MemoryWriter;
  const namespace = MemoryNamespace.define({
    name: "app/discussions",
    version: 1,
    identity: Schema.String,
  }).make("dan");
  const command = MemoryWrite.make({
    _tag: "Put",
    key: MemoryKey.make({ namespace, id: work.workId }),
    operationId: work.workId,
    expectedRevision: null,
    locator: `memory://dan-discussions/${work.workId}`,
    content: {
      ...content,
      metadata: { ...content.metadata, sourceRecordDigest: work.recordDigest },
    },
    scopes: ["dan-approved-chad-and-tim"],
  });
  yield* writer.change(command);
});

const key = ActivityProcessorKey.make({
  processorId: "discussion-statements",
  processorVersion: "1",
  threadId: Schema.decodeSync(ThreadId)("dan-chad"),
});
const limits = ActivityPassLimits.make({
  maxRecords: 128,
  pageSize: 16,
  timeoutMillis: 30_000,
  leaseMillis: 31_000,
});

// Supply a unique owner for each worker lifetime and the application's Layers.
const ingest = (owner: string) => processCommittedActivity({ key, owner, limits, extract, apply });
```

The optional SQLite progress Layer can share a connection with the memory writer. Supply the
host's existing `ThreadStore` and `Crypto` Layer to the pass as well; the processor never uses
Thread ownership epochs, `SubmissionLedger`, or engine checkpoints for its own progress.

```ts twoslash
import { activityProcessorStoreLayer, memoryStoreLayer } from "@effect-agent/storage-sqlite";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer } from "effect";

const MemoryProcessing = Layer.mergeAll(activityProcessorStoreLayer, memoryStoreLayer).pipe(
  Layer.provide(SqliteClient.layer({ filename: "memory.sqlite" })),
);
```

Every claim acquisition allocates a fresh fencing epoch, including reacquisition by the same
owner after release. Expired or superseded workers cannot replace pending output or advance
progress. A destination invocation already in flight may finish the saved output, so its own
idempotency and conditional writes remain required. Extraction and application each own a Scope;
the pass has a deadline and release gets at most another 500ms. If release fails, the lease
expires. The failpoint-enabled Layer exposes initialization and each mutation before, inside,
and after its transaction for recovery tests.
SQLite rejects activity progress whose JSON exceeds 16,777,216 JavaScript string code units before
writing, with `ActivityStoreError` reason `invalid-input`. Rejected writes leave prior progress intact.

`ActivityProcessorStore.inspect` exposes the per-processor, per-version, per-Thread cursor,
pending work, and last advancement time. A successful pass reports its captured tail, through
sequence, and remaining records in that prefix. These are per-Thread watermarks, not a global
freshness promise. Process selected Threads with bounded `Effect.forEach` and handle each pass's
typed result independently when one failed Thread should not hold back others.

For the example workflow, the healthy commit-to-recallable target is 60 seconds. Hosts must
measure that interval from the source commit to successful authoritative recall and choose a
schedule that meets it. Recorded, extracted, advanced, indexed, and accessed times describe
different events; none replaces the original activity time. An embedding index has its own
progress and readiness, and must not advance this extraction cursor.

### Add optional semantic retrieval {#semantic-memory}

`indexMemorySource` and `querySemanticMemory` use the pinned upstream Effect AI
`EmbeddingModel`. Supply its provider Layer directly. Direct loading, keyword retrieval, external
attributed passages, and the cross-Thread workflow above need no embedding model or vector index.
The framework does not define another provider abstraction or impose a ranking or aging policy.

The host binds `SemanticMemoryProfile` to the actual provider, model revision, dimensions, and
chunking configuration. Rebuild when any of those change. Matching dimensions alone does not
make two models compatible. Provider configuration overrides must not silently change the model
behind a live index. Treat different preprocessing or precision settings as a new profile identity.
Choose chunk sizes within the selected provider's input limits. Read-only external sources can
implement `MemoryReader` without a writer; sources with unknown revisions should use direct
passage retrieval instead of this index.

```ts twoslash
import {
  MemoryAccess,
  SemanticIndexLimits,
  SemanticQueryLimits,
  indexMemorySource,
  querySemanticMemory,
  recallMemory,
} from "@effect-agent/capabilities";
import {
  MemoryKey,
  MemoryNamespace,
  MemoryRecallLimits,
  SemanticMemoryProfile,
} from "@effect-agent/core";
import { inMemorySemanticIndexLayer } from "@effect-agent/storage-memory";
import { Effect, Schema } from "effect";

// Keep this Layer alive across refreshes and queries. A new instance starts empty.
export const makeIndex = (profile: SemanticMemoryProfile) =>
  inMemorySemanticIndexLayer(profile, { maxSources: 1_024, maxChunks: 8_192 });

const TeamMemory = MemoryNamespace.define({
  name: "app/team-memory",
  version: 1,
  identity: Schema.String,
});
const namespace = TeamMemory.make("team-a");

export const refresh = (key: MemoryKey<typeof namespace>) =>
  indexMemorySource(
    key,
    SemanticIndexLimits.make({
      maxSourceBytes: 262_144,
      maxChunks: 128,
      timeoutMillis: 30_000,
    }),
  );

export const recall = (query: string) =>
  recallMemory(
    [
      {
        id: "team-semantic",
        essential: false,
        read: querySemanticMemory(
          query,
          MemoryAccess.make({
            namespace,
            scope: "participating-channels",
          }),
          SemanticQueryLimits.make({
            maxQueryBytes: 8_192,
            maxCandidates: 16,
            maxScannedChunks: 8_192,
            minScore: 0.35,
            timeoutMillis: 1_000,
          }),
        ).pipe(Effect.map((result) => result.lookup)),
      },
    ],
    MemoryRecallLimits.make({
      maxSources: 8,
      maxItems: 8,
      maxBytes: 16_384,
      maxTokens: 4_096,
      timeoutMillis: 1_000,
    }),
  );
```

Provide the same index instance, `MemoryReader`, and native `EmbeddingModel` to both operations.
Refresh also requires Effect `Crypto`. The provider Layer owns its resources; the index belongs
to its Layer's Scope. Captured index methods fail after that Scope closes. Put `recall` in the
transient-context hook above. The final envelope, including attribution and citations, must fit
`recallMemory`'s item, UTF-8 byte, and token limits; the engine separately admits the full prompt.
`essential: false` permits explicitly returned unavailable outcomes. It does not swallow errors.
Map only intended expected failures to an `Unavailable` lookup in application policy.

Chunking greedily packs complete Unicode codepoints up to `maxChunkBytes`. It neither summarizes
nor silently drops a source suffix. Chunk IDs include a digest of the whole profile and the
ordinal; candidates also carry source identity, revision, generation, and byte offsets. Indexing
checks source-byte and chunk-count limits before calling the provider. Returned vectors must
match the profile and have a positive finite norm. This simple chunker is a deterministic
baseline; it makes no sentence-boundary or relevance promise.

`inMemorySemanticIndexLayer` is a replaceable exact cosine adapter. It bounds all registered source
keys, including withdrawal tombstones, and all ready chunks. A search over its scan limit fails
instead of ranking an arbitrary prefix. Scores tie by source ID, revision, and chunk ordinal.
Configuration rejects `maxChunks * profile.dimensions` above 16,777,216 vector components before
allocating index state. For a 4,096-dimensional profile, set `maxChunks` to at most 4,096. This bounds
stored vectors, not total process memory or allocations made by callers.
`maxSourceBytes` caps the aggregate UTF-8 JSON of retained source identities, including terminal
tombstones. It defaults to 16 MiB and accepts at most 64 MiB. Replacement and withdrawal check
this bound atomically before changing the index. A rejected change leaves the prior source and
chunks intact; a smaller replacement releases identity capacity. Replaying a withdrawal does not
charge the same tombstone twice. Map keys, object overhead, and vector storage are not included
in this source-encoding budget.
The adapter holds no authoritative documents or attribution.

Refresh prepares chunks and embeddings before calling `SemanticMemoryIndex.replace`. Replacement
validates the profile and source revision, then exchanges all chunks atomically. Failed or cancelled
refreshes leave the last successful index intact. Older generations and divergent same-generation
identities are fenced. Withdrawal is terminal within the instance and blocks delayed replacements.
The index exposes no build epochs, publication states, inspection, or mutation failpoints.
Closing and recreating the Layer discards all chunks and tombstones;
rebuild from current authoritative sources before requiring complete semantic recall.

The index and source are independent. Refresh reads the source again before publication, but a
change can still occur between that read and the index write. Every query therefore rereads each
candidate source, checks namespace, access, revision, generation, locator, and exact excerpt, and
takes attribution and metadata from that source. Stale candidates are omitted; their old score is
never assigned to corrected text. A source correction may temporarily reduce recall until refresh
finishes. Missing, withdrawn, or revoked sources cannot pass checks begun after the authoritative
change. Already captured views may finish, as described under withdrawal. Returned passages bind
their private authority to the query's authorized namespace, so downstream recall can combine
independent namespaces without confusing their source IDs. Recall renders only opaque authority labels.

Queries group candidates by source, read each source once, and restore the original index ranking
after validation. Full source documents and their excerpt-check encodings are local to one group.
`SemanticQueryLimits.maxSourceBytes` separately bounds the aggregate UTF-8 JSON of distinct
authorized sources with generation, revision, and locator matches. It defaults to 16 MiB and can
be set up to 64 MiB. Missing, withdrawn, unauthorized, and identity-stale sources are excluded
without consuming this budget. Exceeding it returns a typed `budget` error with no partial result.
Reader allocation, decoding, and one source's serialization precede this check; it is not a
whole-process heap limit. The indexing limit with the same name bounds one source's text instead.

`SemanticQueryResult` reports scanned chunks, excluded stale and unauthorized candidates, query
embedding usage when the provider supplies it. It makes no completeness claim;
the host owns discovery, refresh scheduling, and required freshness.
An empty index or no-match result does not prove the corpus has no relevant memory. No source text,
query text, attribution, or vectors are attached to the helpers' Effect spans.

Deadlines interrupt cooperative work and run finalizers. A provider that cannot cancel native I/O
may need to drain its active call before finalizing; account for that in the host's latency policy.
The reproducible `examples/semantic-memory-eval` consumer compares direct, lexical, and real local
embedding recall on a frozen synthetic corpus. It separates warm query latency, cached-file cold
model startup, source-commit-to-recallable lag, background extraction/indexing, and injected slow or
failed requests. Its declared targets are 250 ms warm added recall, 3 seconds with a cold model
instance and cached files, and 60 seconds from a healthy source commit to recall. These are example
targets, not provider or production guarantees. The example reports misses and contradictory
retrievals; applications must choose their own decision, commitment, and aging policies.

## Limit tool output

Every application tool result, including MCP output, passes through `toolResultBounds` once before
history or durable storage. Results within the limit keep their encoded bytes. Larger results use
one canonical envelope:

```json
{
  "truncatedToolResult": true,
  "originalBytes": 412887,
  "head": "...first half of the byte budget...",
  "tail": "...last half..."
}
```

The model and journal see the same envelope. Replay therefore stays consistent. The default limit
is 50 KiB. Provider-executed tool results are exempt because the provider has already put them in
the response.

## The run-status message

With `runStatus: "appended"`, each outgoing request ends with a derived status line:

```text
<run-status>turn 3/12 · tool-calls 11/24 · tokens 84210/200000 · research-remaining 83790 · completion-reserve 32000 · last-context 23480 · elapsed 74s/300s</run-status>
```

At 80 percent of a limit, the line asks the model to wrap up. The token warning uses the research
balance after reserving completion capacity. The runtime also warns when that balance cannot cover
another input as large as the last call.

The status line is built for each request and never enters canonical history. Set `runStatus: "off"`
for prompt-sensitive evaluations. Providers that cache at the last user message may need an
explicit cache boundary before this changing suffix. The host owns provider cache fields.

<a id="warnings-and-the-token-soft-landing"></a>

## Budget warnings and finalization

Crossing 80 percent emits one `BudgetWarning` event for that dimension. Turn, tool call, and token
exhaustion follow `onExhaustion`.

With `"final-answer"`, an over-budget tool batch runs no handlers. The next request forbids tool
use, except for the definition's singleton completion tool. Turn exhaustion allows one grace turn.
Token exhaustion completes from the breaching response when it already contains decodable output;
otherwise it allows the same single constrained turn.

The result and `RunCompleted` event report `finishReason: "budget-exhausted"`.
Their `exhausted` field names the limit: `"tokens"`, `"turns"`, or `"tool-calls"`.

Delegated child results carry the same marker through `SubagentCompleted.exhausted` and
`projectResult`. `onExhaustion: "fail"` rejects work after the breach. Duration and cost breaches
always fail because another model call would add time or cost.

## Compaction

With `contextTokenLimit`, the engine estimates the next prompt before every turn. It starts from
the last provider-reported input and estimates appended content. The default compactor then:

1. clears old application tool results outside the protected `keepRecentTokens` tail while keeping
   message structure and call/result pairs;
2. if pruning is insufficient, makes one metered summary call and keeps the instruction prefix,
   summary, and recent tail.

Compaction changes the model view. It never rewrites the thread log. `CompactionPerformed`
reports each reduction. DN and DC also append `CompactionCreated`, so later attempts and runs use
the same compacted view.

A summary must finish successfully and contain non-whitespace text. The interpreter charges its
usage before validating it. A rejected summary leaves the previous summary and coverage in place;
already committed pruning remains. Durable summaries are limited to 65,536 characters.

If the provider reports context overflow, the engine may compact and retry once. Transport
ambiguity can duplicate that model call. A second rejection, or overflow without
`contextTokenLimit`, fails as `ContextOverflowError`.

### Replace compaction {#replacing-compaction}

Install a `ContextCompactor` Layer to change the strategy, estimator, or summary model. The default
is `ContextCompactor.layer`. All `AgentRuntime` entry points also need a
[Thread history policy](./threads#history-policy-and-append-ownership).

```ts
const compactorLayer = ContextCompactor.layerWithModel(summaryModel);

const result = AgentRuntime.run(agent, input).pipe(Effect.provide(compactorLayer));
```

The summary model's Layer requirements stay visible. Its usage is charged under that model's
provider and name.

A custom `compact` implementation emits `CompactionDecision` values. Each decision covers an
exclusive source prefix and either clears old tool results or supplies a summary. The interpreter
rejects cuts through tool pairs, changes to protected instructions or input, decisions that make no
progress, and more than one prune plus one summary in a turn. Summary calls must use
`request.summarize` so metering, response limits, and the run deadline still apply.

`estimate` must return a non-negative finite integer. Strategy failures use `CompactionError`.
Defects and interruption retain their Effect meaning.

Durable coordinators map the covered prefix to complete prior-run records before committing a
decision. A transform or decision that cannot map cleanly fails before the view changes. Decisions
cannot cover the current run. The canonical log remains append-only.

<a id="composing-preparation-and-tool-authorization"></a>
<a id="supplying-a-cloudflare-compactor"></a>

### Install a compactor for durable runs

`contextCompactorRunContextLayer` provides a `RunContextPreparation` service using your compactor:

```ts
import { contextCompactorRunContextLayer } from "@effect-agent/capabilities";
import { ContextCompactor } from "@effect-agent/engine";
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Layer } from "effect";

export const CompactorLive = ContextCompactor.layerWithModel(
  OpenAiLanguageModel.model("gpt-4.1-mini"),
);

export const RunContextLive = contextCompactorRunContextLayer.pipe(Layer.provide(CompactorLive));
```

Provide the summary model's client to `RunContextLive`, then install it through the
[Node host options](../platforms/node#configure-runtime-services),
[Cloudflare application layer](../platforms/cloudflare#configure-runtime-services), or
[custom runtime assembly](./run-agents#assemble-a-custom-durable-runtime).
To use a prompt transform and a custom compactor together, provide one `RunContextPreparation`
value with both `hook` and `compactor` fields.

### Manage summaries yourself {#explicit-compaction-artifacts}

`@effect-agent/capabilities` also has an application-managed data path.
`prepareModelContext` derives bounded text from a `ThreadSnapshot`.
`digestCompactionSource` binds a `CompactionArtifact` to that source. `applyCompaction` validates
the artifact before replacing covered view messages with its summary. The application creates,
stores, and applies the artifact.

`RetainedFact` values remain artifact metadata. They do not enter the prompt or a separate memory
store automatically. This path is separate from `ContextCompactor`; the interpreter does not call
`applyCompaction`. `recallMemory` is the optional read path for application-owned sources; it does
not persist passages or turn compaction artifacts into memory.

## Track usage {#observing-usage}

The budget snapshot separates cumulative and live context usage:

```ts
const report = Effect.gen(function* () {
  const usage = yield* budget.snapshot;
  usage.inputTokens;
  usage.cacheReadInputTokens;
  usage.cacheWriteInputTokens;
  usage.lastInputTokens;
  usage.lastOutputTokens;
});
```

Watch `lastInputTokens` for current context pressure. `inputTokens` is cumulative and grows on
every call. Provider caching may lower its cost. See [Run & stream](/guide/run-agents) for hook
setup.

## Choose limits {#sizing-guidance}

- Leave output and summary room under the model window. For a 200k window, start with a
  `contextTokenLimit` between 150k and 170k.
- `keepRecentTokens` defaults to 20k. Raise it when recent tool output must remain verbatim.
- Use `tokenBudget` as a runaway limit. Use `costBudgetMicrousd` to bound estimated spend.
- Delegate noisy research to bounded children so their raw tool output stays out of the parent
  context.
