import type * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { MemoryContent, MemorySourceReference } from "@effect-agent/core/MemoryReference";
import {
  MemoryDocument,
  MemoryKey,
  MemoryReader,
  MemoryScope,
  MemoryWrite,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import * as Protocol from "@effect-agent/core/RememberingStore";
import { Effect, Encoding, Schema } from "effect";

export class Limits extends Schema.Class<Limits>("@effect-agent/remembering/Limits")({
  maxSourceBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_194_304 })),
  maxProposalBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_048_576 })),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
}) {}

export class SourceSnapshot extends Schema.Class<SourceSnapshot>(
  "@effect-agent/remembering/SourceSnapshot",
)({
  source: Protocol.Source,
  text: Schema.String.check(Schema.isMaxLength(4_194_304)),
}) {}

const SourceResult = Schema.NullOr(Schema.Union([SourceSnapshot, Protocol.Invalidation]));

/** Application-authorized output. The coordinator binds key, operation ID and expected revision.
 * Fact acceptance, sharing scopes, profile schema and source-aware merging remain application code.
 */
export const Decision = Schema.Union([
  Schema.TaggedStruct("Put", {
    locator: MemorySourceReference.fields.locator,
    content: MemoryContent,
    scopes: Schema.Array(MemoryScope).check(Schema.isMaxLength(128)),
  }),
  Schema.TaggedStruct("Withdraw", { reason: Schema.String.check(Schema.isMaxLength(4_096)) }),
  Schema.TaggedStruct("NoChange", {}),
  Schema.TaggedStruct("Reject", {}),
]);

export type Decision = typeof Decision.Type;

export interface MergeInput<Value, S extends MemoryNamespace.Any, T extends MemoryNamespace.Any> {
  readonly intent: Protocol.Intent<S, T>;
  readonly proposal: Protocol.Extracted<Value>;
  readonly current: MemoryDocument<T> | null;
}

export interface CleanupInput<
  Value,
  S extends MemoryNamespace.Any,
  T extends MemoryNamespace.Any,
> extends MergeInput<Value, S, T> {
  readonly suppression: Protocol.Invalidation;
  readonly applied: MemoryDocument<T>;
}

const equal = (left: unknown, right: unknown): boolean =>
  JSON.stringify(left) === JSON.stringify(right);

const sameKey = (left: MemoryKey, right: MemoryKey): boolean =>
  left.id === right.id && left.namespace.address === right.namespace.address;

const encodeIntent = Schema.encodeSync(Protocol.Intent.Wire);
const byteLength = (text: string): number => Encoding.encodeHex(text).length / 2;

const validateSource = Effect.fn("Remembering.validateSource")(function* (
  intent: Protocol.Intent,
  snapshot: SourceSnapshot,
  limits: Limits,
) {
  const source = yield* Schema.decodeUnknownEffect(SourceSnapshot)(snapshot).pipe(
    Effect.mapError(() => Protocol.ProcessingError.make({ reason: "invalid-input" })),
  );

  const actual = yield* Schema.encodeEffect(Protocol.Source)(source.source);
  const expected = yield* Schema.encodeEffect(Protocol.Source)(intent.source);

  if (!equal(actual, expected))
    return yield* Protocol.ProcessingError.make({ reason: "source-changed" });
  if (byteLength(source.text) > limits.maxSourceBytes)
    return yield* Protocol.ProcessingError.make({ reason: "budget" });

  return source;
});

const validateEvidence = Effect.fn("Remembering.validateEvidence")(function* (
  source: SourceSnapshot,
  proposal: Protocol.Proposal,
) {
  const bytes = Encoding.encodeHex(source.text);

  for (const evidence of proposal.evidence) {
    const quote = Encoding.encodeHex(evidence.quote);

    if (
      evidence.source.id !== source.source.key.id ||
      evidence.source.revision !== source.source.revision ||
      evidence.source.locator !== source.source.locator ||
      evidence.endByte <= evidence.startByte ||
      evidence.endByte > bytes.length / 2 ||
      quote.length / 2 !== evidence.endByte - evidence.startByte ||
      quote !== bytes.slice(evidence.startByte * 2, evidence.endByte * 2)
    )
      return yield* Protocol.ProcessingError.make({ reason: "invalid-evidence" });
  }
});

const validateCheckpoint = Effect.fn("Remembering.validateCheckpoint")(function* <
  S extends MemoryNamespace.Any,
  T extends MemoryNamespace.Any,
>(
  intent: Protocol.Intent<S, T>,
  input: Protocol.Checkpoint,
): Effect.fn.Return<Protocol.BoundCheckpoint<S, T>, Protocol.CheckpointError> {
  const checkpoint = yield* Schema.decodeUnknownEffect(Protocol.Checkpoint)(input).pipe(
    Effect.mapError(() => Protocol.CheckpointError.make({ reason: "corrupt" })),
  );

  if (
    !equal(encodeIntent(intent), encodeIntent(checkpoint.intent)) ||
    (checkpoint.suppression !== null && !sameKey(checkpoint.suppression.source, intent.source.key))
  )
    return yield* Protocol.CheckpointError.make({ reason: "corrupt" });
  const progress = checkpoint.progress;

  if (
    progress._tag !== "Pending" &&
    progress.applied !== null &&
    !sameKey(progress.applied.key, intent.target)
  )
    return yield* Protocol.CheckpointError.make({ reason: "corrupt" });
  if (
    progress._tag === "Prepared" &&
    (!sameKey(progress.command.key, intent.target) ||
      progress.command.operationId !== operationId(intent, progress.purpose, progress.attempt))
  )
    return yield* Protocol.CheckpointError.make({ reason: "corrupt" });

  const applied =
    progress._tag === "Pending" || progress.applied === null
      ? null
      : yield* MemoryDocument.restore(intent.target.namespace, progress.applied).pipe(
          Effect.mapError(() => Protocol.CheckpointError.make({ reason: "corrupt" })),
        );

  const boundProgress: Protocol.Progress<T> =
    progress._tag === "Pending"
      ? progress
      : progress._tag === "Prepared"
        ? {
            ...progress,
            applied,
            command: MemoryWrite.make({ ...progress.command, key: intent.target }),
          }
        : { ...progress, applied };

  const suppression: Protocol.BoundInvalidation<S> | null =
    checkpoint.suppression === null
      ? null
      : Object.assign(checkpoint.suppression, {
          source: MemoryKey.make({
            ...checkpoint.suppression.source,
            namespace: intent.source.key.namespace,
          }),
        });

  return Object.assign(checkpoint, { intent, progress: boundProgress, suppression });
});

const operationId = (
  intent: Protocol.Intent,
  purpose: "remember" | "cleanup",
  attempt: number,
): string => `remembering/${intent.id}/${purpose}/${attempt}`;

const completed = (
  progress: Protocol.Progress,
  outcome: "applied" | "no-change" | "rejected" | "suppressed",
  cleaned: boolean,
  applied?: MemoryDocument,
): Protocol.Progress => ({
  _tag: "Completed",
  proposal: progress._tag === "Pending" ? null : progress.proposal,
  attempt: progress._tag === "Pending" ? 0 : progress.attempt,
  applied: applied ?? (progress._tag === "Pending" ? null : progress.applied),
  outcome,
  cleaned,
});

/** Foreground admission performs only one durable port call. Bind authority and idempotency
 * from the trusted invocation before calling; a queued receipt is returned only after commit.
 */
export const admit = Effect.fn("Remembering.admit")(function* <E, R>(
  store: Protocol.Store<E, R>,
  intent: Protocol.Intent,
) {
  const checked = yield* Schema.decodeUnknownEffect(Protocol.Intent.Wire)(intent).pipe(
    Effect.mapError(() => Protocol.AdmissionError.make({ reason: "invalid-input" })),
  );

  const failpoint = yield* Protocol.MutationFailpoint;

  yield* failpoint.hit("admission:before");
  const receipt = yield* store.admit(checked);

  yield* failpoint.hit("admission:after");

  return receipt;
});

/** Source owners await this durable suppression receipt before acknowledging edit/deletion/Forget.
 * Scheduling all affected retained references and conditional cleanup is the host's obligation.
 */
export const invalidate = Effect.fn("Remembering.invalidate")(function* <E, R>(
  store: Protocol.Store<E, R>,
  event: Protocol.Invalidation,
) {
  const checked = yield* Schema.decodeUnknownEffect(Protocol.Invalidation)(event).pipe(
    Effect.mapError(() => Protocol.AdmissionError.make({ reason: "invalid-input" })),
  );

  const failpoint = yield* Protocol.MutationFailpoint;

  yield* failpoint.hit("suppression:before");
  const receipt = yield* store.invalidate(checked);

  yield* failpoint.hit("suppression:after");

  return receipt;
});

/** Define one application learner. Extraction may call native Effect AI LanguageModel directly;
 * callback/model/schema requirements and errors remain in the returned Effect. No model owns
 * authority, source loading, fact acceptance, profile representation or sharing policy.
 *
 * advance is finite: it extracts, prepares, dispatches, or rebases at most once. The host calls it
 * again when ready. A proposal is persisted before any target read; a complete command is saved
 * before dispatch. Unknown outcomes leave that command untouched. Only MemoryConflict rebases.
 * Cleanup receives the immutable proposal, original applied document and latest target so it can
 * remove that contribution without overwriting intervening human corrections.
 *
 * loadSource returns an explicit Invalidation when source authority changed or was revoked; its
 * suppression is committed before processing returns. null means absent on an initial load. A
 * later null or a changed snapshot without an authority event fails source-changed and retains
 * the obligation. Sources must keep revision content immutable. Evidence is checked again before
 * preparing a new command. A saved Prepared command may already have executed, so it never calls
 * loadSource and must reconcile even during a source outage. Source owners durably invalidate
 * before acknowledging edits, and authoritative recall filters suppression while cleanup runs.
 * No source text, proposal, authority identity, or extracted values are added to tracing spans.
 */
export const make = <
  S extends MemoryNamespace.Any,
  T extends MemoryNamespace.Any,
  Value,
  Encoded extends Schema.Json,
  DecodeR,
  EncodeR,
  SourceE,
  SourceR,
  ExtractE,
  ExtractR,
  MergeE,
  MergeR,
  CleanupE,
  CleanupR,
>(options: {
  readonly proposal: Schema.Codec<Value, Encoded, DecodeR, EncodeR>;
  readonly loadSource: (
    intent: Protocol.Intent<S, T>,
  ) => Effect.Effect<SourceSnapshot | Protocol.Invalidation | null, SourceE, SourceR>;
  readonly extract: (
    source: SourceSnapshot,
    intent: Protocol.Intent<S, T>,
  ) => Effect.Effect<Protocol.Extracted<Value> | null, ExtractE, ExtractR>;
  readonly merge: (input: MergeInput<Value, S, T>) => Effect.Effect<Decision, MergeE, MergeR>;
  readonly cleanup: (
    input: CleanupInput<Value, S, T>,
  ) => Effect.Effect<Decision, CleanupE, CleanupR>;
}) => {
  const advance = Effect.fn("Remembering.advance")(function* <StoreE, StoreR>(input: {
    readonly intent: Protocol.Intent<S, T>;
    readonly store: Protocol.Store<StoreE, StoreR>;
    readonly limits: Limits;
    /** Stops new extraction only. Saved commands, rebase and suppression cleanup still run. */
    readonly extractionEnabled: boolean;
  }) {
    const limits = yield* Schema.decodeUnknownEffect(Limits)(input.limits).pipe(
      Effect.mapError(() => Protocol.ProcessingError.make({ reason: "invalid-input" })),
    );

    const decodedIntent = yield* Schema.decodeUnknownEffect(Protocol.Intent.Wire)(
      input.intent,
    ).pipe(Effect.mapError(() => Protocol.ProcessingError.make({ reason: "invalid-input" })));

    const intent: Protocol.Intent<S, T> = Object.assign(decodedIntent, {
      source: {
        ...decodedIntent.source,
        key: MemoryKey.make({
          ...decodedIntent.source.key,
          namespace: input.intent.source.key.namespace,
        }),
      },
      target: MemoryKey.make({ ...decodedIntent.target, namespace: input.intent.target.namespace }),
    });

    const run = Effect.gen(function* () {
      const checkpoint = yield* input.store
        .read(intent)
        .pipe(Effect.flatMap((value) => validateCheckpoint(intent, value)));

      const progress = checkpoint.progress;
      const suppression = checkpoint.suppression;

      if (
        progress._tag === "Proposed" &&
        suppression === null &&
        byteLength(JSON.stringify(progress.proposal)) > limits.maxProposalBytes
      )
        return yield* Protocol.ProcessingError.make({ reason: "budget" });
      const failpoint = yield* Protocol.MutationFailpoint;

      const save = Effect.fn("Remembering.save")(function* (
        next: Protocol.Progress,
        before: Protocol.MutationPoint,
        after: Protocol.MutationPoint,
      ) {
        yield* failpoint.hit(before);

        const stored = yield* input.store.save({
          intent,
          expectedVersion: checkpoint.version,
          progress: next,
        });

        yield* failpoint.hit(after);

        return yield* validateCheckpoint(intent, stored);
      });

      const suppress = Effect.fn("Remembering.suppress")(function* (event: Protocol.Invalidation) {
        if (!sameKey(event.source, intent.source.key))
          return yield* Protocol.ProcessingError.make({ reason: "invalid-input" });
        yield* invalidate(input.store, event);

        return yield* input.store
          .read(intent)
          .pipe(Effect.flatMap((value) => validateCheckpoint(intent, value)));
      });

      if (progress._tag === "Completed" && (suppression === null || progress.cleaned))
        return checkpoint;
      if (
        suppression !== null &&
        progress._tag !== "Prepared" &&
        (progress._tag === "Pending" || progress.applied === null)
      )
        return yield* save(
          completed(progress, "suppressed", true),
          "completion:before",
          "completion:after",
        );

      if (progress._tag === "Pending") {
        if (!input.extractionEnabled)
          return yield* Protocol.ProcessingError.make({ reason: "disabled" });

        const snapshot = yield* options
          .loadSource(intent)
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(SourceResult)));

        if (snapshot === null)
          return yield* save(
            completed(progress, "rejected", false),
            "completion:before",
            "completion:after",
          );
        if (Schema.is(Protocol.Invalidation)(snapshot)) return yield* suppress(snapshot);
        const source = yield* validateSource(intent, snapshot, limits);
        const extracted = yield* options.extract(source, intent);

        if (extracted === null)
          return yield* save(
            completed(progress, "no-change", false),
            "completion:before",
            "completion:after",
          );
        const value = yield* Schema.encodeEffect(options.proposal)(extracted.value);

        const proposal = yield* Schema.decodeUnknownEffect(Protocol.Proposal)({
          version: 1,
          value,
          evidence: extracted.evidence,
        });

        if (byteLength(JSON.stringify(proposal)) > limits.maxProposalBytes)
          return yield* Protocol.ProcessingError.make({ reason: "budget" });
        yield* validateEvidence(source, proposal);

        const latest = yield* options
          .loadSource(intent)
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(SourceResult)));

        if (latest === null)
          return yield* Protocol.ProcessingError.make({ reason: "source-changed" });
        if (Schema.is(Protocol.Invalidation)(latest)) return yield* suppress(latest);
        yield* validateSource(intent, latest, limits);
        if (latest.text !== source.text)
          return yield* Protocol.ProcessingError.make({ reason: "source-changed" });

        return yield* save(
          { _tag: "Proposed", proposal, attempt: 0, purpose: "remember", applied: null },
          "proposal:before",
          "proposal:after",
        );
      }

      if (progress._tag === "Prepared") {
        const writer = yield* MemoryWriter;
        const command = MemoryWrite.make({ ...progress.command, key: intent.target });

        const outcome = yield* writer.change(command).pipe(
          Effect.map((document) => ({ _tag: "Written" as const, document })),
          Effect.catchTag("MemoryConflict", () => Effect.succeed({ _tag: "Conflict" as const })),
          Effect.catchTag("MemoryWithdrawn", () => Effect.succeed({ _tag: "Withdrawn" as const })),
        );

        if (outcome._tag === "Written") {
          return yield* save(
            completed(
              progress,
              progress.purpose === "cleanup" ? "suppressed" : "applied",
              progress.purpose === "cleanup",
              progress.purpose === "remember" ? outcome.document : undefined,
            ),
            "completion:before",
            "completion:after",
          );
        }
        if (
          outcome._tag === "Withdrawn" ||
          (suppression !== null && progress.purpose === "remember")
        )
          return yield* save(
            completed(
              progress,
              suppression !== null ? "suppressed" : "rejected",
              suppression !== null,
            ),
            "completion:before",
            "completion:after",
          );

        return yield* save(
          {
            _tag: "Proposed",
            proposal: progress.proposal,
            attempt: progress.attempt,
            purpose: progress.purpose,
            applied: progress.applied,
          },
          "rebase:before",
          "rebase:after",
        );
      }

      const proposal = progress.proposal;

      if (proposal === null) return yield* Protocol.CheckpointError.make({ reason: "corrupt" });
      if (suppression === null) {
        const latest = yield* options
          .loadSource(intent)
          .pipe(Effect.flatMap(Schema.decodeUnknownEffect(SourceResult)));

        if (latest === null)
          return yield* Protocol.ProcessingError.make({ reason: "source-changed" });
        if (Schema.is(Protocol.Invalidation)(latest)) return yield* suppress(latest);
        yield* validateSource(intent, latest, limits);
        yield* validateEvidence(latest, proposal);
      }
      const value = yield* Schema.decodeUnknownEffect(options.proposal)(proposal.value);
      const reader = yield* MemoryReader;
      const current = yield* reader.get(intent.target);

      const applied =
        progress.applied === null
          ? null
          : yield* MemoryDocument.restore(intent.target.namespace, progress.applied);

      const cleanup = suppression !== null && applied !== null;
      const mergeInput = { intent, proposal: { value, evidence: proposal.evidence }, current };

      const candidate = cleanup
        ? yield* options.cleanup({ ...mergeInput, suppression, applied })
        : yield* options.merge(mergeInput);

      const decision = yield* Schema.decodeUnknownEffect(Decision)(candidate);

      if (cleanup && decision._tag === "Reject")
        return yield* Protocol.ProcessingError.make({ reason: "cleanup-rejected" });
      if (decision._tag === "NoChange" || decision._tag === "Reject")
        return yield* save(
          completed(
            progress,
            cleanup ? "suppressed" : decision._tag === "Reject" ? "rejected" : "no-change",
            cleanup,
            !cleanup && decision._tag === "NoChange" && current !== null ? current : undefined,
          ),
          cleanup ? "cleanup:before" : "completion:before",
          cleanup ? "cleanup:after" : "completion:after",
        );
      if (decision._tag === "Withdraw" && current === null)
        return yield* save(
          completed(progress, cleanup ? "suppressed" : "no-change", cleanup),
          "completion:before",
          "completion:after",
        );
      const purpose = cleanup ? "cleanup" : "remember";
      const attempt = progress.attempt + 1;
      const key = intent.target;

      const write = yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)({
        ...decision,
        key,
        operationId: operationId(intent, purpose, attempt),
        expectedRevision: current?.source.revision ?? null,
      });

      return yield* save(
        { _tag: "Prepared", proposal, applied, attempt, purpose, command: write },
        cleanup ? "cleanup:before" : "command:before",
        cleanup ? "cleanup:after" : "command:after",
      );
    });

    return yield* run.pipe(
      Effect.timeoutOrElse({
        duration: limits.timeoutMillis,
        orElse: () => Protocol.ProcessingError.make({ reason: "timeout" }),
      }),
    );
  });

  return { advance };
};
