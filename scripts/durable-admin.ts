import { ConversationId, SubmissionId } from "@effect-agent/core";
import { NodeDurableRuntime } from "@effect-agent/platform-node";
import {
  DurableAgentRuntime,
  ObligationThresholds,
  OperationCaller,
  Principal,
  RecoveryExplanation,
  RetryCommand,
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
  renderRecoveryExplanation,
  type IntegrityReport,
  type ObligationReport,
  type RecoveryReport,
} from "@effect-agent/session";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";

/**
 * P7 operator CLI (plan §3): explain | verify | retry | wake | obligations over the DN
 * platform Layers. The stack opens the SAME SQLite file the host uses — run it against a live
 * deployment's database only for the read-only subcommands (explain/verify/obligations), and
 * prefer a quiesced host for `retry`. No worker loop starts and no startup recovery pass runs:
 * the Layer assembles the coordinator only, so read-only subcommands perform zero writes
 * (opening a brand-new file would create empty schema tables — point `--database` at an
 * existing deployment file).
 */

class InvalidIdentifier extends Schema.TaggedError<InvalidIdentifier>()("InvalidIdentifier", {
  kind: Schema.String,
  value: Schema.String,
}) {
  override get message() {
    return `${this.value} is not a valid ${this.kind}`;
  }
}

class MissingSelector extends Schema.TaggedError<MissingSelector>()("MissingSelector", {
  message: Schema.String,
}) {}

class IntegrityViolation extends Schema.TaggedError<IntegrityViolation>()("IntegrityViolation", {
  conversationId: Schema.String,
  failed: Schema.Array(Schema.String),
}) {
  override get message() {
    return `Integrity verification failed for ${this.conversationId}: ${this.failed.join("; ")}`;
  }
}

const decodeConversationId = (value: string) =>
  Schema.decodeUnknownEffect(ConversationId)(value).pipe(
    Effect.mapError(() => InvalidIdentifier.make({ kind: "ConversationId", value })),
  );

const decodeSubmissionId = (value: string) =>
  Schema.decodeUnknownEffect(SubmissionId)(value).pipe(
    Effect.mapError(() => InvalidIdentifier.make({ kind: "SubmissionId", value })),
  );

const encodeExplanation = Schema.encodeEffect(RecoveryExplanation);

/** This local database operator runs under an explicit service-possession policy. */
const ADMIN_CALLER = OperationCaller.make({
  principal: Schema.decodeSync(Principal)("principal-durable-admin"),
});

const database = Flag.file("database").pipe(
  Flag.withDescription("SQLite database file of the DN deployment (Conversation Log + ledger)."),
);

const json = Flag.boolean("json").pipe(
  Flag.withDescription("Print Schema-encoded JSON instead of operator text."),
);

/** The admin CLI writes with its own producer identity; epoch fencing stays authoritative. */
const runtimeLayerFor = (filename: string) =>
  NodeDurableRuntime.layer({
    filename,
    deploymentId: "durable-admin",
    producerId: "durable-admin",
  }).pipe(
    Layer.provide(
      Layer.merge(possessionOperationAuthorizerLayer, possessionChildAdmissionAuthorizerLayer),
    ),
  );

const admin = CliCommand.make("durable-admin").pipe(
  CliCommand.withSharedFlags({ database }),
  CliCommand.withDescription(
    "Administrative operations over a DN durable deployment: explain recovery state, verify " +
      "conversation integrity, re-drive one recovery decision, nudge a lane, and report aged " +
      "settlement obligations.",
  ),
);

const withRuntime = <A, E>(effect: Effect.Effect<A, E, DurableAgentRuntime>) =>
  Effect.gen(function* () {
    const { database: filename } = yield* admin;
    return yield* effect.pipe(Effect.provide(runtimeLayerFor(filename)));
  });

const printExplanation = (explanation: RecoveryExplanation, asJson: boolean) =>
  asJson
    ? encodeExplanation(explanation).pipe(
        Effect.flatMap((encoded) => Console.log(JSON.stringify(encoded, null, 2))),
      )
    : Console.log(`${renderRecoveryExplanation(explanation)}\n`);

const explainCommand = CliCommand.make(
  "explain",
  {
    submission: Flag.string("submission").pipe(
      Flag.optional,
      Flag.withDescription("Explain one Submission by identity."),
    ),
    conversation: Flag.string("conversation").pipe(
      Flag.optional,
      Flag.withDescription("Explain every nonterminal Submission of one Conversation lane."),
    ),
    json,
  },
  ({ conversation, json: asJson, submission }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        if (submission._tag === "Some") {
          const submissionId = yield* decodeSubmissionId(submission.value);
          const explanation = yield* runtime.explain(submissionId, ADMIN_CALLER);
          return yield* printExplanation(explanation, asJson);
        }
        if (conversation._tag === "Some") {
          const conversationId = yield* decodeConversationId(conversation.value);
          const explanations = yield* runtime.explainConversation(conversationId, ADMIN_CALLER);
          if (explanations.length === 0) {
            return yield* Console.log(
              `No nonterminal Submissions on conversation ${conversationId}.`,
            );
          }
          return yield* Effect.forEach(
            explanations,
            (explanation) => printExplanation(explanation, asJson),
            { discard: true },
          );
        }
        return yield* MissingSelector.make({
          message: "Pass --submission <id> or --conversation <id>.",
        });
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Read-only recovery explanation: classifier decision, meaning, and predicted disposition. " +
      "Performs zero writes.",
  ),
);

const renderReport = (report: IntegrityReport): ReadonlyArray<string> => [
  `Conversation ${report.conversationId}: ${report.recordCount} records, tail ${report.tailSequence}, ${report.submissionCount} submissions`,
  ...report.checks.map(
    (check) =>
      `  ${check.status === "passed" ? "PASS" : check.status === "failed" ? "FAIL" : "SKIP"} ${check.name}${
        check.detail === undefined ? "" : ` — ${check.detail}`
      }`,
  ),
  report.ok ? "OK — no integrity check failed." : "FAILED — see the checks above.",
];

const verifyCommand = CliCommand.make(
  "verify",
  {
    conversation: Flag.string("conversation").pipe(Flag.withDescription("Conversation to verify.")),
  },
  ({ conversation }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const conversationId = yield* decodeConversationId(conversation);
        const report = yield* runtime.verify(conversationId, ADMIN_CALLER);
        yield* Effect.forEach(renderReport(report), (line) => Console.log(line), {
          discard: true,
        });
        if (!report.ok) {
          return yield* IntegrityViolation.make({
            conversationId: conversation,
            failed: report.checks
              .filter((check) => check.status === "failed")
              .map((check) => check.name),
          });
        }
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Read-only integrity verification: typed per-check results, never a repair. Exits non-zero " +
      "when any check fails.",
  ),
);

const renderRetry = (report: RecoveryReport): string =>
  `Submission ${report.submissionId}: decision ${report.decision._tag} → disposition ${report.disposition}`;

const retryCommand = CliCommand.make(
  "retry",
  {
    submission: Flag.string("submission").pipe(
      Flag.withDescription("Submission whose classified recovery decision is re-driven."),
    ),
    author: Flag.string("author").pipe(
      Flag.withDescription("Mandatory audit author recorded with the operator action (SEC-011)."),
    ),
    reason: Flag.string("reason").pipe(
      Flag.withDescription("Mandatory audit reason recorded with the operator action (SEC-011)."),
    ),
  },
  ({ author, reason, submission }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const submissionId = yield* decodeSubmissionId(submission);
        const command = yield* Schema.decodeUnknownEffect(RetryCommand)({
          submissionId,
          author,
          reason,
        }).pipe(
          Effect.mapError(() =>
            InvalidIdentifier.make({ kind: "RetryCommand", value: `${author}/${reason}` }),
          ),
        );
        const report = yield* runtime.retry(command, ADMIN_CALLER);
        yield* Console.log(renderRetry(report));
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Audited single-Submission re-drive of the classified recovery decision. Refuses typed for " +
      "settled work and for lanes owned by resolveUnknown/resolveApproval.",
  ),
);

const wakeCommand = CliCommand.make(
  "wake",
  {
    conversation: Flag.string("conversation").pipe(
      Flag.withDescription("Conversation lane to nudge."),
    ),
  },
  ({ conversation }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const conversationId = yield* decodeConversationId(conversation);
        yield* runtime.wake(conversationId, ADMIN_CALLER);
        yield* Console.log(
          `Wake hint sent for ${conversationId} (droppable by contract; workers' ledger scans stay authoritative).`,
        );
      }),
    ),
).pipe(CliCommand.withDescription("Send the documented operator liveness nudge to one lane."));

const renderObligations = (report: ObligationReport): ReadonlyArray<string> =>
  report.entries.length === 0
    ? ["No nonterminal Submissions — every accepted obligation is settled."]
    : report.entries.map(
        (entry) =>
          `${entry.severity.toUpperCase().padEnd(7)} ${entry.blockedOn.padEnd(15)} age=${String(entry.ageSeconds).padStart(6)}s ${entry.submissionId} (${entry.state}) on ${entry.conversationId}`,
      );

const obligationsCommand = CliCommand.make(
  "obligations",
  {
    agingSeconds: Flag.integer("aging-seconds").pipe(
      Flag.withDefault(300),
      Flag.withDescription("Age (seconds) at which an obligation is reported aging."),
    ),
    overdueSeconds: Flag.integer("overdue-seconds").pipe(
      Flag.withDefault(3_600),
      Flag.withDescription("Age (seconds) at which an obligation is reported overdue."),
    ),
  },
  ({ agingSeconds, overdueSeconds }) =>
    withRuntime(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const thresholds = yield* Schema.decodeUnknownEffect(ObligationThresholds)({
          agingSeconds,
          overdueSeconds,
        }).pipe(
          Effect.mapError(() =>
            InvalidIdentifier.make({
              kind: "ObligationThresholds",
              value: `${agingSeconds}/${overdueSeconds}`,
            }),
          ),
        );
        const report = yield* runtime.scanObligations(thresholds, ADMIN_CALLER);
        yield* Effect.forEach(renderObligations(report), (line) => Console.log(line), {
          discard: true,
        });
      }),
    ),
).pipe(
  CliCommand.withDescription(
    "Scan-based settlement-obligation report (DUR-017/OPS-001): every nonterminal Submission " +
      "with what it is blocked on, its age, and a threshold-classified severity.",
  ),
);

const command = admin.pipe(
  CliCommand.withSubcommands([
    explainCommand,
    verifyCommand,
    retryCommand,
    wakeCommand,
    obligationsCommand,
  ]),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
