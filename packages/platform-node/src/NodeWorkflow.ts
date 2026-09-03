import {
  WorkflowDispatchError,
  WorkflowDispatchIntent,
  WorkflowDispatchScan,
  WorkflowDispatchStore,
  WorkflowRepairTrigger,
} from "@effect-agent/workflow/WorkflowDispatch";
import { Cause, Duration, Effect, Layer, Option, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql";

const StoredIntent = Schema.Struct({
  deployment_id: Schema.String,
  workflow_name: Schema.String,
  execution_id: Schema.String,
  intent_json: Schema.String,
});

const IntentJson = Schema.fromJsonString(WorkflowDispatchIntent);
const decodeIntent = Schema.decodeUnknownEffect(IntentJson, { onExcessProperty: "error" });
const encodeIntent = Schema.encodeEffect(IntentJson);

const dispatchError = (operation: string) => (cause: unknown) =>
  Schema.is(WorkflowDispatchError)(cause)
    ? cause
    : new WorkflowDispatchError({
        operation,
        message: "Workflow dispatch storage failed or contains incompatible data",
        cause,
      });

const decodeRow = Effect.fn("SqlWorkflowDispatchStore.decodeRow")(function* (value: unknown) {
  const row = yield* Schema.decodeUnknownEffect(StoredIntent, { onExcessProperty: "error" })(value);
  const intent = yield* decodeIntent(row.intent_json);

  if (
    row.deployment_id !== intent.deploymentId ||
    row.workflow_name !== intent.workflowName ||
    row.execution_id !== intent.executionId
  ) {
    return yield* new WorkflowDispatchError({
      operation: "decode",
      message: "Stored Workflow dispatch identity disagrees with its intent",
    });
  }

  return intent;
});

/**
 * Durable dispatch outbox over an application-supplied SqlClient. This adapter uses
 * SQLite/PostgreSQL SQL syntax and is certified with SQLite. It does not own an engine
 * or a database connection. Agent admission, dispatch persistence, and native Workflow
 * storage are separate commits; the registered repair trigger closes those gaps.
 * Stored version or shape mismatches fail typed and require an explicit data reset.
 */
export class SqlWorkflowDispatchStore {
  static readonly layer: Layer.Layer<
    WorkflowDispatchStore,
    WorkflowDispatchError,
    SqlClient.SqlClient
  > = Layer.effect(WorkflowDispatchStore)(
    Effect.gen(function* () {
      const sql = (yield* SqlClient.SqlClient).withoutTransforms();

      yield* sql`
        CREATE TABLE IF NOT EXISTS effect_agent_workflow_dispatch (
          workflow_name TEXT NOT NULL,
          execution_id TEXT NOT NULL,
          deployment_id TEXT NOT NULL,
          intent_json TEXT NOT NULL,
          PRIMARY KEY (workflow_name, execution_id)
        )
      `;
      yield* sql`
        CREATE INDEX IF NOT EXISTS effect_agent_workflow_dispatch_scan
        ON effect_agent_workflow_dispatch (deployment_id, workflow_name, execution_id)
      `;

      const put = Effect.fn("SqlWorkflowDispatchStore.put")(
        function* (input: WorkflowDispatchIntent) {
          const intent = yield* Schema.decodeUnknownEffect(WorkflowDispatchIntent)(input);
          const encoded = yield* encodeIntent(intent);

          yield* sql`
            INSERT INTO effect_agent_workflow_dispatch
              (workflow_name, execution_id, deployment_id, intent_json)
            VALUES (${intent.workflowName}, ${intent.executionId}, ${intent.deploymentId}, ${encoded})
            ON CONFLICT (workflow_name, execution_id) DO NOTHING
          `;

          const rows = yield* sql`
            SELECT deployment_id, workflow_name, execution_id, intent_json
            FROM effect_agent_workflow_dispatch
            WHERE workflow_name = ${intent.workflowName} AND execution_id = ${intent.executionId}
          `;

          const existing = yield* decodeRow(rows[0]);

          if ((yield* encodeIntent(existing)) !== encoded) {
            return yield* new WorkflowDispatchError({
              operation: "put",
              message: "Workflow dispatch identity already belongs to a different immutable intent",
            });
          }
        },
        sql.withTransaction,
        Effect.mapError(dispatchError("put")),
      );

      const scan = Effect.fn("SqlWorkflowDispatchStore.scan")(
        function* (input: WorkflowDispatchScan) {
          const request = yield* Schema.decodeUnknownEffect(WorkflowDispatchScan)(input);

          const rows = yield* sql`
            SELECT deployment_id, workflow_name, execution_id, intent_json
            FROM effect_agent_workflow_dispatch
            WHERE deployment_id = ${request.deploymentId}
              AND workflow_name = ${request.workflowName}
              AND execution_id > ${request.after ?? ""}
            ORDER BY execution_id ASC
            LIMIT ${request.limit}
          `;

          return yield* Effect.forEach(rows, decodeRow);
        },
        Effect.mapError(dispatchError("scan")),
      );

      const remove = Effect.fn("SqlWorkflowDispatchStore.remove")(
        function* (input: WorkflowDispatchIntent) {
          const intent = yield* Schema.decodeUnknownEffect(WorkflowDispatchIntent)(input);

          const rows = yield* sql`
            SELECT deployment_id, workflow_name, execution_id, intent_json
            FROM effect_agent_workflow_dispatch
            WHERE workflow_name = ${intent.workflowName} AND execution_id = ${intent.executionId}
          `;

          if (rows.length === 0) return;
          const existing = yield* decodeRow(rows[0]);

          if ((yield* encodeIntent(existing)) !== (yield* encodeIntent(intent))) {
            return yield* new WorkflowDispatchError({
              operation: "remove",
              message: "Cannot remove a different immutable Workflow dispatch intent",
            });
          }
          yield* sql`
            DELETE FROM effect_agent_workflow_dispatch
            WHERE workflow_name = ${intent.workflowName} AND execution_id = ${intent.executionId}
          `;
        },
        sql.withTransaction,
        Effect.mapError(dispatchError("remove")),
      );

      return WorkflowDispatchStore.of({ put, scan, remove });
    }).pipe(Effect.mapError(dispatchError("initialize"))),
  );
}

export class NodeWorkflowRepairConfigError extends Schema.TaggedError<NodeWorkflowRepairConfigError>()(
  "NodeWorkflowRepairConfigError",
  { message: Schema.String },
) {}

/** A host-scoped startup and polling trigger. No ordinary Node agent worker is started. */
export class NodeWorkflowRepairTrigger {
  static layer(
    options: { readonly interval?: Duration.Input } = {},
  ): Layer.Layer<WorkflowRepairTrigger, NodeWorkflowRepairConfigError> {
    return Layer.effect(WorkflowRepairTrigger)(
      Effect.gen(function* () {
        const interval = Duration.fromInput(options.interval ?? "1 second");

        if (
          Option.isNone(interval) ||
          !Duration.isFinite(interval.value) ||
          !Duration.isPositive(interval.value)
        ) {
          return yield* new NodeWorkflowRepairConfigError({
            message: "Workflow repair interval must be finite and greater than zero",
          });
        }
        const delay = interval.value;

        return WorkflowRepairTrigger.of({
          register: Effect.fn("NodeWorkflowRepairTrigger.register")(function* (repair) {
            const attempt = repair.pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause)
                  ? Effect.failCause(cause)
                  : Effect.logError("Workflow repair trigger failed; next poll will retry", cause),
              ),
            );

            yield* attempt;
            yield* Effect.gen(function* () {
              while (true) {
                yield* Effect.sleep(delay);
                yield* attempt;
              }
            }).pipe(Effect.forkScoped);
          }),
        });
      }),
    );
  }
}
