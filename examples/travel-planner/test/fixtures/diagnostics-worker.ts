import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import { Cause, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { PlannerError } from "../../src/domain.ts";
import {
  DiagnosticFailpoint,
  FailureDiagnostics,
  FailureDiagnosticsLive,
  recordDiagnostic,
} from "../../src/server/diagnostics.ts";

const Command = Schema.Struct({
  action: Schema.Literals(["append", "list", "corrupt", "raw"]),
  point: Schema.optionalKey(Schema.String),
  mode: Schema.optionalKey(Schema.String),
});

export class Diagnostics extends DurableObject {
  async fetch(request: Request) {
    const command = Schema.decodeUnknownSync(Command)(await request.json());

    const store = FailureDiagnosticsLive.pipe(
      Layer.provideMerge(SqliteClient.layer({ storage: this.ctx.storage })),
      Layer.provide(
        Layer.succeed(DiagnosticFailpoint, {
          hit: (point) =>
            point !== command.point
              ? Effect.void
              : command.mode === "defect"
                ? Effect.die("injected defect")
                : command.mode === "interrupt"
                  ? Effect.interrupt
                  : Effect.fail(new PlannerError({ code: "storage", message: "injected failure" })),
        }),
      ),
    );

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const diagnostics = yield* FailureDiagnostics;
        const sql = yield* SqlClient;

        if (command.action === "append") {
          yield* recordDiagnostic(
            "read_travel_page: browser-failure",
            new Error("Browser failure", {
              cause: {
                status: 403,
                requestId: "req-test",
                url: "https://www.airbnb.com/rooms/123?access_token=PRIVATE",
                headers: { authorization: "Bearer PRIVATE", "cf-ray": "ray-test" },
                body: JSON.stringify({ error: "Access denied", apiKey: "PRIVATE" }),
              },
            }),
            { toolCallId: "call-test", runId: "run-test" },
          );

          return "original outcome unchanged";
        }
        if (command.action === "list") return yield* diagnostics.list;
        if (command.action === "corrupt")
          yield* sql`INSERT INTO travel_failure_diagnostics (value) VALUES ('{"version":999}')`;

        return yield* sql`SELECT value FROM travel_failure_diagnostics ORDER BY id`;
      }).pipe(Effect.provide(store), Effect.exit),
    );

    return Response.json(
      result._tag === "Success"
        ? { _tag: "Success", value: result.value }
        : { _tag: "Failure", error: Cause.pretty(result.cause) },
    );
  }
}

export default {
  fetch(request: Request, env: { DIAGNOSTICS: DurableObjectNamespace<Diagnostics> }) {
    return env.DIAGNOSTICS.getByName(new URL(request.url).pathname).fetch(request);
  },
};
