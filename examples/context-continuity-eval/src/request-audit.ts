import { Context, Effect, FileSystem, Layer, Schema } from "effect";

import { EvaluationError } from "./contracts.ts";

export const RequestAudit = Schema.Struct({
  kind: Schema.Literals(["request", "response"]),
  request: Schema.Natural,
  phase: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  /** Exact synthetic provider request, or a bounded response summary. No HTTP headers. */
  json: Schema.String,
});

export type RequestAudit = typeof RequestAudit.Type;

/** The live client acquires this capability; host adapters own its durable implementation. */
export class RequestAuditSink extends Context.Service<
  RequestAuditSink,
  {
    readonly write: (event: RequestAudit) => Effect.Effect<void, EvaluationError>;
  }
>()("example/ContextContinuity/RequestAuditSink") {
  static readonly file = (path: string) =>
    Layer.effect(
      this,
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;

        return RequestAuditSink.of({
          write: (event) =>
            Schema.encodeEffect(Schema.fromJsonString(RequestAudit))(event).pipe(
              Effect.flatMap((json) => fs.writeFileString(path, `${json}\n`, { flag: "a" })),
              Effect.mapError(() =>
                EvaluationError.make({
                  stage: "evidence",
                  message: "Could not write request audit",
                }),
              ),
            ),
        });
      }),
    );
}
