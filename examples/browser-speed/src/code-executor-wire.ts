import { Schema } from "effect";
import {
  CodeExecutionError,
  CodeExecutionRequest,
  CodeExecutionResult,
} from "effect-agent/code-executor";

export const Request = Schema.fromJsonString(
  Schema.toCodecJson(
    Schema.Struct({
      id: Schema.String.check(Schema.isUUID()),
      request: CodeExecutionRequest,
      traceId: Schema.String,
      spanId: Schema.String,
    }),
  ),
);

export const Outcome = Schema.fromJsonString(
  Schema.toCodecJson(
    Schema.Union([
      Schema.Struct({ success: CodeExecutionResult }),
      Schema.Struct({ failure: CodeExecutionError }),
    ]),
  ),
);
