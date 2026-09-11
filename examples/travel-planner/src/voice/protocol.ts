import { Schema } from "effect";

const Id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));

const Caption = Schema.Struct({
  event_id: Id,
  type: Schema.Literals(["session.input_transcript.delta", "session.output_transcript.delta"]),
  delta: Schema.String.check(Schema.isMaxLength(8000)),
  start_ms: Schema.Natural,
  end_ms: Schema.Natural,
});

export const LiveEvent = Schema.Union([
  Caption,
  Schema.Struct({ type: Schema.Literal("session.started"), session: Schema.Struct({ id: Id }) }),
  Schema.Struct({ type: Schema.Literal("session.closed") }),
  Schema.Struct({
    type: Schema.Literal("session.delegation.created"),
    event_id: Id,
    offset_ms: Schema.Natural,
    delegation: Schema.Struct({ id: Id, target: Schema.Literal("client") }),
  }),
  Schema.Struct({
    type: Schema.Literals([
      "session.commentary.appended",
      "session.thinking.appended",
      "session.instructions.appended",
    ]),
    client_event_id: Id,
  }),
  Schema.Struct({
    type: Schema.Literal("error"),
    error: Schema.Struct({
      code: Schema.NullOr(Schema.String),
      message: Schema.String,
      client_event_id: Schema.optionalKey(Schema.String),
    }),
  }),
]);

export type LiveEvent = typeof LiveEvent.Type;
export type Caption = typeof Caption.Type;

export const VoiceOffer = Schema.Struct({
  sdp: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(24 * 1024)),
  history: Schema.Array(
    Schema.Struct({
      role: Schema.Literals(["user", "assistant"]),
      text: Schema.String.check(Schema.isMaxLength(1000)),
    }),
  ).check(Schema.isMaxLength(12)),
});

export const VoiceAnswer = Schema.Struct({
  session: Schema.Struct({ id: Id }),
  transport: Schema.Struct({
    type: Schema.Literal("webrtc"),
    sdp: Schema.String.check(Schema.isMaxLength(64 * 1024)),
  }),
});

export class VoiceError extends Schema.TaggedError<VoiceError>()("VoiceError", {
  message: Schema.String,
}) {}

/** A conservative UTF-8 byte cap also bounds appends below Live's 500-token limit. */
export const shortContext = (text: string, limit = 420): string => {
  let result = "";
  let bytes = 0;
  const encoder = new TextEncoder();

  for (const character of text) {
    bytes += encoder.encode(character).length;
    if (bytes > limit) break;
    result += character;
  }

  return result;
};

/** Quiet chunks preserve the complete public summary, including caveats near its end. */
export const contextParts = (text: string): ReadonlyArray<string> => {
  const parts: string[] = [];
  let remaining = text;

  while (remaining.length) {
    const part = shortContext(remaining, 330);

    parts.push(part);
    remaining = remaining.slice(part.length);
  }

  return parts;
};
