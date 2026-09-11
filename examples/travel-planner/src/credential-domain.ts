import { Schema } from "effect";

/** Only this metadata can leave the private credential store. */
export const OpenAiConnection = Schema.Struct({
  connected: Schema.Boolean,
  lastFour: Schema.NullOr(Schema.String),
  updatedAt: Schema.NullOr(Schema.String),
});

export type OpenAiConnection = typeof OpenAiConnection.Type;

// Wrap before validation so malformed input cannot appear in schema diagnostics.
export const ConnectOpenAi = Schema.Struct({ apiKey: Schema.RedactedFromValue(Schema.String) });
