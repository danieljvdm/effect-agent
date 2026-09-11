import { Schema } from "effect";

export interface DemoAccessEnvironment {
  readonly DEMO_OPENAI_API_KEY?: string;
}

const DemoKey = Schema.String.check(Schema.isPattern(/^[\x21-\x7e]{16,512}$/));

export const demoKeyConfigured = (env: DemoAccessEnvironment) =>
  Schema.is(DemoKey)(env.DEMO_OPENAI_API_KEY);

/** Only this metadata can leave the private credential store. */
export const OpenAiConnection = Schema.Struct({
  connected: Schema.Boolean,
  lastFour: Schema.NullOr(Schema.String),
  updatedAt: Schema.NullOr(Schema.String),
  /** Absent for existing personal-key connections; demo credentials never expose key metadata. */
  source: Schema.optionalKey(Schema.Literal("demo")),
});

export type OpenAiConnection = typeof OpenAiConnection.Type;

// Wrap before validation so malformed input cannot appear in schema diagnostics.
export const ConnectOpenAi = Schema.Struct({ apiKey: Schema.RedactedFromValue(Schema.String) });
