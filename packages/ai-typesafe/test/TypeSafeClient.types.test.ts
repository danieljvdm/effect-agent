import type { TypeSafeSchema } from "@effect-agent/ai-typesafe";
import { TypeSafeClient } from "@effect-agent/ai-typesafe";
import { Config, Effect, type Layer, Redacted } from "effect";
import type { AiError, Tool } from "effect/unstable/ai";
import type { HttpClient } from "effect/unstable/http";
import { expectTypeOf, it } from "vite-plus/test";

import type { evaluateTicket, program } from "../examples/evaluate.ts";
import type { ClassifyTicket, classifyTicket } from "../examples/tool.ts";

it("infers question keys, selected choices, and exact Effect errors and services", () => {
  type Evaluation = Effect.Success<typeof evaluateTicket>;

  expectTypeOf<Evaluation["answers"]["department"]["choice"]>().toEqualTypeOf<
    "billing" | "technical"
  >();
  expectTypeOf<keyof Evaluation["answers"]>().toEqualTypeOf<
    "department" | "frustration" | "urgent"
  >();
  expectTypeOf<Evaluation["answers"]["department"]["probabilities"]>().toEqualTypeOf<{
    readonly billing: number;
    readonly technical: number;
  }>();
  expectTypeOf<Evaluation["answers"]["frustration"]["score"]>().toEqualTypeOf<number>();
  expectTypeOf<Evaluation["answers"]["urgent"]>().toEqualTypeOf<{
    readonly type: "noul";
    readonly noul: number;
  }>();
  expectTypeOf<Effect.Error<typeof evaluateTicket>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<
    Effect.Services<typeof evaluateTicket>
  >().toEqualTypeOf<TypeSafeClient.TypeSafeClient>();
  expectTypeOf<Effect.Services<typeof program>>().toEqualTypeOf<never>();
});

it("keeps dynamic questions and criteria broad and preserves Tool dependencies and failures", () => {
  type Dynamic = TypeSafeSchema.EvaluateResponse<TypeSafeSchema.Questions>["answers"][string];

  expectTypeOf<Dynamic>().toEqualTypeOf<TypeSafeSchema.Answer | undefined>();
  expectTypeOf<TypeSafeSchema.ChoiceAnswer["choice"]>().toEqualTypeOf<string>();
  expectTypeOf<TypeSafeSchema.ChoiceAnswer["probabilities"][string]>().toEqualTypeOf<
    number | undefined
  >();
  expectTypeOf<TypeSafeSchema.ScoreAnswer["probabilities"][string]>().toEqualTypeOf<
    number | undefined
  >();
  expectTypeOf<TypeSafeSchema.ScoreAnswer["legend"][string]>().toEqualTypeOf<string | undefined>();
  expectTypeOf<Effect.Error<typeof classifyTicket>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<
    Effect.Services<typeof classifyTicket>
  >().toEqualTypeOf<TypeSafeClient.TypeSafeClient>();
  expectTypeOf<Tool.Failure<typeof ClassifyTicket>>().toEqualTypeOf<AiError.AiError>();
  expectTypeOf<
    Tool.HandlerServices<typeof ClassifyTicket>
  >().toEqualTypeOf<TypeSafeClient.TypeSafeClient>();
});

it("infers inline and numeric options and distributes over alternative criteria", () => {
  const selected = <Choice extends string>(answer: TypeSafeSchema.ChoiceAnswer<Choice>): Choice =>
    answer.choice;

  expectTypeOf(selected<"billing" | "technical">).returns.toEqualTypeOf<"billing" | "technical">();

  const inline = Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
    client.evaluate({
      model: "jev-latest",
      state: "hello",
      questions: {
        department: {
          type: "choice",
          instructions: "Route",
          criteria: { billing: null, technical: null },
        },
        numbered: { type: "choice", instructions: "Pick", criteria: { 1: null, 2: null } },
      },
    }),
  );

  expectTypeOf<Effect.Success<typeof inline>["answers"]["department"]["choice"]>().toEqualTypeOf<
    "billing" | "technical"
  >();
  expectTypeOf<Effect.Success<typeof inline>["answers"]["numbered"]["choice"]>().toEqualTypeOf<
    "1" | "2"
  >();
  expectTypeOf<
    Effect.Success<typeof inline>["answers"]["numbered"]["probabilities"]
  >().toEqualTypeOf<{
    readonly "1": number;
    readonly "2": number;
  }>();

  type Alternative = TypeSafeSchema.AnswerFor<{
    readonly type: "choice";
    readonly instructions: string;
    readonly criteria: { readonly billing: null } | { readonly technical: null };
  }>;

  expectTypeOf<Alternative["choice"]>().toEqualTypeOf<"billing" | "technical">();
  expectTypeOf<
    Extract<Alternative, { readonly choice: "billing" }>["probabilities"]
  >().toEqualTypeOf<{ readonly billing: number }>();

  const dynamic = (questions: TypeSafeSchema.Questions) =>
    Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
      client.evaluate({ model: "jev-latest", state: "hello", questions }),
    );

  expectTypeOf<Effect.Success<ReturnType<typeof dynamic>>["answers"][string]>().toEqualTypeOf<
    TypeSafeSchema.Answer | undefined
  >();
});

it("preserves optional criteria in inferred probability fields", () => {
  const criteria: { billing: string | null; technical?: string | null } = { billing: null };

  const evaluation = Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
    client.evaluate({
      model: "jev-latest",
      state: "hello",
      questions: { department: { type: "choice", instructions: "Route", criteria } },
    }),
  );

  type Department = Effect.Success<typeof evaluation>["answers"]["department"];

  expectTypeOf<Department["choice"]>().toEqualTypeOf<"billing" | "technical">();
  expectTypeOf<Department["probabilities"]>().toEqualTypeOf<{
    readonly billing: number;
    readonly technical?: number | undefined;
  }>();
});

it("distinguishes required question keys from optional and open indexes", () => {
  type Pattern = TypeSafeSchema.EvaluateResponse<
    Record<`q_${string}`, TypeSafeSchema.NoulQuestion> & {
      readonly q_required: TypeSafeSchema.NoulQuestion;
      readonly optional?: TypeSafeSchema.ChoiceQuestion;
    }
  >["answers"];
  type Numeric = TypeSafeSchema.EvaluateResponse<
    Record<number, TypeSafeSchema.NoulQuestion> & { readonly 1: TypeSafeSchema.NoulQuestion }
  >["answers"];
  type StringIndex = TypeSafeSchema.EvaluateResponse<
    Record<string, TypeSafeSchema.NoulQuestion> & { readonly required: TypeSafeSchema.NoulQuestion }
  >["answers"];

  expectTypeOf<Pattern["q_required"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer>();
  expectTypeOf<Pattern["q_missing"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer | undefined>();
  expectTypeOf<Pattern["optional"]>().toEqualTypeOf<TypeSafeSchema.ChoiceAnswer | undefined>();
  expectTypeOf<Numeric["1"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer>();
  expectTypeOf<Numeric["2"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer | undefined>();
  expectTypeOf<StringIndex["required"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer>();
  expectTypeOf<StringIndex["missing"]>().toEqualTypeOf<TypeSafeSchema.NoulAnswer | undefined>();
});

it("keeps open numeric and patterned choices broad without promising absent probabilities", () => {
  type Pattern = TypeSafeSchema.AnswerFor<{
    readonly type: "choice";
    readonly instructions: string;
    readonly criteria: Record<`option_${string}`, null> & { readonly option_required: null };
  }>;
  type Numeric = TypeSafeSchema.AnswerFor<{
    readonly type: "choice";
    readonly instructions: string;
    readonly criteria: Record<number, null> & { readonly 1: null };
  }>;

  expectTypeOf<Pattern["choice"]>().toEqualTypeOf<`option_${string}`>();
  expectTypeOf<Pattern["probabilities"]["option_required"]>().toEqualTypeOf<number>();
  expectTypeOf<Pattern["probabilities"]["option_missing"]>().toEqualTypeOf<number | undefined>();
  expectTypeOf<Numeric["choice"]>().toEqualTypeOf<`${number}`>();
  expectTypeOf<Numeric["probabilities"]["1"]>().toEqualTypeOf<number>();
  expectTypeOf<Numeric["probabilities"]["2"]>().toEqualTypeOf<number | undefined>();
  expectTypeOf<
    TypeSafeSchema.ChoiceAnswer<`option_${string}`>["probabilities"]["option_missing"]
  >().toEqualTypeOf<number | undefined>();
});

it("exposes only HttpClient requirements at construction and ConfigError for configuration", () => {
  const make = TypeSafeClient.make({ apiKey: Redacted.make("test") });
  const layer = TypeSafeClient.layer({ apiKey: Redacted.make("test") });
  const configured = TypeSafeClient.layerConfig({ apiKey: Config.Redacted("TYPESAFE_API_KEY") });

  expectTypeOf<Effect.Error<typeof make>>().toEqualTypeOf<never>();
  expectTypeOf<Effect.Services<typeof make>>().toEqualTypeOf<HttpClient.HttpClient>();
  expectTypeOf<Layer.Services<typeof layer>>().toEqualTypeOf<HttpClient.HttpClient>();
  expectTypeOf<Layer.Error<typeof layer>>().toEqualTypeOf<never>();
  expectTypeOf<Layer.Services<typeof configured>>().toEqualTypeOf<HttpClient.HttpClient>();
  expectTypeOf<Layer.Error<typeof configured>>().toEqualTypeOf<Config.ConfigError>();
});
