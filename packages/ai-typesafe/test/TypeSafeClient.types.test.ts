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
