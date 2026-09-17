import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import type { TypeSafeSchema } from "@effect-agent/ai-typesafe";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { assert, describe, expect, it } from "@effect/vitest";
import {
  Cause,
  Config,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Redacted,
  Ref,
  Schema,
} from "effect";
import { TestClock } from "effect/testing";
import {
  Headers,
  HttpClient,
  HttpClientError,
  type HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import { classifyTicket } from "../examples/tool.ts";

const apiKey = "typesafe-test-secret";

const questions = {
  department: {
    type: "choice",
    instructions: ["Route the ticket", { use: "message" }],
    criteria: { billing: null, technical: "Bugs and outages" },
  },
  frustration: {
    type: "score",
    instructions: { question: "How frustrated is the customer?" },
    criteria: ["Calm", "Frustrated", "Very angry"],
  },
  urgent: {
    type: "noul",
    instructions: "Does the customer need immediate help?",
    criteria: { true: "Explicitly time-sensitive" },
  },
} satisfies TypeSafeSchema.Questions;

const request = {
  model: "jev-latest",
  state: { message: "I was charged twice", history: [null, { refunded: false, amount: 49 }] },
  questions,
};

const response = {
  model: "jev-actual-version",
  answers: {
    urgent: { type: "noul", noul: 0.9 },
    frustration: {
      type: "score",
      score: 1.6,
      legend: { "0": "Calm", "1": "Frustrated", "2": "Very angry" },
      probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
      confidence: 0.78,
    },
    department: {
      type: "choice",
      choice: "billing",
      probabilities: { billing: 0.8, technical: 0.2 },
      confidence: 0.7,
    },
  },
  usage: { input_tokens: 312, output_tokens: 48 },
};

const jsonResponse = (request: HttpClientRequest.HttpClientRequest, body: unknown, status = 200) =>
  HttpClientResponse.fromWeb(
    request,
    new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } }),
  );

const clientLayer = (
  handler: Parameters<typeof HttpClient.make>[0],
  options?: Partial<TypeSafeClient.Options>,
) =>
  TypeSafeClient.layer({ apiKey: Redacted.make(apiKey), ...options }).pipe(
    Layer.provide(Layer.succeed(HttpClient.HttpClient, HttpClient.make(handler))),
  );

const evaluate = Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
  client.evaluate(request),
);

const requestBody = (request: HttpClientRequest.HttpClientRequest) =>
  request.body._tag === "Uint8Array"
    ? Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(
        new TextDecoder().decode(request.body.body),
      )
    : Effect.die("Expected an encoded JSON request body");

describe("TypeSafeClient", () => {
  it.effect(
    "sends one mixed evaluation and associates answers by ID, preserving distributions and usage",
    () =>
      Effect.gen(function* () {
        const sent: Array<HttpClientRequest.HttpClientRequest> = [];

        const result = yield* evaluate.pipe(
          Effect.provide(
            clientLayer((request) => {
              sent.push(request);

              return Effect.succeed(jsonResponse(request, response));
            }),
          ),
        );

        expect(sent).toHaveLength(1);
        expect(sent[0].method).toBe("POST");
        expect(sent[0].url).toBe("https://api.typesafe.ai/v1/systemone");
        expect(sent[0].headers).toMatchObject({
          authorization: `Bearer ${apiKey}`,
          "content-type": "application/json",
          accept: "application/json",
        });
        expect(yield* requestBody(sent[0])).toEqual(request);
        expect(result.model).toBe("jev-actual-version");
        expect(result.answers.department).toEqual({
          type: "choice",
          choice: "billing",
          confidence: 0.7,
          probabilities: { billing: 0.8, technical: 0.2 },
        });
        expect(result.answers.frustration).toEqual({
          type: "score",
          score: 1.6,
          confidence: 0.78,
          legend: { "0": "Calm", "1": "Frustrated", "2": "Very angry" },
          probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
        });
        expect(result.answers.urgent).toEqual({ type: "noul", noul: 0.9 });
        expect(result.usage).toEqual({ input_tokens: 312, output_tokens: 48 });
      }),
  );

  it.effect(
    "loads redacted configuration and composes an HTTP policy without sending during acquisition",
    () =>
      Effect.gen(function* () {
        const sent: Array<HttpClientRequest.HttpClientRequest> = [];

        const layer = TypeSafeClient.layerConfig({
          apiUrl: Config.String("TYPESAFE_API_URL"),
          transformClient: HttpClient.mapRequest((request) => ({
            ...request,
            url: `${request.url}?policy=applied`,
          })),
        }).pipe(
          Layer.provide(
            Layer.succeed(
              HttpClient.HttpClient,
              HttpClient.make((request) => {
                sent.push(request);

                return Effect.succeed(jsonResponse(request, response));
              }),
            ),
          ),
          Layer.provide(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({
                TYPESAFE_API_KEY: apiKey,
                TYPESAFE_API_URL: "https://typesafe.example/v1",
              }),
            ),
          ),
        );

        yield* Effect.gen(function* () {
          const client = yield* TypeSafeClient.TypeSafeClient;

          expect(sent).toHaveLength(0);
          yield* client.evaluate(request);
        }).pipe(Effect.provide(layer));

        expect(sent[0].url).toBe("https://typesafe.example/v1/systemone?policy=applied");
        expect(sent[0].headers.authorization).toBe(`Bearer ${apiKey}`);
      }),
  );

  const withAnswer = (id: keyof typeof response.answers, answer: unknown) => ({
    ...response,
    answers: { ...response.answers, [id]: answer },
  });

  it.effect("validates against the sent criteria when the caller later changes a dynamic map", () =>
    Effect.gen(function* () {
      const criteria: Record<string, string | null> = { billing: null, technical: null };

      const result = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        client.evaluate({
          model: "jev-latest",
          state: "hello",
          questions: { department: { type: "choice", instructions: "Route", criteria } },
        }),
      ).pipe(
        Effect.provide(
          clientLayer((request) => {
            delete criteria.billing;
            criteria.sales = null;

            return Effect.succeed(
              jsonResponse(request, {
                model: "jev-latest",
                answers: { department: response.answers.department },
                usage: response.usage,
              }),
            );
          }),
        ),
      );

      expect(result.answers.department.choice).toBe("billing");
      expect(result.answers.department.probabilities).toEqual({ billing: 0.8, technical: 0.2 });
    }),
  );

  it.effect("leaves omitted optional criteria absent from the returned probabilities", () =>
    Effect.gen(function* () {
      const criteria: { billing: string | null; technical?: string | null } = { billing: null };

      const result = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        client.evaluate({
          model: "jev-latest",
          state: "hello",
          questions: { department: { type: "choice", instructions: "Route", criteria } },
        }),
      ).pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(
              jsonResponse(request, {
                model: "jev-latest",
                answers: {
                  department: {
                    type: "choice",
                    choice: "billing",
                    probabilities: { billing: 1 },
                    confidence: 1,
                  },
                },
                usage: { input_tokens: 1, output_tokens: 1 },
              }),
            ),
          ),
        ),
      );

      expect(result.answers.department.probabilities).toEqual({ billing: 1 });
      expect(result.answers.department.probabilities.technical).toBeUndefined();
    }),
  );

  it.effect("returns only submitted entries from numeric question and patterned option maps", () =>
    Effect.gen(function* () {
      const criteria: Record<`team_${string}`, null> = { team_billing: null };
      const question = { type: "choice", instructions: "Route", criteria } as const;
      const questions: Record<number, typeof question> = { 1: question };

      const result = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        client.evaluate({ model: "jev-latest", state: "hello", questions }),
      ).pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(
              jsonResponse(request, {
                model: "jev-latest",
                answers: {
                  "1": {
                    type: "choice",
                    choice: "team_billing",
                    probabilities: { team_billing: 1 },
                    confidence: 1,
                  },
                },
                usage: { input_tokens: 1, output_tokens: 1 },
              }),
            ),
          ),
        ),
      );

      expect(Object.keys(result.answers)).toEqual(["1"]);
      expect(result.answers["2"]).toBeUndefined();
      expect(result.answers["1"]?.probabilities).toEqual({ team_billing: 1 });
      expect(result.answers["1"]?.probabilities.team_missing).toBeUndefined();
    }),
  );

  const choice = response.answers.department;
  const score = response.answers.frustration;

  const malformed: ReadonlyArray<readonly [string, unknown]> = [
    ["missing answer", { ...response, answers: { department: choice, frustration: score } }],
    [
      "unexpected answer ID",
      { ...response, answers: { ...response.answers, other: { type: "noul", noul: 1 } } },
    ],
    ["answer kind differs from its question", withAnswer("department", { type: "noul", noul: 1 })],
    ["choice outside criteria", withAnswer("department", { ...choice, choice: "sales" })],
    [
      "choice is not a most likely option",
      withAnswer("department", { ...choice, choice: "technical" }),
    ],
    [
      "missing required confidence",
      withAnswer("department", {
        type: "choice",
        choice: "billing",
        probabilities: { billing: 0.8, technical: 0.2 },
      }),
    ],
    ["confidence outside [0, 1]", withAnswer("department", { ...choice, confidence: 1.1 })],
    [
      "negative probability",
      withAnswer("department", { ...choice, probabilities: { billing: 1.1, technical: -0.1 } }),
    ],
    [
      "missing option probability",
      withAnswer("department", { ...choice, probabilities: { billing: 1 } }),
    ],
    [
      "unexpected option probability",
      withAnswer("department", {
        ...choice,
        probabilities: { billing: 0.8, technical: 0.1, sales: 0.1 },
      }),
    ],
    [
      "probabilities do not sum to one",
      withAnswer("department", { ...choice, probabilities: { billing: 0.6, technical: 0.2 } }),
    ],
    [
      "high-precision drift is not two-decimal rounding",
      withAnswer("department", {
        ...choice,
        probabilities: { billing: 0.790001, technical: 0.2 },
      }),
    ],
    [
      "rounding does not excuse the wrong winning choice",
      withAnswer("department", {
        ...choice,
        probabilities: { billing: 0.49, technical: 0.5 },
      }),
    ],
    [
      "Choice rounding does not relax Score distributions",
      withAnswer("frustration", {
        ...score,
        score: 1.59,
        probabilities: { "0": 0.05, "1": 0.29, "2": 0.65 },
      }),
    ],
    ["score exceeds the rubric", withAnswer("frustration", { ...score, score: 3 })],
    ["score disagrees with its distribution", withAnswer("frustration", { ...score, score: 0.6 })],
    [
      "legend differs from the submitted rubric",
      withAnswer("frustration", {
        ...score,
        legend: { "0": "Calm", "1": "Very angry", "2": "Frustrated" },
      }),
    ],
    [
      "missing required legend",
      withAnswer("frustration", {
        type: "score",
        score: 1.6,
        probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
        confidence: 0.78,
      }),
    ],
    [
      "probability levels differ from the rubric",
      withAnswer("frustration", { ...score, probabilities: { "1": 0.05, "2": 0.3, "3": 0.65 } }),
    ],
    ["noul outside [0, 1]", withAnswer("urgent", { type: "noul", noul: -0.1 })],
    ["missing required answer type", withAnswer("urgent", { noul: 0.9 })],
    ["missing required usage", { model: response.model, answers: response.answers }],
    ["missing required model", { answers: response.answers, usage: response.usage }],
    ["fractional token count", { ...response, usage: { input_tokens: 0.5, output_tokens: 1 } }],
    ["missing token count", { ...response, usage: { input_tokens: 1 } }],
  ];

  it.effect.each(malformed)("rejects %s through the real response decoder", ([, body]) =>
    Effect.gen(function* () {
      const error = yield* evaluate.pipe(
        Effect.provide(clientLayer((request) => Effect.succeed(jsonResponse(request, body)))),
        Effect.flip,
      );

      expect(error).toMatchObject({
        module: "TypeSafeClient",
        method: "evaluate",
        reason: { _tag: "InvalidOutputError" },
      });
    }),
  );

  it.effect.each([
    "",
    "{",
    '{"model":"jev","answers":{"urgent":{"type":"noul","noul":1e999}},"usage":{"input_tokens":1,"output_tokens":1}}',
  ])("rejects empty, malformed, and non-finite JSON: %s", (body) =>
    Effect.gen(function* () {
      const error = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        client.evaluate({
          model: "jev-latest",
          state: "hello",
          questions: { urgent: questions.urgent },
        }),
      ).pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(HttpClientResponse.fromWeb(request, new Response(body))),
          ),
        ),
        Effect.flip,
      );

      expect(error.reason._tag).toBe("InvalidOutputError");
    }),
  );

  it.effect(
    "accepts tied choices, endpoint probabilities, and small serialization error without normalizing",
    () =>
      Effect.gen(function* () {
        const body = {
          ...response,
          answers: {
            department: {
              type: "choice",
              choice: "technical",
              confidence: 0,
              probabilities: { billing: 0.5, technical: 0.5 },
            },
            frustration: {
              ...response.answers.frustration,
              score: 1.6000001,
              probabilities: { "0": 0.05, "1": 0.3000001, "2": 0.65 },
            },
            urgent: { type: "noul", noul: 1 },
          },
        };

        const result = yield* evaluate.pipe(
          Effect.provide(clientLayer((request) => Effect.succeed(jsonResponse(request, body)))),
        );

        expect(result.answers.department.choice).toBe("technical");
        expect(result.answers.department.confidence).toBe(0);
        expect(result.answers.urgent.noul).toBe(1);
        expect(result.answers.frustration.score).toBe(1.6000001);
        expect(result.answers.frustration.probabilities["1"]).toBe(0.3000001);
      }),
  );

  it.effect("treats prototype-like question and option names as ordinary JSON keys", () =>
    Effect.gen(function* () {
      const result = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        client.evaluate({
          model: "jev-latest",
          state: ["hello"],
          questions: {
            ["__proto__"]: {
              type: "choice",
              instructions: "Pick",
              criteria: { ["__proto__"]: null, constructor: null },
            },
          },
        }),
      ).pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(
              jsonResponse(request, {
                model: "jev-latest",
                usage: { input_tokens: 1, output_tokens: 1 },
                answers: {
                  ["__proto__"]: {
                    type: "choice",
                    choice: "__proto__",
                    probabilities: { ["__proto__"]: 1, constructor: 0 },
                    confidence: 1,
                  },
                },
              }),
            ),
          ),
        ),
      );

      expect(Object.hasOwn(result.answers, "__proto__")).toBe(true);
      expect(result.answers["__proto__"].choice).toBe("__proto__");
      expect(Object.hasOwn(result.answers["__proto__"].probabilities, "__proto__")).toBe(true);
      expect(Object.getPrototypeOf(result.answers)).toBe(Object.prototype);
    }),
  );

  const invalidRequests: ReadonlyArray<readonly [string, unknown]> = [
    ["missing model", { state: "hello", questions }],
    ["missing instructions", { ...request, questions: { urgent: { type: "noul" } } }],
    ["scalar state", { ...request, state: 123 }],
    ["non-JSON state", { ...request, state: { value: Number.NaN } }],
    [
      "one score level",
      { ...request, questions: { rating: { ...questions.frustration, criteria: ["Only one"] } } },
    ],
    [
      "empty choice",
      { ...request, questions: { department: { ...questions.department, criteria: {} } } },
    ],
    [
      "null noul description",
      { ...request, questions: { urgent: { ...questions.urgent, criteria: { true: null } } } },
    ],
  ];

  it.effect.each(invalidRequests)("rejects %s before HTTP dispatch", ([, input]) =>
    Effect.gen(function* () {
      let requests = 0;

      const error = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
        // @ts-expect-error Exercise malformed JavaScript input at the public boundary.
        client.evaluate(input),
      ).pipe(
        Effect.provide(
          clientLayer((request) => {
            requests++;

            return Effect.succeed(jsonResponse(request, response));
          }),
        ),
        Effect.flip,
      );

      expect(error.reason._tag).toBe("InvalidRequestError");
      expect(requests).toBe(0);
    }),
  );

  it.effect.each([
    [401, "AuthenticationError", false],
    [422, "InvalidRequestError", false],
    [429, "RateLimitError", true],
    [529, "InternalProviderError", true],
  ] as const)(
    "maps HTTP %i without implicit retries and preserves redacted diagnostics",
    ([status, tag, retryable]) =>
      Effect.gen(function* () {
        let requests = 0;

        const error = yield* evaluate.pipe(
          Effect.provide(
            clientLayer((request) => {
              requests++;

              return Effect.succeed(
                HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({ detail: `Rejected ${apiKey}`, field: "questions" }),
                    {
                      status,
                      headers: {
                        "x-request-id": "req-123",
                        "set-cookie": "private-session",
                        "retry-after": "2",
                      },
                    },
                  ),
                ),
              );
            }),
          ),
          Effect.flip,
        );

        expect(requests).toBe(1);
        expect(error.reason._tag).toBe(tag);
        expect(error.isRetryable).toBe(retryable);
        expect(error.reason).toMatchObject({
          http: {
            request: {
              method: "POST",
              url: "https://api.typesafe.ai/v1/systemone",
              headers: { authorization: "<redacted>" },
            },
            response: {
              status,
              headers: {
                "x-request-id": "req-123",
                "set-cookie": "<redacted>",
                "retry-after": "2",
              },
            },
            body: '{"detail":"Rejected <redacted>","field":"questions"}',
          },
        });
        expect(JSON.stringify(error)).not.toContain(apiKey);
        expect(error.message).not.toContain(apiKey);
      }),
  );

  it.effect(
    "preserves transport failures as retryable NetworkError with redacted credentials",
    () =>
      Effect.gen(function* () {
        const error = yield* evaluate.pipe(
          Effect.provide(
            clientLayer((request) =>
              Effect.fail(
                new HttpClientError.HttpClientError({
                  reason: new HttpClientError.TransportError({
                    request,
                    description: `Connection reset for ${apiKey}`,
                  }),
                }),
              ),
            ),
          ),
          Effect.provideService(Headers.CurrentRedactedNames, []),
          Effect.flip,
        );

        expect(error.reason).toMatchObject({
          _tag: "NetworkError",
          reason: "TransportError",
          description: "Connection reset for <redacted>",
        });
        expect(error.isRetryable).toBe(true);
        expect(JSON.stringify(error)).not.toContain(apiKey);
      }),
  );

  it.effect("redacts the configured credential from schema diagnostics", () =>
    Effect.gen(function* () {
      const error = yield* evaluate.pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(
              jsonResponse(request, {
                ...response,
                [apiKey]: "unexpected provider field",
              }),
            ),
          ),
        ),
        Effect.flip,
      );

      expect(error.reason._tag).toBe("InvalidOutputError");
      expect(error.message).toContain("<redacted>");
      expect(JSON.stringify(error)).not.toContain(apiKey);
    }),
  );

  it.effect("preserves defects and runs transport finalizers", () =>
    Effect.gen(function* () {
      const defect = new Error("transport defect");
      const released = yield* Ref.make(0);

      const exit = yield* evaluate.pipe(
        Effect.provide(
          clientLayer(() =>
            Effect.die(defect).pipe(Effect.ensuring(Ref.update(released, (n) => n + 1))),
          ),
        ),
        Effect.exit,
      );

      assert(Exit.isFailure(exit));
      expect(Cause.squash(exit.cause)).toBe(defect);
      expect(yield* Ref.get(released)).toBe(1);
    }),
  );

  it.effect("an external timeout aborts HTTP and waits for transport finalization", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<AbortSignal>();
      const released = yield* Ref.make(0);

      const fiber = yield* evaluate.pipe(
        Effect.provide(
          clientLayer((_, _url, signal) =>
            Deferred.succeed(started, signal).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Ref.update(released, (n) => n + 1)),
            ),
          ),
        ),
        Effect.timeout("1 second"),
        Effect.forkChild,
      );

      const signal = yield* Deferred.await(started);

      yield* TestClock.adjust("1 second");
      const error = yield* Fiber.join(fiber).pipe(Effect.flip);

      expect(error._tag).toBe("TimeoutError");
      expect(signal.aborted).toBe(true);
      expect(yield* Ref.get(released)).toBe(1);
    }),
  );

  it.effect("interruption during body reading remains interruption and finalizes the reader", () =>
    Effect.gen(function* () {
      const reading = yield* Deferred.make<AbortSignal>();
      const released = yield* Ref.make(0);

      const fiber = yield* evaluate.pipe(
        Effect.provide(
          clientLayer((request, _url, signal) => {
            const result = jsonResponse(request, response);

            Object.defineProperty(result, "json", {
              value: Deferred.succeed(reading, signal).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Ref.update(released, (n) => n + 1)),
              ),
            });

            return Effect.succeed(result);
          }),
        ),
        Effect.forkChild,
      );

      const signal = yield* Deferred.await(reading);

      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      assert(Exit.isFailure(exit));
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true);
      expect(signal.aborted).toBe(true);
      expect(yield* Ref.get(released)).toBe(1);
    }),
  );

  it.effect("executes the native Toolkit example through the real TypeSafe client", () =>
    Effect.gen(function* () {
      const results = yield* classifyTicket.pipe(
        Effect.provide(
          clientLayer((request) =>
            Effect.succeed(
              jsonResponse(request, {
                model: "jev-latest",
                answers: { department: response.answers.department },
                usage: { input_tokens: 15, output_tokens: 2 },
              }),
            ),
          ),
        ),
      );

      expect(results).toHaveLength(1);
      expect(results[0]).toMatchObject({
        isFailure: false,
        result: { department: "billing", confidence: 0.7 },
      });
    }),
  );
});

it.effect.each([
  // Captured Jev 1.13.0 distribution, with option names replaced by fixture names.
  ["captured 0.99 total", { a: 0.27, b: 0.67, c: 0, d: 0.03, e: 0.01, f: 0.01, g: 0 }],
  ["rounded 1.01 total", { a: 0.33, b: 0.34, c: 0.34, d: 0, e: 0, f: 0, g: 0 }],
] as const)("preserves %s through the client and DecisionModel", ([, probabilities]) =>
  Effect.gen(function* () {
    const questions = {
      route: {
        type: "choice",
        instructions: "Pick a route",
        criteria: { a: null, b: null, c: null, d: null, e: null, f: null, g: null },
      },
    } satisfies TypeSafeSchema.Questions;

    const answer = { type: "choice", choice: "b", probabilities, confidence: 0.61 };

    const ClientLive = clientLayer((request) =>
      Effect.succeed(
        jsonResponse(request, {
          model: "jev-1.13.0",
          answers: { route: answer },
          usage: { input_tokens: 10, output_tokens: 2 },
        }),
      ),
    );

    const direct = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
      client.evaluate({ model: "jev-latest", state: "request", questions }),
    ).pipe(Effect.provide(ClientLive));

    const shared = yield* Effect.flatMap(DecisionModel.DecisionModel, (model) =>
      model.evaluate({ state: "request", questions }),
    ).pipe(
      Effect.provide(TypeSafeDecisionModel.model("jev-latest").pipe(Layer.provide(ClientLive))),
    );

    expect(direct.answers.route).toEqual(answer);
    expect(shared.answers.route).toEqual({ type: "choice", choice: "b", probabilities });
  }),
);

it.effect.each([
  ["a single option cannot round to 0.99", [0.99]],
  ["seven options cannot widen the cap to 0.02", [0.67, 0.27, 0, 0.02, 0.01, 0.01, 0]],
  // An uncapped n * 0.005 allowance would accept an all-zero 200-option distribution.
  [
    "a large catalogue cannot round away all probability mass",
    Array.from({ length: 200 }, () => 0),
  ],
] as const)("rejects invalid rounded distributions: %s", ([, values]) =>
  Effect.gen(function* () {
    const criteria = Object.fromEntries(values.map((_, i) => [String(i), null]));
    const probabilities = Object.fromEntries(values.map((value, i) => [String(i), value]));

    const error = yield* Effect.flatMap(TypeSafeClient.TypeSafeClient, (client) =>
      client.evaluate({
        model: "jev-latest",
        state: "request",
        questions: { route: { type: "choice", instructions: "Pick a route", criteria } },
      }),
    ).pipe(
      Effect.provide(
        clientLayer((request) =>
          Effect.succeed(
            jsonResponse(request, {
              model: "jev-1.13.0",
              answers: {
                route: { type: "choice", choice: "0", probabilities, confidence: 0.5 },
              },
              usage: { input_tokens: 10, output_tokens: 2 },
            }),
          ),
        ),
      ),
      Effect.flip,
    );

    expect(error.reason._tag).toBe("InvalidOutputError");
  }),
);

it.effect("evaluates a reusable mixed decision set through the real TypeSafe HTTP boundary", () =>
  Effect.gen(function* () {
    const model = yield* DecisionModel.DecisionModel;

    const assessment = DecisionSet.make({
      input: Schema.Struct({ message: Schema.String, history: Schema.Array(Schema.Json) }),
      questions: {
        department: DecisionQuery.choice({
          instructions: questions.department.instructions,
          options: questions.department.criteria,
        }),
        frustration: DecisionQuery.score({
          instructions: questions.frustration.instructions,
          levels: questions.frustration.criteria,
        }),
        urgent: DecisionQuery.probability(questions.urgent),
      },
    });

    const result = yield* model.evaluate(assessment, request.state);

    expect(result).toEqual({
      provider: "typesafe",
      model: response.model,
      usage: { inputTokens: 312, outputTokens: 48 },
      answers: {
        department: {
          type: "choice",
          choice: "billing",
          probabilities: { billing: 0.8, technical: 0.2 },
        },
        frustration: {
          type: "score",
          score: 1.6,
          legend: { "0": "Calm", "1": "Frustrated", "2": "Very angry" },
          probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
        },
        urgent: { type: "probability", probability: 0.9 },
      },
      providerMetadata: { typesafe: { confidence: { department: 0.7, frustration: 0.78 } } },
    });

    const metadata = yield* Schema.decodeUnknownEffect(TypeSafeDecisionModel.ProviderMetadata)(
      result.providerMetadata?.typesafe,
    );

    expect(metadata.confidence).toEqual({ department: 0.7, frustration: 0.78 });
  }).pipe(
    Effect.provide(
      TypeSafeDecisionModel.model("jev-latest").pipe(
        Layer.provide(
          clientLayer((sent) =>
            Effect.gen(function* () {
              expect(yield* requestBody(sent).pipe(Effect.orDie)).toEqual(request);

              return jsonResponse(sent, response);
            }),
          ),
        ),
      ),
    ),
  ),
);

it.effect("preserves reserved question IDs through the decision adapter", () =>
  Effect.gen(function* () {
    const model = yield* DecisionModel.DecisionModel;

    const result = yield* model.evaluate({
      state: "go",
      questions: {
        ["__proto__"]: { type: "probability", instructions: "Relevant?" },
      },
    });

    expect(Object.hasOwn(result.answers, "__proto__")).toBe(true);
    expect(result.answers["__proto__"]).toEqual({ type: "probability", probability: 0.8 });
  }).pipe(
    Effect.provide(
      TypeSafeDecisionModel.model("jev-latest").pipe(
        Layer.provide(
          clientLayer((sent) =>
            Effect.succeed(
              jsonResponse(sent, {
                model: "jev-resolved",
                usage: { input_tokens: 1, output_tokens: 1 },
                answers: { ["__proto__"]: { type: "noul", noul: 0.8 } },
              }),
            ),
          ),
        ),
      ),
    ),
  ),
);
