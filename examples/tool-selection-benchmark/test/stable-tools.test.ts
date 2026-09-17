import { OpenAiClient, OpenAiLanguageModel, type OpenAiSchema } from "@effect/ai-openai";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Redacted, Stream } from "effect";
import { ToolDiscovery } from "effect-agent";
import { LanguageModel, Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import { tools } from "../src/fixture.ts";
import { refuse } from "../src/measurement.ts";
import { encodeTools, makeAvailabilityNotes, withStableTools } from "../src/stable-tools.ts";

describe("stable catalogue experiment", () => {
  it.effect(
    "preserves availability notes as history grows and refuses rewritten source history",
    () =>
      Effect.gen(function* () {
        const annotate = makeAvailabilityNotes();
        const input = [{ role: "user", content: "Read the invoice." }] as const;
        const first = yield* annotate({ input }, ["discover_tools"]);

        const extended = [
          ...input,
          { type: "function_call", name: "discover_tools", call_id: "find", arguments: "{}" },
          { type: "function_call_output", call_id: "find", output: "get_invoice is now available" },
        ] as const;

        const second = yield* annotate({ input: extended }, ["get_invoice", "discover_tools"]);
        const repeated = yield* annotate({ input: extended }, ["get_invoice", "discover_tools"]);

        expect(first.input).toHaveLength(2);
        expect(second.input).toHaveLength(5);
        expect(second.input?.slice(0, 2)).toEqual(first.input);
        expect(repeated.input).toEqual(second.input);
        expect(JSON.stringify(second.input)).toContain(
          "currently callable functions are get_invoice, discover_tools",
        );
        expect(
          Exit.isFailure(yield* annotate({ input }, ["discover_tools"]).pipe(Effect.exit)),
        ).toBe(true);
        expect(
          Exit.isFailure(yield* makeAvailabilityNotes()({ input: [] }, []).pipe(Effect.exit)),
        ).toBe(true);
      }),
  );

  it.effect("matches real OpenAI schema conversion and restricts the callable subset", () =>
    Effect.gen(function* () {
      const discovery = yield* ToolDiscovery.fromDecisionModel();
      const full = yield* encodeTools([...tools, discovery.tool]);
      const native = yield* OpenAiClient.OpenAiClient;
      const captured: Array<typeof OpenAiSchema.CreateResponse.Encoded> = [];

      const client = OpenAiClient.OpenAiClient.of({
        ...native,
        createResponseStream: Effect.fnUntraced(function* (payload) {
          captured.push(yield* withStableTools(payload, full));

          return yield* refuse("Captured before network I/O");
        }),
      });

      yield* LanguageModel.streamText({
        prompt: "Read the shipping record.",
        toolkit: Toolkit.make(tools[1]!, discovery.tool),
        disableToolCallResolution: true,
      }).pipe(
        Stream.runDrain,
        Effect.provide(OpenAiLanguageModel.layer({ model: "gpt-6-astra" })),
        Effect.provideService(OpenAiClient.OpenAiClient, client),
        Effect.exit,
      );
      expect(captured).toHaveLength(1);
      expect(captured[0]!.tools).toEqual(full);
      expect(captured[0]!.tool_choice).toEqual({
        type: "allowed_tools",
        mode: "auto",
        tools: [
          { type: "function", name: "get_order_shipping" },
          { type: "function", name: "discover_tools" },
        ],
      });
    }).pipe(
      Effect.provide(
        OpenAiClient.layer({ apiKey: Redacted.make("unused-test-key") }).pipe(
          Layer.provide(FetchHttpClient.layer),
        ),
      ),
    ),
  );

  it.effect(
    "keeps definitions byte-identical through subset changes and final no-tool requests",
    () =>
      Effect.gen(function* () {
        const full = yield* encodeTools(tools);

        for (const index of [1, 36, 1]) {
          const result = yield* withStableTools(
            { tools: [full[index]!], tool_choice: "required" },
            full,
          );

          expect(JSON.stringify(result.tools)).toBe(JSON.stringify(full));
          expect(result.tool_choice).toEqual({
            type: "allowed_tools",
            mode: "required",
            tools: [{ type: "function", name: tools[index]!.name }],
          });
        }
        expect((yield* withStableTools({}, full)).tool_choice).toBe("none");
        expect(
          (yield* withStableTools({ tools: full, tool_choice: "none" }, full)).tool_choice,
        ).toBe("none");
        const forced = { type: "function", name: "get_order_shipping" } as const;

        expect(
          (yield* withStableTools({ tools: [full[1]!], tool_choice: forced }, full)).tool_choice,
        ).toEqual(forced);
      }),
  );

  it.effect("rejects catalogue drift, unknown names and unsupported or unavailable choices", () =>
    Effect.gen(function* () {
      const full = yield* encodeTools(tools);

      const cases: ReadonlyArray<typeof OpenAiSchema.CreateResponse.Encoded> = [
        { tools: [{ type: "function", name: "unknown" }] },
        { tools: [{ type: "function", name: "get_order_shipping", parameters: {} }] },
        { tools: [full[0]!, full[0]!] },
        { tools: [full[0]!], tool_choice: { type: "function", name: "get_order_shipping" } },
        { tools: [full[0]!], tool_choice: { type: "web_search" } },
        { tools: [], tool_choice: "required" },
      ];

      for (const payload of cases) {
        expect(Exit.isFailure(yield* withStableTools(payload, full).pipe(Effect.exit))).toBe(true);
      }
      expect(
        Exit.isFailure(yield* withStableTools({}, [full[0]!, full[0]!]).pipe(Effect.exit)),
      ).toBe(true);
    }),
  );
});
