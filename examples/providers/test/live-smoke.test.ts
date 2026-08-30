import { Redactor, StructuralRedactorLive } from "@effect-agent/capabilities";
import { RunEvent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  PHASE7_LIVE_CREDENTIAL_ENV,
  PHASE7_LIVE_GATE_ENV,
  phase1Trip,
  phase7LiveProfileEnabled,
  phase7TravelPlannerProfile,
  TravelPlan,
  TravelPlannerPhase7Profile,
  TravelPlannerRuntimeLayer,
} from "@effect-agent/testing/fixtures/travel-planner";
import { OpenAiClient } from "@effect/ai-openai";
import { Config, Effect, Layer, Schema, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { describe, expect, it } from "vite-plus/test";

import { OpenAiTravelPlanner } from "../src/index.ts";

// ---------------------------------------------------------------------------
// The P7 test-side live-gating pattern (plan §6): ONE environment predicate,
// exported by the fixture, applied with `describe.skipIf`. The ungated
// describe below runs on every ordinary `bun run test`; the live describe is
// skipped unless BOTH the explicit opt-in flag and the credential are present,
// so no ordinary gate ever makes a network request.
// ---------------------------------------------------------------------------

const liveEnabled = phase7LiveProfileEnabled(process.env);

describe("TEST-014 P7 Travel Planner dual-profile pin", () => {
  it("pins the deterministic offline conformance profile alongside the opt-in live-model profile", () => {
    const decoded = Schema.decodeUnknownSync(TravelPlannerPhase7Profile)(
      Schema.encodeSync(TravelPlannerPhase7Profile)(phase7TravelPlannerProfile),
    );
    expect(decoded).toEqual(phase7TravelPlannerProfile);
    expect(phase7TravelPlannerProfile).toEqual({
      phase: "P7",
      offlineConformanceDeterministic: true,
      offlineRequiresCredentials: false,
      liveProfileOptIn: true,
      // Live profiles bind live MODEL Layers over the SAME deterministic
      // suppliers; "selected supplier Layers" is honestly scoped to
      // live-model-only (P7 plan decision 9).
      liveModelLayers: true,
      liveSupplierLayers: false,
      structurallyRedactedTranscripts: true,
      exactlyOnceExternalEffects: false,
    });
  });

  it("enables the live profile only for EFFECT_AGENT_LIVE=1 plus a present credential", () => {
    expect(PHASE7_LIVE_GATE_ENV).toBe("EFFECT_AGENT_LIVE");
    expect(PHASE7_LIVE_CREDENTIAL_ENV).toBe("OPENAI_API_KEY");
    expect(phase7LiveProfileEnabled({})).toBe(false);
    expect(phase7LiveProfileEnabled({ EFFECT_AGENT_LIVE: "1" })).toBe(false);
    expect(phase7LiveProfileEnabled({ EFFECT_AGENT_LIVE: "true", OPENAI_API_KEY: "k" })).toBe(
      false,
    );
    expect(phase7LiveProfileEnabled({ OPENAI_API_KEY: "k" })).toBe(false);
    expect(phase7LiveProfileEnabled({ EFFECT_AGENT_LIVE: "1", OPENAI_API_KEY: "" })).toBe(false);
    expect(phase7LiveProfileEnabled({ EFFECT_AGENT_LIVE: "1", OPENAI_API_KEY: "k" })).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Opt-in live smoke (class E, one Run, rate-limited by construction): the
// existing `OpenAiTravelPlanner` binding over the application-supplied OpenAI
// client Layer and the SAME deterministic supplier/travel-service Layers the
// offline suites use. Transcript evidence never leaves this test in raw form:
// every RunEvent is Schema-encoded and passed through the structural Redactor
// before it can appear in an assertion message (SEC-008, testing.md §12).
// ---------------------------------------------------------------------------

const OpenAiClientLayer = OpenAiClient.layerConfig({
  apiKey: Config.redacted(PHASE7_LIVE_CREDENTIAL_ENV),
}).pipe(Layer.provide(FetchHttpClient.layer));

const liveSmokeLayer = Layer.mergeAll(
  TravelPlannerRuntimeLayer,
  OpenAiClientLayer,
  StructuralRedactorLive,
);

const encodeRunEvent = Schema.encodeEffect(RunEvent);

describe.skipIf(!liveEnabled)("TEST-014 P7 Travel Planner live-model smoke (opt-in)", () => {
  it(
    "completes one live planning Run over deterministic suppliers and emits only structurally redacted transcript evidence",
    { timeout: 120_000 },
    () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const events = yield* AgentRuntime.stream(OpenAiTravelPlanner, phase1Trip).pipe(
            Stream.runCollect,
          );

          // The redacted transcript is the ONLY transcript representation this
          // test materializes: shape and event order survive, every scalar the
          // live model produced is replaced by its type marker.
          const redactor = yield* Redactor;
          const redactedTranscript: Array<string> = [];
          for (const event of events) {
            const encoded = yield* encodeRunEvent(event);
            redactedTranscript.push(`${event._tag} ${yield* redactor.redact(encoded)}`);
          }
          const transcriptText = redactedTranscript.join("\n");

          // Exactly one successful settlement whose output decodes through the
          // SAME output Schema the offline conformance profile enforces.
          const completed = events.filter((event) => event._tag === "RunCompleted");
          expect(completed).toHaveLength(1);
          const plan = yield* Schema.decodeUnknownEffect(TravelPlan)(completed[0]?.output);
          expect(plan.itineraries.length).toBeGreaterThan(0);

          // Structural redaction holds: no live-model string scalar survives
          // into the transcript evidence, only bounded type markers.
          expect(transcriptText).toContain("[REDACTED:");
          for (const itinerary of plan.itineraries) {
            for (const probe of [itinerary.title, itinerary.flight, itinerary.lodging]) {
              if (probe.length > 0) {
                expect(transcriptText).not.toContain(probe);
              }
            }
          }

          // The live Run exercised the deterministic supplier Layers (the
          // supplier side stays offline by design — decision 9).
          const toolResults = events.filter((event) => event._tag === "ToolCallSucceeded");
          expect(toolResults.length).toBeGreaterThan(0);
        }).pipe(Effect.scoped, Effect.provide(liveSmokeLayer)),
      ),
  );
});
