import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Phase 7 (P7): the Travel Planner as an INTERNAL agent with two explicit
// profiles (ROADMAP P7 exit gate: "Travel Planner ... retains a deterministic
// offline conformance profile alongside its live integration profiles").
//
// - The OFFLINE profile is the cumulative P1–P6/S1/S2 suite set that already
//   exists: scripted `LanguageModel`, deterministic supplier Layers,
//   controllable time and IDs, no network, no credentials. Nothing in P7
//   weakens it; this module only pins the claim as a Schema value so evidence
//   can cite one committed fact instead of prose.
// - The LIVE profile is opt-in and test-side: a suite gates itself with
//   `describe.skipIf(...)` on the environment predicate below, so ordinary
//   `bun run test` never makes a network request and never needs a credential.
//   Live profiles bind live MODEL Layers only. No real travel supplier exists
//   to integrate, so the deterministic supplier desk is retained deliberately
//   and `liveSupplierLayers` is pinned `false` — the roadmap's "selected
//   supplier Layers" is honestly scoped to live-model-only (P7 plan decision
//   9), recorded here rather than silently claimed.
// ---------------------------------------------------------------------------

/**
 * The P7 dual-profile claim, schema-first so the exact scope of "live
 * integration profiles" is a committed, decodable value:
 *
 * - `offlineConformanceDeterministic` / `offlineRequiresCredentials`: the
 *   cumulative conformance suites stay deterministic and credential-free.
 * - `liveProfileOptIn`: live suites are excluded from ordinary gates by the
 *   environment predicate (`phase7LiveProfileEnabled`), never by test-runner
 *   configuration that could silently drift.
 * - `liveModelLayers` / `liveSupplierLayers`: live profiles exercise real
 *   model Layers over the SAME deterministic supplier desk — no claim of a
 *   live supplier integration is made anywhere (decision 9).
 * - `structurallyRedactedTranscripts`: transcript evidence a live profile
 *   emits passes through the structural `Redactor` first (SEC-008,
 *   testing.md §12: "live model and supplier profiles are opt-in smoke or
 *   release tests, rate-limited and structurally redacted").
 * - `exactlyOnceExternalEffects`: never claimed at any phase (DUR-003).
 */
export class TravelPlannerPhase7Profile extends Schema.Class<TravelPlannerPhase7Profile>(
  "@effect-agent/testing/travel-planner/TravelPlannerPhase7Profile",
)({
  phase: Schema.Literal("P7"),
  offlineConformanceDeterministic: Schema.Literal(true),
  offlineRequiresCredentials: Schema.Literal(false),
  liveProfileOptIn: Schema.Literal(true),
  liveModelLayers: Schema.Literal(true),
  liveSupplierLayers: Schema.Literal(false),
  structurallyRedactedTranscripts: Schema.Literal(true),
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const phase7TravelPlannerProfile = TravelPlannerPhase7Profile.make({
  phase: "P7",
  offlineConformanceDeterministic: true,
  offlineRequiresCredentials: false,
  liveProfileOptIn: true,
  liveModelLayers: true,
  liveSupplierLayers: false,
  structurallyRedactedTranscripts: true,
  exactlyOnceExternalEffects: false,
});

/**
 * The one opt-in switch for EVERY live profile in this repository. `"1"` is
 * the only enabling value: an unset, empty, or differently-truthy value keeps
 * the suite skipped, so CI and ordinary developer runs stay offline.
 */
export const PHASE7_LIVE_GATE_ENV = "EFFECT_AGENT_LIVE";

/** The credential a Travel Planner live-model profile additionally requires. */
export const PHASE7_LIVE_CREDENTIAL_ENV = "OPENAI_API_KEY";

/** Structural shape of `process.env` without importing Node types here. */
export interface Phase7LiveGateEnvironment {
  readonly [name: string]: string | undefined;
}

/**
 * The test-side live gate (P7 plan §6: no test-side live-gating pattern
 * existed before this — the demo gates at serve time via
 * `Config.redacted("OPENAI_API_KEY")`). Suites use it as
 * `describe.skipIf(!phase7LiveProfileEnabled(process.env))`, which keeps the
 * live block out of ordinary gates while the SAME file's ungated tests keep
 * pinning the profile schema on every run.
 */
export const phase7LiveProfileEnabled = (env: Phase7LiveGateEnvironment): boolean =>
  env[PHASE7_LIVE_GATE_ENV] === "1" && (env[PHASE7_LIVE_CREDENTIAL_ENV] ?? "") !== "";

// ---------------------------------------------------------------------------
// Authoring friction note (WP7 input; real observations from wiring the P7
// live profile onto the existing Travel Planner):
//
// 1. There was no framework-owned place to put a test-side live gate: every
//    earlier profile was either always-deterministic or gated at serve time
//    inside an application. The predicate had to be invented here as a plain
//    exported function because `packages/testing/src` must stay
//    platform-neutral and cannot read `process.env` itself — fine, but the
//    split (fixture exports the predicate, the example test applies it to
//    `process.env`) is a convention a future author has to discover by
//    reading this file rather than a typed seam.
// 2. Capturing a structurally redacted transcript from a live Run takes
//    manual assembly: collect `RunEvent`s, `Schema.encode` each one, pass the
//    encoded value through `Redactor.redact`. Nothing composes those three
//    steps, and nothing type-level stops a test from logging the RAW event by
//    accident. A `redactedTranscript(events)` helper in capabilities (or an
//    engine stream combinator) would make the safe path the short path.
// 3. Binding a live model to the existing definition was pleasantly trivial
//    (`Agent.withModel` + the upstream client Layer): no friction to report
//    on the authoring surface itself for plain ephemeral runs.
// ---------------------------------------------------------------------------
