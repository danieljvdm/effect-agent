// Deterministic test helpers: in-memory adapters for both ports and
// prompt-keyed scripted models that walk the real tool surfaces. Everything
// here runs with no network and no credentials, so consumers can test their
// adaptations — guidance, ignore globs, extra tools, custom ports — on every
// ordinary gate.
export * from "./internal/fan-out-scripted.ts";
export * from "./internal/fixtures.ts";
export * from "./internal/scripted.ts";
