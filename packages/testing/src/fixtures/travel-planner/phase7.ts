/** Explicit opt-in switch for live integration tests. */
export const PHASE7_LIVE_GATE_ENV = "EFFECT_AGENT_LIVE";

/** The credential required by the Travel Planner live-model tests. */
export const PHASE7_LIVE_CREDENTIAL_ENV = "OPENAI_API_KEY";

/** Structural shape of an environment without importing Node types. */
export interface Phase7LiveGateEnvironment {
  readonly [name: string]: string | undefined;
}

/** Enable live tests only when both the opt-in flag and credential are present. */
export const phase7LiveProfileEnabled = (env: Phase7LiveGateEnvironment): boolean =>
  env[PHASE7_LIVE_GATE_ENV] === "1" && (env[PHASE7_LIVE_CREDENTIAL_ENV] ?? "") !== "";
