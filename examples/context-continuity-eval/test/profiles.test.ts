import { expect, it } from "vite-plus/test";

import { productionCostPlan, MAX_INPUT_TOKENS } from "../src/live-model.ts";
import { manifestPage } from "../src/pressure.ts";
import { DEFAULT_PROFILE, profilePlan } from "../src/profiles.ts";

it("keeps automatic coverage reduced and full-capacity preparation explicitly unexecuted", () => {
  const reduced = profilePlan(DEFAULT_PROFILE, 200_000);
  const full = profilePlan("production-capacity-v1", 200_000);

  expect(reduced).toMatchObject({
    contextTokenLimit: 16_000,
    trigger: "requested",
    recovery: "service-reacquisition",
    liveEnabled: true,
  });
  expect(full).toMatchObject({
    contextTokenLimit: 200_000,
    liveEnabled: false,
    requiredCommittedWindows: 12,
    manifestPageCharacters: 400_000,
    maxCostMicrousd: 10_000_000,
  });
  expect(MAX_INPUT_TOKENS).toBe(32_000);
  expect(
    productionCostPlan(200_000).models.find((m) => m.model === "gpt-6-astra")?.inputOnlyMicrousd,
  ).toBe(24_000_000);
  expect(manifestPage(1, 0, 200_000).length).toBe(400_000);
});
