import { Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { PlannerSettings, defaultPlannerSettings } from "../src/domain.ts";
import { changeSettingsAtom, sessionAtom, settingsAtom, settingsStatusAtom } from "../src/state.ts";

const owner = "danieljmerwe@gmail.com";
const guest = "guest@example.com";

const Packet = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.Literals(["GetPlannerSettings", "SavePlannerSettings"]),
  payload: Schema.Unknown,
});

const fetchMock = vi.fn<typeof fetch>();

type PreferenceRequest = {
  readonly tag: "GetPlannerSettings" | "SavePlannerSettings";
  readonly email: string;
  readonly settings: PlannerSettings | null;
  readonly succeed: (value?: PlannerSettings) => void;
  readonly fail: () => void;
};

const setup = (initial: PlannerSettings) => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let email = owner;

  const persisted = new Map<string, PlannerSettings>([
    [owner, initial],
    [guest, defaultPlannerSettings],
  ]);

  const requests: PreferenceRequest[] = [];

  vi.stubGlobal(
    "fetch",
    fetchMock.mockImplementation(async (input, init) => {
      const request = new Request(
        input instanceof Request ? input : new URL(String(input), "https://planner.test"),
        init,
      );

      const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
        (await request.text()).trim(),
      );

      const response = (exit: unknown) =>
        new Response(`${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit })}\n`, {
          headers: { "content-type": "application/ndjson" },
        });

      const requestEmail = email;
      const tag = packet.tag;

      const settings =
        packet.tag === "SavePlannerSettings"
          ? Schema.decodeUnknownSync(PlannerSettings)(packet.payload)
          : null;

      return new Promise<Response>((resolve) => {
        requests.push({
          tag,
          email: requestEmail,
          settings,
          succeed: (value) => {
            if (settings !== null) persisted.set(requestEmail, settings);
            resolve(
              response({
                _tag: "Success",
                value: value ?? persisted.get(requestEmail) ?? defaultPlannerSettings,
              }),
            );
          },
          fail: () =>
            resolve(
              response({
                _tag: "Failure",
                cause: [
                  {
                    _tag: "Fail",
                    error: {
                      _tag: "PlannerError",
                      code: "storage",
                      message: "Storage unavailable",
                    },
                  },
                ],
              }),
            ),
        });
      });
    }),
  );

  return {
    requests,
    persisted,
    createDevice: () => {
      const registry = AtomRegistry.make({ defaultIdleTTL: 0, timeoutResolution: 1 });

      registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));

      const unmounts = [
        registry.mount(settingsAtom),
        registry.mount(settingsStatusAtom),
        registry.mount(changeSettingsAtom),
      ];

      return {
        registry,
        setEmail: (value: string) => {
          email = value;
          registry.set(sessionAtom, AsyncResult.success({ subjectId: email, displayName: email }));
        },
        close: () => {
          for (const unmount of unmounts) unmount();
          registry.dispose();
        },
      };
    },
  };
};

const flush = () => vi.advanceTimersByTimeAsync(1);

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("waits for account hydration before applying an early edit without persisting presentation defaults", async () => {
  const stored: PlannerSettings = { model: "gpt-5.6-luna", reasoningEffort: "none", fast: true };
  const fixture = setup(stored);
  const device = fixture.createDevice();

  try {
    await flush();
    expect(fixture.requests.map(({ tag }) => tag)).toEqual(["GetPlannerSettings"]);
    expect(device.registry.get(settingsStatusAtom).loading).toBe(true);
    device.registry.set(changeSettingsAtom, { kind: "model", value: "gpt-6-astra" });
    await flush();
    expect(fixture.requests).toHaveLength(1);
    fixture.requests[0]!.succeed();
    await flush();
    const expected: PlannerSettings = { model: "gpt-6-astra", reasoningEffort: "low", fast: true };

    expect(fixture.requests[1]).toMatchObject({
      tag: "SavePlannerSettings",
      email: owner,
      settings: expected,
    });
    expect(device.registry.get(settingsAtom)).toEqual(expected);
    expect(device.registry.get(settingsStatusAtom)).toEqual({
      loading: false,
      saving: true,
      error: null,
    });
    fixture.requests[1]!.succeed();
    await flush();
    expect(fixture.persisted.get(owner)).toEqual(expected);
    expect(device.registry.get(settingsStatusAtom).saving).toBe(false);
  } finally {
    device.close();
  }
});

it("serializes rapid choices, ignores stale hydration, reports failed saves, and restores saved choices on another device", async () => {
  const initial: PlannerSettings = { model: "gpt-6-astra", reasoningEffort: "low", fast: false };
  const fixture = setup(initial);
  const device = fixture.createDevice();

  try {
    await flush();
    fixture.requests[0]!.succeed();
    await flush();
    device.registry.set(changeSettingsAtom, { kind: "reasoning", value: "high" });
    device.registry.set(changeSettingsAtom, { kind: "speed" });
    await flush();
    const firstSave = fixture.requests.filter(({ tag }) => tag === "SavePlannerSettings");

    expect(firstSave).toHaveLength(1);
    expect(firstSave[0]?.settings).toEqual({ ...initial, reasoningEffort: "high" });
    firstSave[0]!.succeed();
    await flush();
    const saves = fixture.requests.filter(({ tag }) => tag === "SavePlannerSettings");
    const latest: PlannerSettings = { model: "gpt-6-astra", reasoningEffort: "high", fast: true };

    expect(saves).toHaveLength(2);
    expect(saves[1]?.settings).toEqual(latest);
    const staleRead = fixture.requests.filter(({ tag }) => tag === "GetPlannerSettings")[1];

    expect(staleRead).toBeDefined();
    staleRead!.succeed(initial);
    await flush();
    expect(device.registry.get(settingsAtom)).toEqual(latest);
    saves[1]!.fail();
    await flush();
    expect(device.registry.get(settingsStatusAtom)).toMatchObject({
      loading: false,
      saving: false,
    });
    expect(device.registry.get(settingsStatusAtom).error).toContain("Couldn't save");
    expect(device.registry.get(settingsAtom)).toEqual(latest);
    device.registry.set(changeSettingsAtom, { kind: "reasoning", value: "high" });
    await flush();
    const retry = fixture.requests.filter(({ tag }) => tag === "SavePlannerSettings")[2];

    expect(retry?.settings).toEqual(latest);
    retry!.succeed();
    await flush();
    fixture.requests
      .filter(({ tag }) => tag === "GetPlannerSettings")
      .at(-1)!
      .succeed();
    await flush();
    expect(device.registry.get(settingsStatusAtom).error).toBeNull();
    device.close();
    const otherDevice = fixture.createDevice();

    try {
      await flush();
      expect(otherDevice.registry.get(settingsStatusAtom).loading).toBe(true);
      fixture.requests.at(-1)!.succeed();
      await flush();
      expect(otherDevice.registry.get(settingsAtom)).toEqual(latest);
      expect(otherDevice.registry.get(settingsStatusAtom)).toEqual({
        loading: false,
        saving: false,
        error: null,
      });
    } finally {
      otherDevice.close();
    }
  } finally {
    device.close();
  }
});

it("does not transfer a waiting account edit or cached preferences to another verified identity", async () => {
  const fixture = setup({ model: "gpt-6-astra", reasoningEffort: "high", fast: true });
  const device = fixture.createDevice();

  try {
    await flush();
    device.registry.set(changeSettingsAtom, { kind: "speed" });
    await flush();
    device.setEmail(guest);
    await flush();
    const ownerRead = fixture.requests.find(({ email }) => email === owner);
    const guestRead = fixture.requests.find(({ email }) => email === guest);

    expect(guestRead).toBeDefined();
    ownerRead!.succeed();
    await flush();
    expect(device.registry.get(settingsStatusAtom).loading).toBe(true);
    guestRead!.succeed();
    await flush();
    expect(device.registry.get(settingsAtom)).toEqual(defaultPlannerSettings);
    expect(fixture.requests.filter(({ tag }) => tag === "SavePlannerSettings")).toEqual([]);
    expect(fixture.persisted.get(guest)).toEqual(defaultPlannerSettings);
  } finally {
    device.close();
  }
});
