import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { afterAll, describe, expect, it } from "vite-plus/test";

/**
 * WP0 probe 4: Miniflare programmatic runtime restarts.
 *
 * (a) Durable Object SQLite storage written before `dispose()` is readable
 *     after reopening a fresh Miniflare over the same persist directory —
 *     the "runtime restart" analogue of the DN reopen tests.
 * (b) Whether a persisted, overdue alarm re-delivers after reopen WITHOUT any
 *     incoming request to the Durable Object. The alarm handler reports its
 *     firing to a Node-side HTTP listener, so detection never touches the DO.
 *
 * WP3's Miniflare restart lane builds directly on both outcomes.
 */

/**
 * Inline JS worker module: Miniflare 4 no longer bundles, and this probe needs
 * no imports, so an inline module keeps the probe self-contained. (The real
 * WP5 restart lane bundles fixture code with esbuild instead.)
 */
const probeWorkerScript = `
export class RestartProbeObject {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (url.pathname === "/prepare") {
      await this.state.storage.put("wp0:persisted", "before-restart");
      await this.state.storage.setAlarm(Date.now() + Number(url.searchParams.get("alarmDelayMs")));
      return Response.json({ prepared: true });
    }
    if (url.pathname === "/report") {
      return Response.json({
        persisted: (await this.state.storage.get("wp0:persisted")) ?? null,
        scheduledAlarm: await this.state.storage.getAlarm(),
        alarmFiredAt: (await this.state.storage.get("wp0:alarm-fired-at")) ?? null,
      });
    }
    return new Response("not found", { status: 404 });
  }

  async alarm() {
    await this.state.storage.put("wp0:alarm-fired-at", Date.now());
    await fetch(this.env.ALARM_REPORT_URL, { method: "POST" });
  }
}

export default {
  fetch(request, env) {
    const id = env.PROBE.idFromName("wp0-restart-probe");
    return env.PROBE.get(id).fetch(request);
  },
};
`;

const openRuntime = (persistDirectory: string, alarmReportUrl: string) =>
  new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script: probeWorkerScript,
      compatibilityDate: "2025-05-01",
      durableObjects: {
        PROBE: { className: "RestartProbeObject", useSQLite: true },
      },
      resourcePersistencePath: persistDirectory,
      bindings: { ALARM_REPORT_URL: alarmReportUrl },
    }),
  );

describe("Miniflare persist-dir restart lane (WP0 probe 4)", () => {
  const cleanups: Array<() => Promise<void>> = [];

  afterAll(async () => {
    for (const cleanup of cleanups.reverse()) {
      await cleanup();
    }
  });

  it("persists DO storage across dispose/reopen and re-delivers the overdue alarm without an incoming request", async () => {
    const persistDirectory = await mkdtemp(join(tmpdir(), "wp0-miniflare-probe-"));
    cleanups.push(() => rm(persistDirectory, { recursive: true, force: true }));

    // Node-side listener the alarm handler pings, so alarm re-delivery is
    // observable without ever sending the Durable Object a request.
    const alarmPings: Array<number> = [];
    const server = createServer((_request, response) => {
      alarmPings.push(Date.now());
      response.writeHead(204).end();
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    cleanups.push(() => new Promise((resolve) => server.close(() => resolve())));
    const alarmReportUrl = `http://127.0.0.1:${(server.address() as AddressInfo).port}/alarm-fired`;

    // First runtime: persist a value and arm an alarm, then dispose before
    // the alarm can fire.
    const first = openRuntime(persistDirectory, alarmReportUrl);
    const prepared = await first.dispatchFetch("http://placeholder/prepare?alarmDelayMs=120000");
    expect(await prepared.json()).toEqual({ prepared: true });
    await first.dispose();
    expect(alarmPings).toHaveLength(0);

    // Second runtime over the same directory. Rewind the persisted alarm's
    // deadline? No — the alarm was armed 120s out so it is NOT overdue yet;
    // first verify storage and the alarm schedule survived the restart.
    const second = openRuntime(persistDirectory, alarmReportUrl);
    const report = (await (await second.dispatchFetch("http://placeholder/report")).json()) as {
      persisted: string | null;
      scheduledAlarm: number | null;
      alarmFiredAt: number | null;
    };
    expect(report.persisted).toBe("before-restart");
    expect(report.scheduledAlarm).not.toBeNull();
    expect(report.alarmFiredAt).toBeNull();
    await second.dispose();

    // Third runtime: arm a short alarm, dispose before it fires, reopen, and
    // wait for the ping with no DO request in between.
    const third = openRuntime(persistDirectory, alarmReportUrl);
    const preparedAt = Date.now();
    await third.dispatchFetch("http://placeholder/prepare?alarmDelayMs=1500");
    await third.dispose();
    const disposedAt = Date.now();
    // Guard the probe's own timing assumption: dispose must complete before
    // the alarm deadline, or the re-delivery evidence below is inconclusive.
    expect(disposedAt - preparedAt).toBeLessThan(1500);

    const fourth = openRuntime(persistDirectory, alarmReportUrl);
    await fourth.ready;
    cleanups.push(() => fourth.dispose());
    const reopenedAt = Date.now();

    const deadline = Date.now() + 15_000;
    while (alarmPings.length === 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 250));
    }

    expect(alarmPings.length).toBeGreaterThan(0);
    // The ping arrived after the restart, so delivery came from the reopened
    // runtime's persisted alarm, not from the disposed one.
    expect(alarmPings[0]).toBeGreaterThan(disposedAt);
    expect(alarmPings[0]).toBeGreaterThanOrEqual(reopenedAt - 1);
  }, 60_000);
});
