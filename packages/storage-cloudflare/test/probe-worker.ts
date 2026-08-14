import { DurableObject } from "cloudflare:workers";

/**
 * Minimal SQLite-backed Durable Object for the WP0 toolchain probes.
 *
 * Tests use `runInDurableObject` to execute Effect programs directly against
 * `ctx.storage`, so the class only needs to exist as an alarm-counting shell.
 * The real Conversation Object class factory arrives with WP3 in
 * `@effect-agent/platform-cloudflare`.
 */
export class ProbeDurableObject extends DurableObject {
  override async alarm(): Promise<void> {
    const fires = ((await this.ctx.storage.get<number>("wp0:alarm-fires")) ?? 0) + 1;
    await this.ctx.storage.put("wp0:alarm-fires", fires);
  }
}

export default {
  fetch(): Response {
    return new Response("wp0 probe worker");
  },
};
