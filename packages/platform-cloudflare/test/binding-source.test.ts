import { env } from "cloudflare:test";
import { describe, expect, it } from "vite-plus/test";

import { PRODUCER_PREFIX } from "./fixtures.ts";

const dynamicStub = (conversation: string) =>
  env.DYNAMIC_BINDINGS.get(env.DYNAMIC_BINDINGS.idFromName(conversation));
const arrayStub = (conversation: string) =>
  env.ARRAY_BINDINGS.get(env.ARRAY_BINDINGS.idFromName(conversation));
const effectStub = (conversation: string) =>
  env.EFFECT_BINDINGS.get(env.EFFECT_BINDINGS.idFromName(conversation));

describe("Cloudflare Binding sources", () => {
  it("evaluates the callback once with each incarnation's live host context and identities", async () => {
    const firstConversation = `binding-source-first-${crypto.randomUUID()}`;
    const secondConversation = `binding-source-second-${crypto.randomUUID()}`;
    const first = dynamicStub(firstConversation);
    const second = dynamicStub(secondConversation);

    const firstProbe = await first.bindingSourceProbe();
    expect(await first.bindingSourceProbe()).toEqual(firstProbe);
    expect(firstProbe).toMatchObject({
      evaluationCount: 1,
      conversationId: firstConversation,
      producerId: `${PRODUCER_PREFIX}:${firstConversation}`,
      rawEnvHasNamespace: true,
      stateMatches: true,
    });

    const secondProbe = await second.bindingSourceProbe();
    expect(secondProbe).toMatchObject({
      evaluationCount: 1,
      conversationId: secondConversation,
      producerId: `${PRODUCER_PREFIX}:${secondConversation}`,
      rawEnvHasNamespace: true,
      stateMatches: true,
    });
    expect(secondProbe.incarnation).not.toBe(firstProbe.incarnation);
  });

  it("keeps the legacy array and Effect source forms", async () => {
    const conversation = `binding-source-legacy-${crypto.randomUUID()}`;
    expect(await arrayStub(conversation).bindingSourceKind()).toBe("array");
    expect(await effectStub(conversation).bindingSourceKind()).toBe("effect");
  });
});
