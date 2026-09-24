import { Effect, Schema } from "effect";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import { describe, expect, it } from "vite-plus/test";

const definition = MemoryNamespace.define({
  name: "app/user-conversations",
  version: 1,
  identity: Schema.Struct({ tenantId: Schema.String, userId: Schema.String }),
});

describe("memory namespace addresses", () => {
  it("uses independently specified canonical fixtures and restores a definition", async () => {
    const fixtures = [
      [
        { tenantId: "a:b", userId: "c" },
        '[1,"app/user-conversations",1,{"tenantId":"a:b","userId":"c"}]',
      ],
      [
        { tenantId: "a", userId: "b:c" },
        '[1,"app/user-conversations",1,{"tenantId":"a","userId":"b:c"}]',
      ],
    ] as const;

    for (const [identity, address] of fixtures) {
      const value = definition.make(identity);

      expect(value.address).toBe(address);
      const restored = await Effect.runPromise(definition.restore(address));

      expect(restored.identity).toEqual(identity);
    }
  });

  it("sorts nested keys without Unicode aliases", () => {
    const records = MemoryNamespace.define({ name: "records", version: 1, identity: Schema.Json });

    expect(records.make({ z: { b: 2, a: 1 }, a: 3 }).address).toBe(
      '[1,"records",1,{"a":3,"z":{"a":1,"b":2}}]',
    );
    expect(records.make({ a: 3, z: { a: 1, b: 2 } }).address).toBe(
      records.make({ z: { b: 2, a: 1 }, a: 3 }).address,
    );
    expect(records.make({ "10": 10, "2": 2 }).address).toBe('[1,"records",1,{"10":10,"2":2}]');
    expect(records.make("é").address).not.toBe(records.make("é").address);
  });
});
