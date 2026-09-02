import { Effect, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { MemoryKey, MemoryNamespace, MemoryNamespaceAddress } from "../src/index.ts";

const definition = MemoryNamespace.define({
  name: "app/user-conversations",
  version: 1,
  identity: Schema.Struct({ tenantId: Schema.String, userId: Schema.String }),
});

describe("memory namespace addresses", () => {
  it("uses independently specified canonical fixtures and restores a definition", async () => {
    const fixtures = [
      [
        { tenantId: "a", userId: "b" },
        '[1,"app/user-conversations",1,{"tenantId":"a","userId":"b"}]',
      ],
      [
        { tenantId: "a:b", userId: "c" },
        '[1,"app/user-conversations",1,{"tenantId":"a:b","userId":"c"}]',
      ],
      [
        { tenantId: "a", userId: "b:c" },
        '[1,"app/user-conversations",1,{"tenantId":"a","userId":"b:c"}]',
      ],
      [
        { tenantId: "海🌊", userId: 'quote"\\\n' },
        '[1,"app/user-conversations",1,{"tenantId":"海🌊","userId":"quote\\"\\\\\\n"}]',
      ],
    ] as const;

    for (const [identity, address] of fixtures) {
      const value = definition.make(identity);

      expect(value.address).toBe(address);
      const restored = await Effect.runPromise(definition.restore(address));

      expect(restored.identity).toEqual(identity);
      expect(MemoryNamespace.equals(value, restored)).toBe(true);

      const wire = Schema.encodeSync(MemoryKey.Wire)(
        MemoryKey.make({ namespace: value, id: "same" }),
      );

      expect(Schema.decodeSync(MemoryKey.Wire)(wire).namespace.address).toBe(address);
    }
  });

  it("normalizes through the codec and sorts nested keys without delimiter or Unicode aliases", async () => {
    const records = MemoryNamespace.define({ name: "records", version: 1, identity: Schema.Json });

    expect(records.make({ z: { b: 2, a: 1 }, a: 3 }).address).toBe(
      '[1,"records",1,{"a":3,"z":{"a":1,"b":2}}]',
    );
    expect(records.make({ a: 3, z: { a: 1, b: 2 } }).address).toBe(
      records.make({ z: { b: 2, a: 1 }, a: 3 }).address,
    );
    expect(records.make({ "10": 10, "2": 2 }).address).toBe('[1,"records",1,{"10":10,"2":2}]');
    expect(records.make("é").address).not.toBe(records.make("é").address);
    expect(records.make(-0).address).toBe(records.make(0).address);

    const dates = MemoryNamespace.define({
      name: "dates",
      version: 1,
      identity: Schema.DateFromString,
    });

    const decoded = await Effect.runPromise(dates.decode("2026-01-01T01:00:00+01:00"));

    expect(decoded.address).toBe('[1,"dates",1,"2026-01-01T00:00:00.000Z"]');
    expect((await Effect.runPromise(dates.restore(decoded.address))).identity.toISOString()).toBe(
      "2026-01-01T00:00:00.000Z",
    );
  });

  it("rejects wrong definitions, unsupported formats, malformed and out-of-bound identities", async () => {
    const address = definition.make({ tenantId: "a", userId: "b" }).address;
    const other = MemoryNamespace.define({ name: "other", version: 1, identity: Schema.Json });

    const newer = MemoryNamespace.define({
      name: definition.name,
      version: 2,
      identity: Schema.Json,
    });

    expect(await Effect.runPromise(other.restore(address).pipe(Effect.flip))).toMatchObject({
      reason: "wrong-definition",
    });
    expect(await Effect.runPromise(newer.restore(address).pipe(Effect.flip))).toMatchObject({
      reason: "wrong-definition",
    });
    expect(
      await Effect.runPromise(
        definition.restore('[2,"app/user-conversations",1,{}]').pipe(Effect.flip),
      ),
    ).toMatchObject({ reason: "unsupported-format" });
    for (const input of [
      "raw-string",
      address + " ",
      '[1,"app/user-conversations",1,{"userId":"b","tenantId":"a"}]',
    ]) {
      expect(await Effect.runPromise(definition.restore(input).pipe(Effect.flip))).toMatchObject({
        reason: "invalid-address",
      });
    }
    expect(
      await Effect.runPromise(definition.decode({ tenantId: "a" }).pipe(Effect.flip)),
    ).toMatchObject({ reason: "invalid-identity" });
    expect(
      await Effect.runPromise(
        definition.restore('[1,"app/user-conversations",1,42]').pipe(Effect.flip),
      ),
    ).toMatchObject({ reason: "invalid-identity" });
    for (const name of ["", "x".repeat(257)]) {
      expect(() => MemoryNamespace.define({ name, version: 1, identity: Schema.String })).toThrow(
        /Expected/,
      );
    }
    for (const version of [0, 1.5, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() =>
        MemoryNamespace.define({ name: "bounded", version, identity: Schema.String }),
      ).toThrow(/Expected/);
    }
    const bounded = MemoryNamespace.define({ name: "n", version: 1, identity: Schema.Json });

    expect(bounded.make("x".repeat(4084)).address.length).toBe(4096);
    for (const identity of [
      "x".repeat(4085),
      "海".repeat(1400),
      Array.from({ length: 129 }, () => 1),
      Number.NaN,
      undefined,
    ]) {
      expect(await Effect.runPromise(bounded.decode(identity).pipe(Effect.flip))).toMatchObject({
        reason: "invalid-identity",
      });
    }
    let nested: Schema.Json = 0;

    for (let depth = 0; depth < 17; depth++) nested = [nested];
    expect(await Effect.runPromise(bounded.decode(nested).pipe(Effect.flip))).toMatchObject({
      reason: "invalid-identity",
    });
    expect(Schema.is(MemoryNamespaceAddress)('[1,"n",1,{"x":1,"x":2}]')).toBe(false);
  });
});
