import {
  decodeMemoryWire,
  encodeMemoryWire,
} from "@effect-agent/storage-cloudflare/MemoryProtocol";
import { Effect, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

describe("MemoryProtocol", () => {
  it("enforces the UTF-8 wire budget at the exact multibyte boundary", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const text = "é€😀";
        const encoded = '"é€😀"';

        expect(yield* encodeMemoryWire(Schema.String, text, 11)).toBe(encoded);
        expect(yield* decodeMemoryWire(Schema.String, encoded, 11)).toBe(text);
        expect(yield* encodeMemoryWire(Schema.String, text, 10).pipe(Effect.flip)).toMatchObject({
          _tag: "MemoryRpcError",
          reason: "budget",
        });
        expect(yield* decodeMemoryWire(Schema.String, encoded, 10).pipe(Effect.flip)).toMatchObject(
          {
            _tag: "MemoryRpcError",
            reason: "budget",
          },
        );
      }),
    ));
});
