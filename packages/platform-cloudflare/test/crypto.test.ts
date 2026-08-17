import { Crypto, Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { cloudflareCryptoLayer } from "../src/crypto.ts";

describe("Cloudflare Crypto", () => {
  it("chunks random byte requests above WebCrypto's per-call limit", async () => {
    const bytes = await Effect.runPromise(
      Effect.gen(function* () {
        const crypto = yield* Crypto.Crypto;
        return yield* crypto.randomBytes(65_537);
      }).pipe(Effect.provide(cloudflareCryptoLayer)),
    );

    expect(bytes).toHaveLength(65_537);
  });
});
