// Implement the Effect Crypto service with the Workers native hashing primitive.
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { createHash } from "node:crypto";

import { BrowserCrypto } from "@effect/platform-browser";
import { Crypto, Effect, Layer, PlatformError } from "effect";

/** Workers use native SHA-256 while retaining BrowserCrypto's services and overrides. */
export const cloudflareCryptoLayer: Layer.Layer<Crypto.Crypto> = Layer.effect(
  Crypto.Crypto,
  Effect.gen(function* () {
    const base = yield* Crypto.Crypto;
    const webCrypto = yield* BrowserCrypto.WebCrypto;
    const hostGlobals: object = globalThis;

    return Crypto.Crypto.of({
      ...base,
      digest: (algorithm, data) => {
        if (
          algorithm !== "SHA-256" ||
          !("crypto" in hostGlobals) ||
          webCrypto !== hostGlobals.crypto ||
          typeof webCrypto.subtle?.digest !== "function"
        )
          return base.digest(algorithm, data);

        return Effect.try({
          try: () => Uint8Array.from(createHash("sha256").update(new Uint8Array(data)).digest()),
          catch: (cause) =>
            PlatformError.systemError({
              module: "Crypto",
              method: "digest",
              _tag: "Unknown",
              description: "Could not compute digest",
              cause,
            }),
        });
      },
    });
  }),
).pipe(Layer.provide(BrowserCrypto.layer));
