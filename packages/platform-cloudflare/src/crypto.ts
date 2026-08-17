import { Crypto, Effect, Layer, PlatformError } from "effect";

declare const crypto: {
  readonly getRandomValues: (bytes: Uint8Array) => Uint8Array;
  readonly subtle: {
    readonly digest: (algorithm: string, data: Uint8Array) => Promise<ArrayBuffer>;
  };
};

/** Effect Crypto authority backed by the Worker runtime's WebCrypto implementation. */
export const cloudflareCryptoLayer: Layer.Layer<Crypto.Crypto> = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => crypto.getRandomValues(new Uint8Array(size)),
    digest: (algorithm, data) =>
      Effect.tryPromise({
        try: async () =>
          new Uint8Array(await crypto.subtle.digest(algorithm, Uint8Array.from(data))),
        catch: (cause) =>
          PlatformError.systemError({
            _tag: "Unknown",
            module: "CloudflareCrypto",
            method: "digest",
            cause,
          }),
      }),
  }),
);
