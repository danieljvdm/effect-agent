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
    randomBytes: (size) => {
      const bytes = new Uint8Array(size);
      for (let offset = 0; offset < bytes.length; offset += 65_536) {
        crypto.getRandomValues(bytes.subarray(offset, Math.min(offset + 65_536, bytes.length)));
      }
      return bytes;
    },
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
