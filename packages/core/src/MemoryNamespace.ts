import { Effect, Encoding, Schema } from "effect";

const Name = Schema.NonEmptyString.check(Schema.isMaxLength(256));

const Version = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);

const invariant = <Value>(value: Value): Value => value;

const canonicalJson = (value: Schema.Json): string => {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.entries(value)
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
      .join(",")}}`;
  }

  return JSON.stringify(value);
};

const boundedIdentity = (value: Schema.Json, depth = 0): boolean => {
  if (depth > 16) return false;
  if (value !== null && typeof value === "object") {
    const entries = Object.values(value);

    return entries.length <= 128 && entries.every((entry) => boundedIdentity(entry, depth + 1));
  }

  return true;
};

const Envelope = Schema.Tuple([
  Schema.Literal(1),
  Name,
  Version,
  Schema.Json.check(
    Schema.makeFilter((value) => boundedIdentity(value), {
      expected: "JSON identity at most 16 levels deep and 128 entries per container",
    }),
  ),
]);

const EnvelopeJson = Schema.fromJsonString(Envelope);

export class MemoryNamespaceError extends Schema.TaggedError<MemoryNamespaceError>()(
  "MemoryNamespaceError",
  {
    reason: Schema.Literals([
      "invalid-identity",
      "invalid-address",
      "wrong-definition",
      "unsupported-format",
    ]),
  },
) {}

/** Portable address used by storage, receipts, indexes, and owner routing. */
const BoundedAddress = Schema.String.check(
  Schema.isMaxLength(4_096),
  Schema.makeFilter((address) => Encoding.encodeHex(address).length / 2 <= 4_096, {
    expected: "at most 4096 UTF-8 bytes",
  }),
);

export const MemoryNamespaceAddress = BoundedAddress.check(
  Schema.makeFilter(
    (address) => {
      const decoded = Schema.decodeUnknownOption(EnvelopeJson)(address);

      return decoded._tag === "Some" && canonicalJson(decoded.value) === address;
    },
    { expected: "canonical memory namespace format 1" },
  ),
).pipe(Schema.brand("@effect-agent/core/MemoryNamespaceAddress"));

/** Application-defined identity. No membership or authorization policy is implied. */

/** Heterogeneous adapter representation. Restore through a definition before typed use. */
export const Any = Schema.Struct({
  address: MemoryNamespaceAddress,
});

export type Any = typeof Any.Type;

export interface Value<Name extends string, Version extends number, Identity> extends Any {
  readonly name: Name;
  readonly version: Version;
  readonly identity: Identity;
  readonly _type: (value: readonly [Name, Version, Identity]) => readonly [Name, Version, Identity];
}

export const equals = (left: Any, right: Any): boolean => left.address === right.address;

export const define = <
  const Name extends string,
  const Version extends number,
  Identity,
  Encoded extends Schema.Json,
>(options: {
  readonly name: Name;
  readonly version: Version;
  readonly identity: Schema.Codec<Identity, Encoded, never, never>;
}) => {
  Schema.decodeUnknownSync(Schema.Struct({ name: Name, version: Version }))({
    name: options.name,
    version: options.version,
  });
  const identityCodec = options.identity;
  const name = options.name;
  const version = options.version;

  const create = Effect.fn("MemoryNamespace.create")(
    function* (input: Identity) {
      const encoded = yield* Schema.encodeEffect(identityCodec)(input);
      const normalized = yield* Schema.decodeUnknownEffect(identityCodec)(encoded);
      const stable = yield* Schema.encodeEffect(identityCodec)(normalized);

      const repeated = yield* Schema.decodeUnknownEffect(identityCodec)(stable).pipe(
        Effect.flatMap(Schema.encodeEffect(identityCodec)),
      );

      const envelope = yield* Schema.decodeUnknownEffect(Envelope)([1, name, version, stable]);

      if (canonicalJson(stable) !== canonicalJson(repeated))
        return yield* MemoryNamespaceError.make({ reason: "invalid-identity" });

      const address = yield* Schema.decodeUnknownEffect(MemoryNamespaceAddress)(
        canonicalJson(envelope),
      );

      const value: Value<Name, Version, Identity> = Object.assign(Any.make({ address }), {
        name,
        version,
        identity: normalized,
        _type: invariant<readonly [Name, Version, Identity]>,
      });

      Object.defineProperties(value, {
        name: { enumerable: false },
        version: { enumerable: false },
        identity: { enumerable: false },
        _type: { enumerable: false },
      });

      return Object.freeze(value);
    },
    Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-identity" })),
  );

  const decode = Effect.fn("MemoryNamespace.decode")(function* (input: unknown) {
    const identity = yield* Schema.decodeUnknownEffect(identityCodec)(input).pipe(
      Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-identity" })),
    );

    return yield* create(identity);
  });

  const restore = Effect.fn("MemoryNamespace.restore")(function* (input: unknown) {
    const bounded = yield* Schema.decodeUnknownEffect(BoundedAddress)(input).pipe(
      Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-address" })),
    );

    const header = yield* Schema.decodeUnknownEffect(
      Schema.fromJsonString(Schema.Array(Schema.Json)),
    )(bounded).pipe(
      Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-address" })),
    );

    if (header[0] !== 1) return yield* MemoryNamespaceError.make({ reason: "unsupported-format" });

    const address = yield* Schema.decodeUnknownEffect(MemoryNamespaceAddress)(input).pipe(
      Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-address" })),
    );

    const envelope = yield* Schema.decodeUnknownEffect(EnvelopeJson)(address).pipe(
      Effect.mapError(() => MemoryNamespaceError.make({ reason: "invalid-address" })),
    );

    if (envelope[1] !== name || envelope[2] !== version)
      return yield* MemoryNamespaceError.make({ reason: "wrong-definition" });
    const value = yield* decode(envelope[3]);

    if (value.address !== address)
      return yield* MemoryNamespaceError.make({ reason: "invalid-address" });

    return value;
  });

  return {
    name,
    version,
    make: (identity: Identity): Value<Name, Version, Identity> => Effect.runSync(create(identity)),
    decode,
    restore,
  };
};
