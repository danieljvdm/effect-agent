import { Encoding } from "effect";

/** Encode through Effect without requiring browser or Node ambient types in consumers. */
export const utf8Bytes = (value: string): Uint8Array => {
  const hex = Encoding.encodeHex(value);

  return Uint8Array.from({ length: hex.length / 2 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
};

/** Count UTF-8 bytes without allocating an encoded copy, including replacement bytes for lone surrogates. */
export const utf8ByteLength = (value: string): number => {
  let total = 0;

  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;

    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }

  return total;
};
