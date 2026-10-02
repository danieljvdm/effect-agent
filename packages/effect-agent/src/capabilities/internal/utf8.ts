import { Hex } from "effect/encoding";

/** Encode through Effect without requiring browser or Node ambient types in consumers. */
export const utf8Bytes = (value: string): Uint8Array => {
  const hex = Hex.encode(value);

  return Uint8Array.from({ length: hex.length / 2 }, (_, index) =>
    Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
  );
};
