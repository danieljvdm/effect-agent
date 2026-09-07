/** Count UTF-8 bytes without allocating an encoded copy, including replacement bytes for lone surrogates. */
export const utf8ByteLength = (value: string): number => {
  let total = 0;

  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;

    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }

  return total;
};
