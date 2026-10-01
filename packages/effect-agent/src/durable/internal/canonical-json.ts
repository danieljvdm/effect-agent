import { Array, type Schema } from "effect";

/** Stable, locale-independent JSON object-key ordering shared by digests and derived caches. */
export const canonicalJson = (value: Schema.Json): string => {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return JSON.stringify(value);
  }
  if (Array.isArray<Schema.Json>(value)) {
    return `[${globalThis.Array.from(value, canonicalJson).join(",")}]`;
  }

  const entries = Object.entries(value).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );

  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
};
