import { Schema } from "effect";

/**
 * The minimal `TruncatedToolResult` envelope (empty head/tail, a 16-digit
 * `originalBytes`) encodes to at most 81 UTF-8 bytes; 256 guarantees every
 * accepted bound fits the envelope plus some real content, so the bound is a
 * hard resource guard rather than a best-effort one.
 */
const ENVELOPE_FLOOR_BYTES = 256;

/** Byte bound applied to one encoded application Tool result before it enters history. */
export class ToolResultBounds extends Schema.Class<ToolResultBounds>("ToolResultBounds")({
  maxBytes: Schema.Int.check(Schema.isGreaterThanOrEqualTo(ENVELOPE_FLOOR_BYTES)),
}) {}

/**
 * Canonical envelope replacing an encoded Tool result that exceeded its
 * policy bounds. `head` and `tail` are an exact prefix and suffix of the
 * original encoded JSON text; the middle is dropped, never paraphrased.
 */
export class TruncatedToolResult extends Schema.Class<TruncatedToolResult>("TruncatedToolResult")({
  truncatedToolResult: Schema.Literal(true),
  originalBytes: Schema.Natural,
  head: Schema.String,
  tail: Schema.String,
}) {}

const codePointUtf8Length = (codePoint: number): number => {
  if (codePoint < 0x80) return 1;
  if (codePoint < 0x800) return 2;
  if (codePoint < 0x10000) return 3;
  return 4;
};

const utf8ByteLength = (value: string): number => {
  let bytes = 0;
  let index = 0;
  while (index < value.length) {
    const codePoint = value.codePointAt(index) ?? 0;
    bytes += codePointUtf8Length(codePoint);
    index += codePoint > 0xffff ? 2 : 1;
  }
  return bytes;
};

const takePrefixWithinBytes = (value: string, maxBytes: number): string => {
  let bytes = 0;
  let index = 0;
  while (index < value.length) {
    const codePoint = value.codePointAt(index) ?? 0;
    const width = codePointUtf8Length(codePoint);
    if (bytes + width > maxBytes) break;
    bytes += width;
    index += codePoint > 0xffff ? 2 : 1;
  }
  return value.slice(0, index);
};

const takeSuffixWithinBytes = (value: string, maxBytes: number): string => {
  let bytes = 0;
  let index = value.length;
  while (index > 0) {
    const unit = value.charCodeAt(index - 1);
    const isLowSurrogate = unit >= 0xdc00 && unit <= 0xdfff;
    const pairStart = isLowSurrogate && index >= 2 ? index - 2 : index - 1;
    const codePoint = value.codePointAt(pairStart) ?? 0;
    const width = codePointUtf8Length(codePoint);
    if (bytes + width > maxBytes) break;
    bytes += width;
    index = pairStart;
  }
  return value.slice(index);
};

/**
 * Bound one encoded Tool result to `bounds.maxBytes` UTF-8 bytes.
 *
 * Within bounds the input is returned unchanged. Over bounds the result is
 * the JSON-encoded `TruncatedToolResult` envelope preserving an exact prefix
 * and suffix of the input, split ~50/50 across the byte budget left after
 * the envelope's own overhead and shrunk deterministically until the encoded
 * envelope itself fits. Slicing never splits a UTF-16 surrogate pair, so the
 * envelope always re-encodes as valid JSON.
 *
 * The Schema-level 256-byte floor on `ToolResultBounds.maxBytes` guarantees
 * every accepted bound fits the minimal envelope (at most 81 bytes), so the
 * returned value never exceeds `maxBytes` for a decodable bound. The
 * minimal-envelope fallback below survives purely as defense in depth.
 */
export const applyToolResultBounds = (encodedJson: string, bounds: ToolResultBounds): string => {
  const originalBytes = utf8ByteLength(encodedJson);
  if (originalBytes <= bounds.maxBytes) return encodedJson;
  const render = (head: string, tail: string): string =>
    JSON.stringify(
      Schema.encodeSync(TruncatedToolResult)(
        TruncatedToolResult.make({ truncatedToolResult: true, originalBytes, head, tail }),
      ),
    );
  const minimal = render("", "");
  const contentBudget = bounds.maxBytes - utf8ByteLength(minimal);
  if (contentBudget <= 0) return minimal;
  let headBudget = Math.floor(contentBudget / 2);
  let tailBudget = contentBudget - headBudget;
  for (;;) {
    const head = takePrefixWithinBytes(encodedJson, headBudget);
    const tail = takeSuffixWithinBytes(encodedJson, tailBudget);
    const output = render(head, tail);
    const outputBytes = utf8ByteLength(output);
    if (outputBytes <= bounds.maxBytes) return output;
    if (headBudget === 0 && tailBudget === 0) return minimal;
    const shrink = Math.max(1, Math.ceil((outputBytes - bounds.maxBytes) / 2));
    headBudget = Math.max(0, headBudget - shrink);
    tailBudget = Math.max(0, tailBudget - shrink);
  }
};
