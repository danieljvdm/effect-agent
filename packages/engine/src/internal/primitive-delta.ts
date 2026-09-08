import { Option, Schema } from "effect";
import { Response } from "effect/unstable/ai";

import { boundedValueFootprint, utf8ByteLength } from "./bounded-value.ts";

const brand = "~effect/ai/Content/Part";
const keys = [brand, "type", "id", "delta", "metadata"];

const decode = Schema.decodeUnknownOption(
  Schema.toType(Schema.Union([Response.TextDeltaPart, Response.ReasoningDeltaPart])),
);

const overhead = (type: "text-delta" | "reasoning-delta") => ({
  source: boundedValueFootprint(
    Response.makePart(type, { id: "", delta: "" }),
    Number.MAX_SAFE_INTEGER,
  ),
  encoded: boundedValueFootprint(
    { type, id: "", delta: "", metadata: {} },
    Number.MAX_SAFE_INTEGER,
  ),
});

const overheads = {
  "text-delta": overhead("text-delta"),
  "reasoning-delta": overhead("reasoning-delta"),
};

/**
 * Copy the common primitive delta into owned data before native Schema validation.
 * Descriptor checks select this optimization; they do not replace the native codec.
 * Extra fields, accessors, and nonempty metadata use the general ownership path.
 * No provider object survives, including an empty metadata object's hidden storage.
 * Unlike a generic footprint shortcut, this cannot retain an exotic backing buffer.
 */
export const ownPrimitiveDelta = (part: unknown, maxBytes: number) => {
  try {
    if (part === null || typeof part !== "object") return undefined;
    if (Array.isArray(part) || ArrayBuffer.isView(part)) return undefined;
    const prototype = Object.getPrototypeOf(part);

    if (prototype !== Object.prototype && prototype !== null) return undefined;
    if (Reflect.ownKeys(part).length !== keys.length) return undefined;
    const snapshot: Record<string, unknown> = {};

    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(part, key);

      if (descriptor === undefined || !("value" in descriptor)) return undefined;
      snapshot[key] = descriptor.value;
    }
    const { type, id, delta, metadata } = snapshot;

    if (type !== "text-delta" && type !== "reasoning-delta") return undefined;
    if (typeof id !== "string" || typeof delta !== "string") return undefined;
    if (metadata === null || typeof metadata !== "object") return undefined;
    if (Array.isArray(metadata) || ArrayBuffer.isView(metadata)) return undefined;
    const metadataPrototype = Object.getPrototypeOf(metadata);

    if (metadataPrototype !== Object.prototype && metadataPrototype !== null) return undefined;
    if (Reflect.ownKeys(metadata).length !== 0) return undefined;
    const fixed = overheads[type];

    if (fixed.source === undefined || fixed.encoded === undefined) return undefined;
    const bytes = utf8ByteLength(id) + utf8ByteLength(delta);

    if (bytes + fixed.source > maxBytes || bytes + fixed.encoded > maxBytes) return undefined;
    // Never pass the provider's metadata object (or any other object) to the decoder.
    snapshot.metadata = {};
    const validated = decode(snapshot);

    if (Option.isNone(validated)) return undefined;

    return { ownedPart: validated.value, retainedBytes: bytes + fixed.encoded };
  } catch {
    // Reflection may throw for proxies. The general path owns the typed failure.
    return undefined;
  }
};
