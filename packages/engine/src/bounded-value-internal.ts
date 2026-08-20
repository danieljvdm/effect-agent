const DEFAULT_MAX_DEPTH = 128;
const OBJECT_OVERHEAD_BYTES = 32;
const PROPERTY_OVERHEAD_BYTES = 8;

/** Platform-neutral UTF-8 byte length; the engine's TypeScript lib excludes `TextEncoder`. */
export const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

/**
 * Conservatively measure one retained JavaScript value without invoking getters, coercion,
 * `toJSON`, or other user code. `undefined` means the value exceeded the allowance or hostile
 * reflection prevented a trustworthy measurement.
 *
 * This is a memory-retention guard, not a wire codec. Schema boundaries still own validation and
 * canonical encoding. Object and property overheads deliberately make the estimate larger than
 * the visible primitive payload for ordinary response values.
 */
export const boundedValueFootprint = (
  root: unknown,
  maxBytes: number,
  maxDepth = DEFAULT_MAX_DEPTH,
): number | undefined => {
  if (
    !Number.isSafeInteger(maxBytes) ||
    maxBytes < 0 ||
    !Number.isSafeInteger(maxDepth) ||
    maxDepth < 0
  ) {
    return undefined;
  }

  let total = 0;
  const ancestors = new WeakSet<object>();
  const add = (bytes: number): boolean => {
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > maxBytes - total) return false;
    total += bytes;
    return true;
  };

  const visit = (value: unknown, depth: number): boolean => {
    if (depth > maxDepth) return false;
    if (value === null) return add(4);
    switch (typeof value) {
      case "string":
        return add(utf8ByteLength(value) + 2);
      case "boolean":
        return add(4);
      case "number":
      case "bigint":
        return add(16);
      case "undefined":
      case "symbol":
      case "function":
        return add(16);
      case "object": {
        if (ancestors.has(value) || !add(OBJECT_OVERHEAD_BYTES)) return false;
        if (ArrayBuffer.isView(value)) return add(value.byteLength);
        if (value instanceof ArrayBuffer) return add(value.byteLength);
        if (Array.isArray(value) && !add(value.length)) return false;

        ancestors.add(value);
        try {
          for (const key of Reflect.ownKeys(value)) {
            if (key === "length" && Array.isArray(value)) continue;
            if (!add(PROPERTY_OVERHEAD_BYTES)) return false;
            if (typeof key === "string" && !add(utf8ByteLength(key))) return false;
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (descriptor === undefined) return false;
            if ("value" in descriptor) {
              if (!visit(descriptor.value, depth + 1)) return false;
            } else if (!add(16)) {
              // Accessors are retained but never invoked while measuring hostile values.
              return false;
            }
          }
          return true;
        } finally {
          ancestors.delete(value);
        }
      }
    }
    return false;
  };

  try {
    return visit(root, 0) ? total : undefined;
  } catch {
    return undefined;
  }
};
