import { DateTime, Redacted } from "effect";

const DEFAULT_MAX_DEPTH = 128;
const OBJECT_OVERHEAD_BYTES = 32;
const PROPERTY_OVERHEAD_BYTES = 8;
const dateTimeUtcPrototype = Object.getPrototypeOf(DateTime.makeUnsafe(0)) as object;
const redactedPrototype = Object.getPrototypeOf(Redacted.make(undefined)) as object;

const intrinsicViewPrototypes = new Set<object>([
  DataView.prototype,
  Int8Array.prototype,
  Uint8Array.prototype,
  Uint8ClampedArray.prototype,
  Int16Array.prototype,
  Uint16Array.prototype,
  Int32Array.prototype,
  Uint32Array.prototype,
  Float32Array.prototype,
  Float64Array.prototype,
  BigInt64Array.prototype,
  BigUint64Array.prototype,
]);

const arrayBufferByteLengthGetter = Object.getOwnPropertyDescriptor(
  ArrayBuffer.prototype,
  "byteLength",
)?.get;

const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
const typedArrayBufferGetter = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer")?.get;
const typedArrayLengthGetter = Object.getOwnPropertyDescriptor(typedArrayPrototype, "length")?.get;
const dataViewBufferGetter = Object.getOwnPropertyDescriptor(DataView.prototype, "buffer")?.get;
const urlConstructor = Reflect.get(globalThis, "URL");

const urlPrototypeDescriptor =
  typeof urlConstructor === "function"
    ? Object.getOwnPropertyDescriptor(urlConstructor, "prototype")
    : undefined;

const urlHrefGetter =
  urlPrototypeDescriptor !== undefined &&
  "value" in urlPrototypeDescriptor &&
  urlPrototypeDescriptor.value !== null &&
  typeof urlPrototypeDescriptor.value === "object"
    ? Object.getOwnPropertyDescriptor(urlPrototypeDescriptor.value, "href")?.get
    : undefined;

const urlPrototype =
  urlPrototypeDescriptor !== undefined &&
  "value" in urlPrototypeDescriptor &&
  urlPrototypeDescriptor.value !== null &&
  typeof urlPrototypeDescriptor.value === "object"
    ? urlPrototypeDescriptor.value
    : undefined;

const intrinsicArrayBufferByteLength = (value: object): number | undefined => {
  if (arrayBufferByteLengthGetter === undefined) return undefined;
  try {
    const byteLength = Reflect.apply(arrayBufferByteLengthGetter, value, []);

    return Number.isSafeInteger(byteLength) && byteLength >= 0 ? byteLength : undefined;
  } catch {
    return undefined;
  }
};

const intrinsicViewBackingByteLength = (value: object): number | undefined => {
  const getters = [typedArrayBufferGetter, dataViewBufferGetter];

  for (const getter of getters) {
    if (getter === undefined) continue;
    try {
      const buffer = Reflect.apply(getter, value, []);

      if (buffer !== null && typeof buffer === "object") {
        return intrinsicArrayBufferByteLength(buffer);
      }
    } catch {
      // Try the other supported view family without consulting user properties.
    }
  }

  return undefined;
};

const intrinsicTypedArrayLength = (value: object): number | undefined => {
  if (typedArrayLengthGetter === undefined) return undefined;
  try {
    const length = Reflect.apply(typedArrayLengthGetter, value, []);

    return Number.isSafeInteger(length) && length >= 0 ? length : undefined;
  } catch {
    return undefined;
  }
};

const intrinsicUrlByteLength = (value: object): number | undefined => {
  if (urlHrefGetter === undefined) return undefined;
  try {
    const href = Reflect.apply(urlHrefGetter, value, []);

    return typeof href === "string" ? utf8ByteLength(href) + 2 : undefined;
  } catch {
    return undefined;
  }
};

const inspectPrototype = (
  prototype: object | null,
  isArray: boolean,
  knownSafePrototypes: ReadonlySet<object>,
): boolean => {
  if (prototype === null) return true;
  if (isArray) {
    return prototype === Array.prototype;
  }

  return (
    prototype === Object.prototype ||
    prototype === dateTimeUtcPrototype ||
    knownSafePrototypes.has(prototype)
  );
};

const isCanonicalArrayIndex = (key: string): boolean => {
  const index = Number(key);

  return Number.isInteger(index) && index >= 0 && index < 0xffff_ffff && String(index) === key;
};

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
 * Conservatively measure one retained, engine-owned JavaScript value without invoking getters,
 * coercion, or `toJSON`. `undefined` means the value exceeded the allowance or its shape could not
 * be measured without executing an accessor.
 *
 * This is a memory-retention guard, not an untrusted wire boundary. Callers must first canonicalize
 * provider values into owned data because JavaScript offers no portable, trap-free Proxy test.
 * Object and property overheads deliberately make the estimate larger than the visible primitive
 * payload for ordinary response values.
 */
export const boundedValueFootprint = (
  root: unknown,
  maxBytes: number,
  knownSafePrototypes: ReadonlySet<object> = new Set(),
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

  const canMaterializeIndexedKeys = (count: number): boolean => {
    if (!Number.isSafeInteger(count) || count < 0) return false;
    if (count === 0) return true;
    const largestKeyBytes = utf8ByteLength(String(count - 1));
    const temporaryBytesPerKey = PROPERTY_OVERHEAD_BYTES + largestKeyBytes;

    return count <= Math.floor((maxBytes - total) / temporaryBytesPerKey);
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
        return add(16);
      case "bigint":
      case "symbol":
        return false;
      case "undefined":
        return add(16);
      case "function":
        return false;
      case "object": {
        if (ancestors.has(value) || !add(OBJECT_OVERHEAD_BYTES)) return false;
        const prototype = Object.getPrototypeOf(value);
        let skipIndexedProperties = false;
        let indexedPropertyCount = 0;
        let supportedSpecialObject = false;

        if (ArrayBuffer.isView(value)) {
          if (!intrinsicViewPrototypes.has(prototype)) return false;
          const byteLength = intrinsicViewBackingByteLength(value);

          if (byteLength === undefined || !add(byteLength)) return false;
          if (prototype !== DataView.prototype) {
            const length = intrinsicTypedArrayLength(value);

            if (length === undefined) return false;
            indexedPropertyCount = length;
          }
          skipIndexedProperties = true;
          supportedSpecialObject = true;
        }
        if (!supportedSpecialObject) {
          const bufferByteLength = intrinsicArrayBufferByteLength(value);

          if (bufferByteLength !== undefined) {
            if (prototype !== ArrayBuffer.prototype) return false;
            if (!add(bufferByteLength)) return false;
            supportedSpecialObject = true;
          }
        }
        if (!supportedSpecialObject) {
          const urlByteLength = intrinsicUrlByteLength(value);

          if (urlByteLength !== undefined) {
            if (prototype !== urlPrototype) return false;
            if (!add(urlByteLength)) return false;
            supportedSpecialObject = true;
          }
        }
        let redactedValue: unknown;
        let isRedacted = false;

        if (prototype === redactedPrototype) {
          const labelDescriptor = Object.getOwnPropertyDescriptor(value, "label");

          if (labelDescriptor !== undefined && !("value" in labelDescriptor)) return false;
          try {
            redactedValue = Redacted.value(value as Redacted.Redacted<unknown>);
            isRedacted = true;
          } catch {
            return false;
          }
        }

        const isArray = Array.isArray(value);

        if (
          !supportedSpecialObject &&
          !isRedacted &&
          !inspectPrototype(prototype, isArray, knownSafePrototypes)
        ) {
          // Maps, Sets, arbitrary class instances, and other objects can retain storage that
          // own-key traversal cannot see. Only plain data and caller-supplied exact prototypes
          // whose implementations guarantee own-property state are measurable.
          return false;
        }
        if (isArray) {
          const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");

          if (
            lengthDescriptor === undefined ||
            !("value" in lengthDescriptor) ||
            !Number.isSafeInteger(lengthDescriptor.value) ||
            lengthDescriptor.value < 0 ||
            !add(lengthDescriptor.value)
          ) {
            return false;
          }
          indexedPropertyCount = lengthDescriptor.value;
        }

        // `Reflect.ownKeys` eagerly allocates one string per dense array or typed-array index.
        // Reject before that allocation when even the conservative temporary key list cannot fit
        // inside the caller's allowance. Sparse arrays deliberately use their worst-case length.
        if (!canMaterializeIndexedKeys(indexedPropertyCount)) return false;

        ancestors.add(value);
        try {
          if (isRedacted && !visit(redactedValue, depth + 1)) return false;
          for (const key of Reflect.ownKeys(value)) {
            if (key === "length" && isArray) continue;
            if (typeof key === "symbol") return false;
            if (skipIndexedProperties && isCanonicalArrayIndex(key)) continue;
            if (!add(PROPERTY_OVERHEAD_BYTES)) return false;
            if (!add(utf8ByteLength(key))) return false;
            const descriptor = Object.getOwnPropertyDescriptor(value, key);

            if (descriptor === undefined) return false;
            if ("value" in descriptor) {
              if (!visit(descriptor.value, depth + 1)) return false;
            } else {
              // Accessors and functions may retain arbitrarily large closure graphs.
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
