import { DateTime, Redacted, Schema } from "effect";

const DEFAULT_MAX_DEPTH = 128;
const OBJECT_OVERHEAD_BYTES = 32;
const PROPERTY_OVERHEAD_BYTES = 8;
const dateTimeUtcPrototype = Object.getPrototypeOf(DateTime.makeUnsafe(0)) as object;
const redactedPrototype = Object.getPrototypeOf(Redacted.make(undefined)) as object;

const arrayBufferByteLengthGetter = Object.getOwnPropertyDescriptor(
  ArrayBuffer.prototype,
  "byteLength",
)?.get;
const typedArrayPrototype = Object.getPrototypeOf(Uint8Array.prototype) as object;
const typedArrayBufferGetter = Object.getOwnPropertyDescriptor(typedArrayPrototype, "buffer")?.get;
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

const intrinsicUrlByteLength = (value: object): number | undefined => {
  if (urlHrefGetter === undefined) return undefined;
  try {
    const href = Reflect.apply(urlHrefGetter, value, []);
    return typeof href === "string" ? utf8ByteLength(href) + 2 : undefined;
  } catch {
    return undefined;
  }
};

interface PrototypeInspection {
  readonly trustChildren: boolean;
}

const inspectPrototype = (
  prototype: object | null,
  isArray: boolean,
  trustedSchemaProduct: boolean,
): PrototypeInspection | undefined => {
  if (prototype === null) return { trustChildren: trustedSchemaProduct };
  if (isArray) {
    return prototype === Array.prototype ? { trustChildren: trustedSchemaProduct } : undefined;
  }
  if (prototype === Object.prototype) return { trustChildren: trustedSchemaProduct };
  if (prototype === dateTimeUtcPrototype) return { trustChildren: false };

  const constructorDescriptor = Object.getOwnPropertyDescriptor(prototype, "constructor");
  if (
    constructorDescriptor !== undefined &&
    "value" in constructorDescriptor &&
    typeof constructorDescriptor.value === "function"
  ) {
    const constructorPrototype = Object.getOwnPropertyDescriptor(
      constructorDescriptor.value,
      "prototype",
    );
    if (
      constructorPrototype !== undefined &&
      "value" in constructorPrototype &&
      constructorPrototype.value === prototype &&
      Schema.isSchema(constructorDescriptor.value)
    ) {
      // Schema classes are decoded data products. Their state remains in own fields, unlike
      // arbitrary class instances with private or native internal slots.
      return { trustChildren: true };
    }
  }

  if (trustedSchemaProduct) {
    // Effect data types such as DateTime use fixed-shape, constructor-free prototypes with a
    // self-identifying TypeId. Admit them only beneath a decoded Schema class, so arbitrary
    // provider metadata cannot forge the marker into authority.
    const isEffectData = Reflect.ownKeys(prototype).some((key) => {
      if (typeof key !== "string" || !key.startsWith("~effect/")) return false;
      const descriptor = Object.getOwnPropertyDescriptor(prototype, key);
      return descriptor !== undefined && "value" in descriptor && descriptor.value === key;
    });
    if (isEffectData) return { trustChildren: true };
  }

  return undefined;
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

  const visit = (value: unknown, depth: number, trustedSchemaProduct = false): boolean => {
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
        return add(16);
      case "function":
        return false;
      case "object": {
        if (ancestors.has(value) || !add(OBJECT_OVERHEAD_BYTES)) return false;
        if (ArrayBuffer.isView(value)) {
          const byteLength = intrinsicViewBackingByteLength(value);
          return byteLength !== undefined && add(byteLength);
        }
        const bufferByteLength = intrinsicArrayBufferByteLength(value);
        if (bufferByteLength !== undefined) return add(bufferByteLength);
        const urlByteLength = intrinsicUrlByteLength(value);
        if (urlByteLength !== undefined) return add(urlByteLength);

        const prototype = Object.getPrototypeOf(value);
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
        const inspection = isRedacted
          ? { trustChildren: false }
          : inspectPrototype(prototype, isArray, trustedSchemaProduct);
        if (inspection === undefined) {
          // Maps, Sets, arbitrary class instances, and other objects can retain storage that
          // own-key traversal cannot see. Plain data and known Effect Schema values are
          // measurable because their state lives in own data properties.
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
        }

        ancestors.add(value);
        try {
          if (isRedacted && !visit(redactedValue, depth + 1)) return false;
          for (const key of Reflect.ownKeys(value)) {
            if (key === "length" && isArray) continue;
            if (!add(PROPERTY_OVERHEAD_BYTES)) return false;
            if (typeof key === "string" && !add(utf8ByteLength(key))) return false;
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (descriptor === undefined) return false;
            if ("value" in descriptor) {
              if (!visit(descriptor.value, depth + 1, inspection.trustChildren)) return false;
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
