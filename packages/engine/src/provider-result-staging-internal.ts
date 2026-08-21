import type { Schema } from "effect";

const MAX_JSON_DEPTH = 128;

export interface BoundedJsonSnapshot {
  readonly value: Schema.Json;
  readonly bytes: number;
}

const FailedSnapshot = Symbol("@effect-agent/engine/FailedProviderResultSnapshot");

/**
 * @internal Normalize one untrusted JSON value into an owned, frozen snapshot while counting the
 * exact UTF-8 bytes of that same snapshot. The traversal never invokes `toJSON` or accessors,
 * rejects non-plain objects and excessive nesting, and stops as soon as the byte budget is spent.
 */
const snapshotJson = (
  root: unknown,
  maxBytes: number,
  maxDepth: number,
  rejectNonFiniteNumbers: boolean,
): BoundedJsonSnapshot | undefined => {
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
    if (bytes > maxBytes - total) return false;
    total += bytes;
    return true;
  };
  const addString = (value: string): boolean => {
    if (!add(2)) return false;
    for (let index = 0; index < value.length; index += 1) {
      const code = value.charCodeAt(index);
      if (code === 0x22 || code === 0x5c) {
        if (!add(2)) return false;
      } else if (code <= 0x1f) {
        // Backspace, tab, newline, form-feed, and carriage-return use two-byte escapes; all other
        // control characters use a six-byte `\\u00xx` escape.
        if (!add(code >= 0x08 && code <= 0x0d && code !== 0x0b ? 2 : 6)) return false;
      } else if (code <= 0x7f) {
        if (!add(1)) return false;
      } else if (code <= 0x7ff) {
        if (!add(2)) return false;
      } else if (code >= 0xd800 && code <= 0xdbff) {
        const low = value.charCodeAt(index + 1);
        if (low >= 0xdc00 && low <= 0xdfff) {
          if (!add(4)) return false;
          index += 1;
        } else if (!add(6)) {
          return false;
        }
      } else if (code >= 0xdc00 && code <= 0xdfff) {
        if (!add(6)) return false;
      } else if (!add(3)) {
        return false;
      }
    }
    return true;
  };
  const visit = (value: unknown, depth: number): Schema.Json | typeof FailedSnapshot => {
    if (depth > maxDepth) return FailedSnapshot;
    if (value === null) return add(4) ? null : FailedSnapshot;
    switch (typeof value) {
      case "string":
        return addString(value) ? value : FailedSnapshot;
      case "boolean":
        return add(value ? 4 : 5) ? value : FailedSnapshot;
      case "number": {
        // JSON's numeric representation is inherently bounded for one IEEE-754 value. Retain the
        // wire-equivalent null for non-finite values so counting and later serialization agree.
        if (!Number.isFinite(value)) {
          return rejectNonFiniteNumbers ? FailedSnapshot : add(4) ? null : FailedSnapshot;
        }
        const encoded = JSON.stringify(value);
        return encoded !== undefined && add(encoded.length) ? value : FailedSnapshot;
      }
      case "object": {
        if (ancestors.has(value)) return FailedSnapshot;
        const isArray = Array.isArray(value);
        const prototype = Object.getPrototypeOf(value);
        if (prototype !== (isArray ? Array.prototype : Object.prototype) && prototype !== null) {
          return FailedSnapshot;
        }
        if (
          Object.getOwnPropertyDescriptor(value, "toJSON") !== undefined ||
          (prototype !== null && Object.getOwnPropertyDescriptor(prototype, "toJSON") !== undefined)
        ) {
          return FailedSnapshot;
        }
        ancestors.add(value);
        try {
          if (isArray) {
            const lengthDescriptor = Object.getOwnPropertyDescriptor(value, "length");
            if (
              lengthDescriptor === undefined ||
              !("value" in lengthDescriptor) ||
              typeof lengthDescriptor.value !== "number" ||
              !Number.isSafeInteger(lengthDescriptor.value) ||
              lengthDescriptor.value < 0
            ) {
              return FailedSnapshot;
            }
            if (!add(1)) return FailedSnapshot;
            const snapshot: Array<Schema.Json> = [];
            for (let index = 0; index < lengthDescriptor.value; index += 1) {
              if (index > 0 && !add(1)) return FailedSnapshot;
              const descriptor = Object.getOwnPropertyDescriptor(value, index);
              if (descriptor === undefined) {
                if (!add(4)) return FailedSnapshot;
                snapshot.push(null);
                continue;
              }
              if (!("value" in descriptor)) return FailedSnapshot;
              const item = visit(descriptor.value, depth + 1);
              if (item === FailedSnapshot) return FailedSnapshot;
              snapshot.push(item);
            }
            if (!add(1)) return FailedSnapshot;
            Object.setPrototypeOf(snapshot, null);
            return Object.freeze(snapshot);
          }
          if (!add(1)) return FailedSnapshot;
          const entries: Array<readonly [string, Schema.Json]> = [];
          let first = true;
          for (const key in value) {
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (descriptor === undefined || !descriptor.enumerable) continue;
            if (!("value" in descriptor)) return FailedSnapshot;
            if (!first && !add(1)) return FailedSnapshot;
            first = false;
            if (!addString(key) || !add(1)) return FailedSnapshot;
            const item = visit(descriptor.value, depth + 1);
            if (item === FailedSnapshot) return FailedSnapshot;
            entries.push([key, item]);
          }
          if (!add(1)) return FailedSnapshot;
          const snapshot: Record<string, Schema.Json> = Object.fromEntries(entries);
          Object.setPrototypeOf(snapshot, null);
          return Object.freeze(snapshot);
        } finally {
          ancestors.delete(value);
        }
      }
      case "bigint":
      case "function":
      case "symbol":
      case "undefined":
        return FailedSnapshot;
    }
    return FailedSnapshot;
  };

  try {
    const value = visit(root, 0);
    return value === FailedSnapshot ? undefined : { value, bytes: total };
  } catch {
    // Revoked or adversarial proxies and reflection failures are protocol-invalid, never trusted.
    return undefined;
  }
};

/**
 * Normalize provider-owned JSON using JSON serialization's `NaN`/infinity to
 * `null` behavior. Provider events use this form so byte accounting matches
 * their eventual JSON representation.
 */
export const boundedJsonSnapshot = (
  root: unknown,
  maxBytes: number,
  maxDepth = MAX_JSON_DEPTH,
): BoundedJsonSnapshot | undefined => snapshotJson(root, maxBytes, maxDepth, false);

/**
 * Validate canonical JSON without repairing non-finite numbers. Recovery
 * inputs use this form because changing a recorded value would fabricate
 * history.
 */
export const boundedCanonicalJsonSnapshot = (
  root: unknown,
  maxBytes: number,
  maxDepth = MAX_JSON_DEPTH,
): BoundedJsonSnapshot | undefined => snapshotJson(root, maxBytes, maxDepth, true);
