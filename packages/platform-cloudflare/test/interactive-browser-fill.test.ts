import {
  BrowserFillRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "@effect-agent/sandbox";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import { vi } from "vite-plus/test";

import {
  BrowserRunInteractiveBinding,
  browserRunInteractiveLayer,
} from "../src/interactive-browser.ts";

const sdk = vi.hoisted(() => ({ launch: vi.fn<() => Promise<object>>() }));

vi.mock("@cloudflare/puppeteer", () => ({ default: sdk }));

// Only the SDK transport is replaced: fill runs the production $eval callback.
const nativeLayer = (element: object) => {
  const page = {
    $eval: async (
      selector: string,
      evaluate: (field: object, value: string) => void,
      value: string,
    ) => {
      if (selector !== "#field") throw new Error("No element matches the selector");
      evaluate(element, value);
    },
    url: () => "https://example.com/form",
    close: async () => {},
    setBypassServiceWorker: async () => {},
    setRequestInterception: async () => {},
    on: () => {},
    off: () => {},
  };

  sdk.launch.mockResolvedValue({
    createBrowserContext: async () => ({
      newPage: async () => page,
      close: async () => {},
    }),
    sessionId: () => "fill-regression",
    isConnected: () => true,
    on: () => {},
    off: () => {},
    close: async () => {},
  });

  const unusedRpc = async (): Promise<Response> => {
    throw new Error("The SDK transport must not call Browser Run");
  };

  return browserRunInteractiveLayer().pipe(
    Layer.provide(
      BrowserRunInteractiveBinding.layer({
        browser: { fetch: unusedRpc, quickAction: unusedRpc },
      }),
    ),
  );
};

const open = Effect.gen(function* () {
  const browser = yield* InteractiveBrowser;

  return yield* browser.open(
    InteractiveBrowserPolicy.make({
      allowedHosts: ["example.com"],
      maxActions: 8,
      maxElapsedMillis: 5_000,
      maxReturnedBytes: 1_024,
    }),
  );
});

class Field extends EventTarget {
  #value = "initial";
  focused = false;

  get value() {
    return this.#value;
  }

  set value(value: string) {
    this.#value = value;
  }

  focus() {
    this.focused = true;
  }
}

class InheritedField extends Field {}

describe("Browser Run native field filling", () => {
  it.effect.each([Field, InheritedField])(
    "notifies controlled fields when replacing and clearing their value (%#)",
    (FieldType) =>
      Effect.gen(function* () {
        const field = new FieldType();
        let trackedValue = field.value;
        let state = field.value;
        let changes = 0;

        // React's own setter tracks assignments before input-event change detection.
        Object.defineProperty(field, "value", {
          get: () => Reflect.get(Field.prototype, "value", field),
          set: (value: string) => {
            trackedValue = value;
            Reflect.set(Field.prototype, "value", value, field);
          },
        });
        field.addEventListener("input", () => {
          if (field.value !== trackedValue) {
            trackedValue = field.value;
            state = field.value;
            changes += 1;
          }
        });

        yield* Effect.gen(function* () {
          const handle = yield* open;

          for (const value of ["agent-first", "replacement", ""]) {
            yield* handle.fill(BrowserFillRequest.make({ selector: "#field", value }));
            expect(state).toBe(value);
            expect(field.value).toBe(value);
          }
        }).pipe(Effect.scoped, Effect.provide(nativeLayer(field)));

        expect(changes).toBe(3);
      }),
  );

  it.effect("focuses ordinary fields and emits bubbling input then change with the new value", () =>
    Effect.gen(function* () {
      const field = new Field();
      const events: Array<{
        type: string;
        bubbles: boolean;
        value: string;
        focused: boolean;
      }> = [];

      for (const type of ["input", "change"]) {
        field.addEventListener(type, (event) => {
          events.push({
            type: event.type,
            bubbles: event.bubbles,
            value: field.value,
            focused: field.focused,
          });
        });
      }

      yield* Effect.gen(function* () {
        const handle = yield* open;

        yield* handle.fill(BrowserFillRequest.make({ selector: "#field", value: "agent-first" }));
      }).pipe(Effect.scoped, Effect.provide(nativeLayer(field)));

      expect(events).toEqual([
        { type: "input", bubbles: true, value: "agent-first", focused: true },
        { type: "change", bubbles: true, value: "agent-first", focused: true },
      ]);
    }),
  );

  it.effect.each([
    {},
    { value: "own data property" },
    new (class {
      get value() {
        return "read only";
      }
    })(),
  ])("rejects nonfillable elements with a typed fill failure (%#)", (element) =>
    Effect.gen(function* () {
      const handle = yield* open;
      const error = yield* handle
        .fill(BrowserFillRequest.make({ selector: "#field", value: "agent-first" }))
        .pipe(Effect.flip);

      expect(error).toMatchObject({
        _tag: "InteractiveBrowserActionError",
        operation: "fill",
        cause: new Error("The selector did not resolve to a fillable field"),
      });
    }).pipe(Effect.scoped, Effect.provide(nativeLayer(element))),
  );
});
