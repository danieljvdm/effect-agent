import { FillCredentialRequest } from "@effect-agent/platform-cloudflare/browser-credentials";
import type {
  BrowserSession,
  CredentialTargetGuard,
} from "@effect-agent/platform-cloudflare/browser-session";
import { Effect, Schema } from "effect";
import type { Frame, Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { CheckoutOwner } from "./checkout-agent.ts";
import { failure } from "./checkout-contract.ts";
import { IndexedControl, IndexedObservation } from "./checkout-indexed-contract.ts";
import { measured } from "./checkout-telemetry.ts";

const Registry = "__effectCheckoutIndexedObservation";

const FrameRead = Schema.Struct({
  text: Schema.String.check(Schema.isMaxLength(24_000)),
  controls: Schema.Array(Schema.Struct({ ...IndexedControl.fields, selector: Schema.String })),
});

const Dispatch = Schema.Literals(["completed", "stale"]);

// Trusted consumer program in Puppeteer's isolated realm. The page cannot replace this registry.
// No selector or executable source is accepted from a model. Nodes never cross a native callback.
const capture = (registry: string, nonce: string, frame: number) => {
  const doc = document;
  const url = location.href;

  const path = (node: Element) => {
    const parts: string[] = [];

    for (let el: Element | null = node; el; el = el.parentElement) {
      const parent = el.parentElement;

      parts.unshift(
        `${el.tagName.toLowerCase()}:nth-child(${parent ? [...parent.children].indexOf(el) + 1 : 1})`,
      );
    }

    return parts.join(" > ");
  };

  const visible = (el: Element) => {
    const r = el.getBoundingClientRect();

    return (
      el.isConnected &&
      el.ownerDocument === doc &&
      doc === document &&
      location.href === url &&
      el.checkVisibility({
        contentVisibilityAuto: true,
        opacityProperty: true,
        visibilityProperty: true,
      }) &&
      r.width > 0 &&
      r.height > 0
    );
  };

  const enabled = (el: Element) => !el.matches(":disabled,[aria-disabled=true],[readonly]");

  const fingerprint = (el: Element) =>
    JSON.stringify([
      el.outerHTML,
      Reflect.get(el, "value"),
      Reflect.get(el, "checked"),
      el instanceof HTMLInputElement ||
      el instanceof HTMLSelectElement ||
      el instanceof HTMLTextAreaElement ||
      el instanceof HTMLButtonElement
        ? [el.form?.action, el.form?.method, el.form?.enctype]
        : null,
    ]);

  const nodes = [
    ...doc.querySelectorAll("a[href],button,input,select,textarea,summary,[role=button]"),
  ]
    .filter(visible)
    .slice(0, 128);

  const states = nodes.map(fingerprint);

  const valid = (index: number, field?: Element) => {
    const el = nodes[index];

    if (
      !el ||
      (field !== undefined && field !== el) ||
      !visible(el) ||
      !enabled(el) ||
      fingerprint(el) !== states[index]
    )
      return false;
    const r = el.getBoundingClientRect();
    const x = Math.max(0, Math.min(innerWidth - 1, r.x + r.width / 2));
    const y = Math.max(0, Math.min(innerHeight - 1, r.y + r.height / 2));
    const hit = doc.elementFromPoint(x, y);

    return hit !== null && (hit === el || el.contains(hit));
  };

  Reflect.set(globalThis, registry, {
    nonce,
    doc,
    nodes,
    valid,
    act(index: number, operation: string, value: string) {
      const el = nodes[index];

      if (!el || !visible(el)) return "stale";
      el.scrollIntoView({ block: "center", inline: "center" });
      if (!valid(index)) return "stale";
      if (operation === "CLICK" && el instanceof HTMLElement) el.click();
      else if (
        (operation === "TYPE" || operation === "SELECT") &&
        (el instanceof HTMLInputElement ||
          el instanceof HTMLTextAreaElement ||
          el instanceof HTMLSelectElement)
      ) {
        if (
          operation === "SELECT" &&
          (!(el instanceof HTMLSelectElement) ||
            [...el.options].filter((o) => o.value === value && !o.matches(":disabled")).length !==
              1)
        )
          return "stale";

        const prototype =
          el instanceof HTMLInputElement
            ? HTMLInputElement.prototype
            : el instanceof HTMLTextAreaElement
              ? HTMLTextAreaElement.prototype
              : HTMLSelectElement.prototype;

        Object.getOwnPropertyDescriptor(prototype, "value")?.set?.call(el, value);
        el.dispatchEvent(new Event("input", { bubbles: true }));
        el.dispatchEvent(new Event("change", { bubbles: true }));
      } else return "stale";

      return "completed";
    },
  });

  return {
    text: (doc.body?.innerText ?? "").slice(0, 24_000),
    controls: nodes.map((el, index) => {
      const input = el instanceof HTMLInputElement || el instanceof HTMLTextAreaElement;
      const select = el instanceof HTMLSelectElement;
      const autocomplete = input || select ? el.autocomplete : "";

      const secret =
        /^(username|current-password|new-password|cc-)/.test(autocomplete) ||
        (el instanceof HTMLInputElement && el.type === "password");

      const operations = !enabled(el)
        ? []
        : select
          ? ["SELECT"]
          : input &&
              !(
                el instanceof HTMLInputElement &&
                ["radio", "checkbox", "submit", "button"].includes(el.type)
              )
            ? secret
              ? []
              : ["TYPE"]
            : ["CLICK"];

      return {
        index: `${frame}:${index}`,
        frame,
        document: nonce,
        node: String(index),
        kind: `${el.tagName.toLowerCase()}${el instanceof HTMLInputElement ? `:${el.type}` : ""}`,
        label: (
          el.getAttribute("aria-label") ||
          (input || select
            ? [...(el.labels ?? [])].map((l) => l.innerText).join(" ")
            : el.textContent) ||
          el.getAttribute("name") ||
          ""
        )
          .trim()
          .slice(0, 2048),
        value: secret
          ? "[saved credential]"
          : String(Reflect.get(el, "value") ?? "").slice(0, 2048),
        checked: Reflect.get(el, "checked") === true,
        disabled: !enabled(el),
        readOnly: Reflect.get(el, "readOnly") === true,
        href: el instanceof HTMLAnchorElement ? el.href.slice(0, 2048) : "",
        form: input || select ? (el.form ? path(el.form) : "") : "",
        recipient:
          input || select || el instanceof HTMLButtonElement
            ? (el instanceof HTMLButtonElement || el instanceof HTMLInputElement) &&
              el.hasAttribute("formaction")
              ? el.formAction
              : (el.form?.action ?? "")
            : "",
        autocomplete,
        operations,
        selector: path(el),
        options: select
          ? [...el.options].slice(0, 64).map((o) => ({
              value: o.value.slice(0, 2048),
              label: o.text.slice(0, 2048),
              disabled: o.matches(":disabled"),
            }))
          : [],
      };
    }),
  };
};

const framePath = async (frame: Frame, scroll: boolean) => {
  const path: string[] = [];

  for (let current: Frame | null = frame; current?.parentFrame(); current = current.parentFrame()) {
    const element = await current.frameElement();

    if (element === null) return undefined;
    try {
      const selector = await element.evaluate((el, shouldScroll) => {
        if (shouldScroll) el.scrollIntoView({ block: "center" });
        const r = el.getBoundingClientRect();

        if (
          !el.checkVisibility({ opacityProperty: true, visibilityProperty: true }) ||
          r.width <= 0 ||
          r.height <= 0
        )
          return null;
        if (shouldScroll) {
          const hit = document.elementFromPoint(
            Math.max(0, Math.min(innerWidth - 1, r.x + r.width / 2)),
            Math.max(0, Math.min(innerHeight - 1, r.y + r.height / 2)),
          );

          if (hit !== el) return null;
        }
        const parts: string[] = [];

        for (let node: Element | null = el; node; node = node.parentElement) {
          const parent = node.parentElement;

          parts.unshift(
            `${node.tagName.toLowerCase()}:nth-child(${parent ? [...parent.children].indexOf(node) + 1 : 1})`,
          );
        }

        return parts.join(" > ");
      }, scroll);

      if (selector === null) return undefined;
      path.unshift(selector);
    } finally {
      await element.dispose();
    }
  }

  return path;
};

export const observeIndexed = Effect.fnUntraced(function* (
  session: Pick<BrowserSession, "run">,
  origins: ReadonlyArray<string>,
) {
  const owner = yield* CheckoutOwner;

  const result = yield* measured(
    "observation",
    "indexed.observe",
    session.run(owner.authorize, async (page) => {
      const frames = [];

      for (const [index, frame] of page.frames().entries()) {
        if (!origins.includes(new URL(frame.url()).origin)) continue;
        const path = await framePath(frame, false);

        if (path === undefined) continue;
        const nonce = crypto.randomUUID();
        const raw = await frame.isolatedRealm().evaluate(capture, Registry, nonce, index);
        const value = Schema.decodeUnknownSync(FrameRead)(raw);

        frames.push({ index, url: frame.url(), document: nonce, path, ...value });
        if (frames.length === 8) break;
      }

      return { url: page.url(), frames };
    }),
    {},
    (result) => ({ observationBytes: new TextEncoder().encode(JSON.stringify(result)).byteLength }),
  );

  const observation = yield* Schema.decodeEffect(IndexedObservation)({
    url: result.url,
    frames: result.frames.map(({ index, url, document, text }) => ({ index, url, document, text })),
    controls: result.frames
      .flatMap((f) => f.controls.map(({ selector: _selector, ...control }) => control))
      .slice(0, 128),
  });

  return { observation, frames: result.frames };
});

export type Snapshot = Effect.Success<ReturnType<typeof observeIndexed>>;

export const dispatchIndexed = Effect.fnUntraced(function* (
  session: Pick<BrowserSession, "run">,
  snapshot: Snapshot,
  control: IndexedControl,
  operation: "CLICK" | "TYPE" | "SELECT",
  value: string,
  origins: ReadonlyArray<string>,
) {
  const owner = yield* CheckoutOwner;

  return yield* session.run(owner.authorize, async (page) => {
    const frame = page.frames()[control.frame];
    const source = snapshot.frames.find((f) => f.index === control.frame);

    if (
      !frame ||
      !source ||
      page.url() !== snapshot.observation.url ||
      frame.url() !== source.url ||
      (await framePath(frame, true)) === undefined
    )
      return "stale";
    if (operation === "CLICK" && control.href && !origins.includes(new URL(control.href).origin))
      return "stale";

    if (control.recipient && !origins.includes(new URL(control.recipient).origin)) return "stale";

    const raw = await frame.isolatedRealm().evaluate(
      (registry, nonce, index, op, text) => {
        const state = Reflect.get(globalThis, registry);

        if (!state || state.nonce !== nonce) return "stale";

        return state.act(index, op, text);
      },
      Registry,
      control.document,
      Number(control.node),
      operation,
      value,
    );

    return Schema.decodeUnknownSync(Dispatch)(raw);
  });
});

export const credentialTarget = (
  snapshot: Snapshot,
  controls: ReadonlyArray<IndexedControl>,
  credential: string,
  kind: "login" | "card",
) => {
  const first = controls[0];
  const source = snapshot.frames.find((f) => f.index === first?.frame);

  if (!first || !source) return undefined;

  const roles: Record<string, (typeof FillCredentialRequest.Type.fields)[number]["role"]> = {
    username: "username",
    email: "username",
    "current-password": "password",
    "new-password": "password",
    "cc-name": "card-name",
    "cc-number": "card-number",
    "cc-exp": "card-expiry",
    "cc-exp-month": "card-expiry-month",
    "cc-exp-year": "card-expiry-year",
    "cc-csc": "card-security-code",
  };

  const fields = controls.flatMap((control) => {
    const privateControl = source.controls.find((c) => c.index === control.index);
    const role = roles[control.autocomplete];

    return privateControl && role ? [{ selector: privateControl.selector, role }] : [];
  });

  if (
    fields.length !== controls.length ||
    fields.length === 0 ||
    new Set(fields.map((f) => f.role)).size !== fields.length
  )
    return undefined;

  const request = Schema.decodeSync(FillCredentialRequest)({
    credential,
    kind,
    frame: source.path,
    fields,
  });

  const guard: CredentialTargetGuard = async (frame) => {
    if (frame.url() !== source.url || (await framePath(frame, true)) === undefined)
      throw failure("authority", "Credential frame changed");

    return frame.isolatedRealm().evaluateHandle(
      (registry, nonce, indices) => {
        const state = Reflect.get(globalThis, registry);

        return (field: unknown, index: number) => {
          if (!state || state.nonce !== nonce || !(field instanceof Element)) return false;
          field.scrollIntoView({ block: "center" });

          return state.valid(indices[index], field);
        };
      },
      Registry,
      first.document,
      controls.map((c) => Number(c.node)),
    );
  };

  return { request, guard };
};

/** Drop renderer references before disconnecting the scoped attachment, including on failure. */
export const releaseIndexed = (session: Pick<BrowserSession, "run">) =>
  session
    .run(Effect.void, async (page: Page) => {
      for (const frame of page.frames())
        await frame
          .isolatedRealm()
          .evaluate((registry) => {
            Reflect.deleteProperty(globalThis, registry);
          }, Registry)
          .catch(() => {});
    })
    .pipe(Effect.ignore, Effect.timeoutOption("2 seconds"), Effect.asVoid);
