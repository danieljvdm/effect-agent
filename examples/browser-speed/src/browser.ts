import type { BrowserSession } from "@effect-agent/platform-cloudflare/browser-session";
import { Context, Effect, Layer, Schema } from "effect";
import * as BrowserUse from "effect-agent/browser-use";
import { Action, Observation, ActionResult } from "effect-agent/browser-use";
import { Tool, Toolkit } from "effect/unstable/ai";
import type { Page } from "puppeteer-core/lib/esm/puppeteer/puppeteer-core-browser.js";

import { Board, LabError, type Scenario } from "./contract.ts";
import { fixtureHtml } from "./fixture.ts";
import { Trace } from "./telemetry.ts";

export { Action, Observation, ActionResult };

export const TaskResult = Schema.Struct({ message: Schema.String });

export const finishTool = Tool.make("finish", {
  description:
    "Finish the run after the observed task board shows all requested changes are saved.",
  parameters: TaskResult,
  success: TaskResult,
});

export const completionTools = Toolkit.make(finishTool);
export const completionLayer = completionTools.toLayer({ finish: Effect.succeed });

export const makeBrowser = Effect.fnUntraced(function* (
  session: Pick<BrowserSession, "run">,
  screenshots: boolean,
  image: (value: string) => void,
) {
  const trace = yield* Trace;
  let observed = new Set<string>();
  let actions = 0;

  const native = <A>(action: (page: Page) => Promise<A>) =>
    session.run(Effect.void, action).pipe(
      Effect.mapError(
        (error) =>
          new LabError({
            code: "browser",
            message: `Browser ${error.reason}; dispatch ${error.dispatch}. Observe before attempting another action.`,
          }),
      ),
    );

  const capture = (force = false): Effect.Effect<void, LabError> =>
    screenshots || force
      ? trace
          .measure(
            "capture",
            "Screenshot",
            native((page) => page.screenshot({ encoding: "base64", type: "jpeg", quality: 65 })),
          )
          .pipe(
            Effect.tap((value) => Effect.sync(() => image(`data:image/jpeg;base64,${value}`))),
            Effect.asVoid,
          )
      : Effect.void;

  const observe = () =>
    trace
      .measure(
        "observation",
        "Read visible controls",
        native((page) =>
          page.evaluate(() => {
            const dialog = document.querySelector("dialog[open]");

            const controls = Array.from(
              document.querySelectorAll("button,input,select,textarea"),
            ).flatMap((node) => {
              if (
                !(node instanceof HTMLElement) ||
                !node.id ||
                !node.checkVisibility() ||
                (dialog !== null && !dialog.contains(node)) ||
                node.matches(":disabled")
              )
                return [];

              const field =
                node instanceof HTMLInputElement ||
                node instanceof HTMLSelectElement ||
                node instanceof HTMLTextAreaElement;

              return [
                {
                  ref: node.id,
                  kind: node.tagName.toLowerCase(),
                  name:
                    node.getAttribute("aria-label") ??
                    node.closest("label")?.textContent?.trim() ??
                    node.textContent?.trim() ??
                    "",
                  value: field ? node.value : "",
                  options:
                    node instanceof HTMLSelectElement
                      ? Array.from(node.options).map((option) => option.value)
                      : [],
                },
              ];
            });

            return {
              text: (dialog instanceof HTMLElement
                ? dialog.innerText
                : document.body.innerText
              ).slice(0, 12_000),
              controls,
            };
          }),
        ),
        (value) => ({
          bytes: new TextEncoder().encode(
            Schema.encodeSync(Schema.fromJsonString(Observation))(value),
          ).length,
        }),
      )
      .pipe(
        Effect.tap((value) =>
          Effect.sync(() => {
            observed = new Set(value.controls.map((control) => control.ref));
          }),
        ),
      );

  const action = Effect.fnUntraced(function* (value: Action) {
    if (!observed.has(value.ref))
      return yield* new LabError({
        code: "invalid",
        message: "Control was not observed. Observe before acting.",
      });
    if (++actions > 100)
      return yield* new LabError({ code: "invalid", message: "The 100-action limit was reached." });

    yield* trace.measure(
      "action",
      `${value.kind} #${value.ref}`,
      native(async (page) => {
        const visible = await page.$eval(`#${value.ref}`, (node) => {
          const dialog = document.querySelector("dialog[open]");

          return (
            node instanceof HTMLElement &&
            node.checkVisibility() &&
            !node.matches(":disabled") &&
            (dialog === null || dialog.contains(node))
          );
        });

        if (!visible) throw new Error("Control is no longer actionable");
        switch (value.kind) {
          case "click":
            await page.click(`#${value.ref}`);
            break;
          case "fill":
            await page.locator(`#${value.ref}`).fill(value.value);
            break;
          case "select": {
            const selected = await page.select(`#${value.ref}`, value.value);

            if (!selected.includes(value.value)) throw new Error("Option unavailable");
            break;
          }
        }
      }),
    );
  });

  const act = Effect.fnUntraced(function* (values: ReadonlyArray<Action>) {
    let completed = 0;
    let error: string | null = null;

    for (const value of values) {
      const result = yield* action(value).pipe(Effect.result);

      if (result._tag === "Failure") {
        error = result.failure.message;
        break;
      }
      completed++;
    }

    const observation = yield* observe().pipe(
      Effect.catch((failure) => {
        error ??= `Actions completed; observation failed. Use observe, do not replay. ${failure.message}`;

        return Effect.succeed(null);
      }),
    );

    yield* capture().pipe(Effect.catch(() => Effect.void));

    return { completed, error, observation };
  });

  return {
    native,
    prepare: trace.measure(
      "setup",
      "Load task board",
      native(async (page) => {
        await page.setViewport({ width: 1100, height: 740 });
        await page.setContent(fixtureHtml(), { waitUntil: "domcontentloaded" });
        await page.waitForSelector("#edit-1", { visible: true, timeout: 5_000 });
      }),
    ),
    observe,
    act,
    capture,
    readBoard: trace
      .measure(
        "verify",
        "Read independent task ledger",
        native((page) => page.$eval("#board-state", (element) => element.textContent ?? "")),
      )
      .pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(Board))),
        Effect.mapError(
          () => new LabError({ code: "browser", message: "Could not validate the task ledger." }),
        ),
      ),
    actionsLayer: Layer.succeed(BrowserUse.BrowserActions, {
      observe: observe().pipe(
        Effect.mapError(
          (error) => new BrowserUse.BrowserUseError({ code: "browser", message: error.message }),
        ),
      ),
      act,
    }),
  };
});

export class Browser extends Context.Service<
  Browser,
  Effect.Success<ReturnType<typeof makeBrowser>>
>()("browser-speed/Browser") {}

/** A fixed diagnostic baseline uses the same input and observation path as individual tools. */
export const scripted = Effect.fnUntraced(function* (scenario: Scenario) {
  const browser = yield* Browser;

  const step = Effect.fnUntraced(function* (action: Action) {
    const result = yield* browser.act([action]);

    if (result.error !== null)
      return yield* new LabError({ code: "browser", message: result.error });
  });

  const create = Effect.fnUntraced(function* (title: string) {
    yield* step({ kind: "click", ref: "new-task" });
    yield* step({ kind: "fill", ref: "title", value: title });
    yield* step({ kind: "select", ref: "assignee", value: "Alex" });
    yield* step({ kind: "select", ref: "priority", value: "High" });
    yield* step({ kind: "select", ref: "status", value: "Todo" });
    yield* step({ kind: "click", ref: "save" });
  });

  if (scenario === "create") yield* create("Ship demo");
  else if (scenario === "triage") {
    yield* step({ kind: "select", ref: "filter", value: "Sam" });
    for (const id of [1, 3]) {
      yield* step({ kind: "click", ref: `edit-${id}` });
      yield* step({ kind: "select", ref: "priority", value: "High" });
      yield* step({ kind: "select", ref: "status", value: "Doing" });
      yield* step({ kind: "click", ref: "save" });
    }
  } else if (scenario === "batch") {
    for (const title of ["Write launch notes", "Record demo", "Publish release"])
      yield* create(title);
    yield* step({ kind: "click", ref: "edit-5" });
    yield* step({ kind: "select", ref: "status", value: "Done" });
    yield* step({ kind: "click", ref: "save" });
  } else
    return yield* new LabError({
      code: "invalid",
      message: "Scripted mode requires a preset task.",
    });
});
