import type { BrowserSession } from "@effect-agent/platform-cloudflare/browser-session";
import * as NativeBrowser from "@effect-agent/platform-cloudflare/browser-use";
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

  const asLabError = (error: { readonly message: string }) =>
    new LabError({ code: "browser", message: error.message });

  const controller = yield* NativeBrowser.make(session, {
    authorize: () => Effect.void,
    maxActions: 100,
    maxReturnedBytes: 256 * 1024,
  }).pipe(Effect.mapError(asLabError));

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
    trace.measure(
      "observation",
      "Read current controls",
      controller.actions.observe.pipe(Effect.mapError(asLabError)),
    );

  const act = Effect.fnUntraced(function* (values: ReadonlyArray<Action>) {
    const result = yield* trace.measure(
      "action",
      "Native browser actions",
      controller.actions.act(values).pipe(Effect.mapError(asLabError)),
    );

    yield* capture().pipe(Effect.ignore);

    return result;
  });

  return {
    native,
    prepare: trace.measure(
      "setup",
      "Load task board",
      native(async (page) => {
        await page.setViewport({ width: 1100, height: 740 });
        await page.goto(`data:text/html;charset=utf-8,${encodeURIComponent(fixtureHtml())}`, {
          waitUntil: "domcontentloaded",
        });
        await page.waitForSelector("#edit-1", { visible: true, timeout: 5_000 });

        return await page.$eval(
          "dialog",
          (element) => typeof Reflect.get(element, "showModal") === "function",
        );
      }).pipe(
        Effect.flatMap((supported) =>
          supported
            ? Effect.void
            : Effect.fail(
                new LabError({
                  code: "configuration",
                  message: "The task board requires native HTML dialog support.",
                }),
              ),
        ),
      ),
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
    actionsLayer: Layer.merge(
      Layer.succeed(BrowserUse.BrowserActions, {
        observe: controller.actions.observe,
        act: (values) =>
          act(values).pipe(
            Effect.mapError(
              (error) =>
                new BrowserUse.BrowserUseError({ code: "browser", message: error.message }),
            ),
          ),
      }),
      Layer.succeed(BrowserUse.BrowserControl, controller.control),
    ),
    inspect: controller.control.inspect,
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
    const observed = yield* browser.observe();
    const control = observed.controls.find((control) => control.attributes?.id === action.ref);

    if (control === undefined)
      return yield* new LabError({
        code: "browser",
        message: `Missing diagnostic control ${action.ref}`,
      });
    const result = yield* browser.act([{ ...action, ref: control.ref }]);

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
