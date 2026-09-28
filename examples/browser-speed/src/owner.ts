import {
  BrowserSessionReference,
  BrowserSessions,
} from "@effect-agent/platform-cloudflare/browser-session";
import { Cause, Clock, Effect, Exit, Fiber, Option, Redacted, Schema } from "effect";

import { Browser, makeBrowser } from "./browser.ts";
import {
  LabError,
  browserCommandTimeoutMillis,
  defaultChallenge,
  racePrompt,
  Report,
  scenarios,
  type ModelApi,
  type ModelId,
  type RunInput,
  type Snapshot,
} from "./contract.ts";
import type { connectKitesurf } from "./kitesurf.ts";
import { executeTask } from "./runner.ts";
import { makeTrace, Trace } from "./telemetry.ts";
import { normalizeTitle } from "./wikipedia.ts";

export const Control = Schema.Struct({
  version: Schema.Literal(1),
  reference: Schema.NullOr(Schema.toCodecJson(BrowserSessionReference)),
  active: Schema.NullOr(Schema.String),
  lastRun: Schema.NullOr(Schema.String),
  admitted: Schema.Array(Schema.String).check(Schema.isMaxLength(100)),
  report: Schema.NullOr(Report),
});

export type Control = typeof Control.Type;

export const emptyControl: Control = {
  version: 1,
  reference: null,
  active: null,
  lastRun: null,
  admitted: [],
  report: null,
};

export interface OwnerStore {
  readonly read: () => Control;
  /** Implementations must atomically persist the whole control record. */
  readonly write: (state: Control, transition: string) => void;
  readonly alarm: (at: number | null) => Effect.Effect<void, LabError>;
}

/** The host alone owns references, run admission and cleanup. Lost actions are never replayed. */
export const makeOwner = (
  store: OwnerStore,
  config: {
    model: string;
    apiKey: string;
    apiUrl?: string;
    apiType?: typeof ModelApi.Type;
    browserConfigured?: boolean;
    models?: ReadonlyArray<{
      model: ModelId;
      label: string;
      apiKey: string;
      apiUrl?: string;
      apiType?: typeof ModelApi.Type;
    }>;
    jevApiKey?: string;
    kitesurf?: (
      retainClose: Parameters<typeof connectKitesurf>[1],
    ) => ReturnType<typeof connectKitesurf>;
  },
) => {
  let trace: Trace["Service"] | undefined;
  let running: Fiber.Fiber<void, LabError> | undefined;
  let liveViewUrl: string | null = null;
  let image: string | null = null;
  let notice: string | null = null;
  let closeKitesurf: Effect.Effect<void, LabError> | undefined;

  const patch = (change: Partial<Control>, transition: string) =>
    store.write({ ...store.read(), ...change }, transition);

  const provider = <A, E extends { readonly _tag: string }, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError(
        (error) =>
          new LabError({
            code: "browser",
            message: `Browser operation failed (${error._tag}). Close the browser before trying a fresh run.`,
          }),
      ),
    );

  const snapshot = Effect.fnUntraced(function* (): Effect.fn.Return<typeof Snapshot.Type> {
    const state = store.read();
    const now = yield* Clock.currentTimeMillis;

    return {
      ready:
        (state.reference !== null && state.reference.expiresAt > now) ||
        closeKitesurf !== undefined,
      busy: state.active !== null,
      canClose:
        running === undefined &&
        (state.reference !== null || state.active !== null || state.report?.cleanup === "failed"),
      model: config.model,
      browserConfigured: config.browserConfigured ?? true,
      agentConfigured: config.apiKey.length > 0,
      models: (config.models ?? []).map(({ model, label, apiKey }) => ({
        id: model,
        label,
        configured: apiKey.length > 0,
      })),
      jevConfigured: Boolean(config.jevApiKey),
      report: trace?.snapshot() ?? state.report,
      liveViewUrl,
      image,
      notice:
        state.active !== null && running === undefined
          ? "The previous request is unresolved. Close the browser to recover; it will not be replayed."
          : notice,
    };
  });

  const closeReference = Effect.fnUntraced(function* () {
    if (closeKitesurf !== undefined) {
      yield* closeKitesurf;
      closeKitesurf = undefined;
    }
    const reference = store.read().reference;

    if (reference !== null) {
      const sessions = yield* BrowserSessions;

      yield* provider(sessions.close(reference.sessionId));
      patch({ reference: null }, "closed");
    }
    liveViewUrl = null;
    yield* store.alarm(null);
  });

  const allocate = Effect.fnUntraced(function* () {
    const sessions = yield* BrowserSessions;
    const now = yield* Clock.currentTimeMillis;

    yield* store.alarm(now + 600_000);

    return yield* provider(
      sessions.create(
        {
          maxElapsedMillis: 600_000,
          keepAliveMillis: 600_000,
          commandTimeoutMillis: browserCommandTimeoutMillis,
        },
        (reference) => Effect.sync(() => patch({ reference }, "retained")),
      ),
    );
  });

  const stop = Effect.fnUntraced(function* () {
    if (running !== undefined) yield* Fiber.interrupt(running);
  });

  const close = Effect.fnUntraced(function* () {
    if (running !== undefined)
      return yield* new LabError({
        code: "busy",
        message: "Stop the active run before closing its browser.",
      });
    yield* closeReference();
    const state = store.read();

    patch(
      {
        active: null,
        report:
          state.report?.status === "running"
            ? {
                ...state.report,
                status: "cancelled",
                cleanup: "closed",
                message: "Lost request closed. No actions were replayed.",
              }
            : state.report === null
              ? null
              : { ...state.report, cleanup: "closed" },
      },
      "released",
    );
    trace = undefined;
  });

  const run = Effect.fnUntraced(function* (requested: RunInput) {
    const challenge = requested.wikipedia ?? defaultChallenge;

    const wikipedia = {
      start: normalizeTitle(challenge.start),
      target: normalizeTitle(challenge.target),
    };

    if (
      requested.scenario === "wikipedia" &&
      (requested.mode !== "agent" ||
        !wikipedia.start ||
        !wikipedia.target ||
        wikipedia.start === wikipedia.target)
    )
      return yield* new LabError({
        code: "invalid",
        message:
          "A Wikipedia race needs two different article titles and individual agent actions.",
      });

    const jevOnly = requested.wikiDriver === "jev";

    if (
      jevOnly &&
      (requested.scenario !== "wikipedia" ||
        requested.mode !== "agent" ||
        requested.model !== undefined ||
        requested.grounding !== undefined ||
        requested.reasoning !== undefined ||
        requested.serviceTier !== undefined)
    )
      return yield* new LabError({
        code: "invalid",
        message:
          "Jev-only routing is for Wikipedia races; omit planner model, grounding, reasoning and service tier settings.",
      });

    const openai =
      !jevOnly &&
      requested.mode !== "scripted" &&
      !(requested.model ?? config.model).startsWith("@cf/");

    if (!openai && (requested.reasoning !== undefined || requested.serviceTier !== undefined))
      return yield* new LabError({
        code: "invalid",
        message: "Reasoning and service tier are available only for OpenAI agent runs.",
      });

    const { temperature: _legacyTemperature, ...settings } = requested;

    const input: RunInput = {
      ...settings,
      engine: requested.engine ?? "chromium",
      ...(requested.scenario === "wikipedia" ? { wikipedia } : {}),
      ...(openai
        ? { reasoning: requested.reasoning ?? "none", serviceTier: requested.serviceTier ?? "fast" }
        : {}),
      prompt:
        requested.scenario === "wikipedia"
          ? racePrompt(wikipedia)
          : (scenarios.find((scenario) => scenario.id === requested.scenario)?.prompt ??
            requested.prompt),
    };

    const state = store.read();

    if (state.active !== null || state.admitted.includes(input.id) || input.id === state.lastRun)
      return yield* new LabError({
        code: "busy",
        message: "This run is active or was already admitted. It will not be replayed.",
      });
    if (state.admitted.length >= 100)
      return yield* new LabError({
        code: "invalid",
        message:
          "This tab has reached its 100-run limit. Close the browser, export your results, and reload to start a new session.",
      });
    if (state.report?.cleanup === "failed")
      return yield* new LabError({
        code: "busy",
        message: "Close the browser to resolve the previous cleanup failure before another run.",
      });

    if (input.engine === "kitesurf" && config.kitesurf === undefined)
      return yield* new LabError({
        code: "configuration",
        message: "Kitesurf is not configured in this host.",
      });

    const selectedModel =
      input.model === undefined
        ? config
        : config.models?.find((candidate) => candidate.model === input.model);

    if (selectedModel === undefined)
      return yield* new LabError({
        code: "configuration",
        message: "This model is not available in the lab.",
      });
    if (!jevOnly && input.mode !== "scripted" && !selectedModel.apiKey)
      return yield* new LabError({
        code: "configuration",
        message: "The selected model's API key is not configured in the Worker.",
      });
    if ((jevOnly || (input.mode !== "scripted" && input.grounding === "jev")) && !config.jevApiKey)
      return yield* new LabError({
        code: "configuration",
        message: "Set TYPESAFE_API_KEY in the Worker to use Jev.",
      });
    if (input.scenario === "custom" && (input.mode === "scripted" || !input.prompt.trim()))
      return yield* new LabError({
        code: "invalid",
        message: "Free-form requests need a prompt and an agent mode.",
      });
    trace = yield* makeTrace(
      input,
      jevOnly ? "jev-latest" : input.mode === "scripted" ? "none" : selectedModel.model,
    );
    const current = trace;

    image = null;
    notice = null;
    liveViewUrl = null;
    patch(
      {
        active: input.id,
        lastRun: input.id,
        admitted: [...state.admitted, input.id],
        report: current.snapshot(),
      },
      "admitted",
    );

    const attempt = Effect.fnUntraced(function* (ordinal: number) {
      const sessions = yield* BrowserSessions;

      current.update({
        message:
          ordinal === 1
            ? "Preparing browser and starting page…"
            : "Retrying browser preparation · attempt 2 of 2…",
      });
      yield* current.measure("cleanup", "Close previous browser", closeReference());

      const session = yield* Effect.gen(function* () {
        if (input.engine === "kitesurf") {
          const open = config.kitesurf;

          if (open === undefined)
            return yield* new LabError({
              code: "configuration",
              message: "Kitesurf is unavailable.",
            });
          yield* store.alarm((yield* Clock.currentTimeMillis) + 600_000);

          return yield* current.measure(
            "setup",
            `Connect Kitesurf · preparation ${ordinal}/2`,
            open((close) => {
              closeKitesurf = close;
            }),
          );
        }
        yield* current.measure("setup", `Launch browser · preparation ${ordinal}/2`, allocate());
        const reference = store.read().reference;

        if (reference === null)
          return yield* new LabError({
            code: "expired",
            message: "The browser is no longer available.",
          });

        return yield* current.measure(
          "setup",
          "Attach to browser",
          provider(sessions.attach(reference)),
        );
      });

      const browser = yield* makeBrowser(session, input.screenshots, (value) => {
        image = value;
      });

      const identity = yield* current.measure(
        "setup",
        "Verify browser engine",
        "identity" in session
          ? Effect.succeed(session.identity)
          : browser.native(async (page) => {
              const cdp = await page.createCDPSession();

              try {
                return await cdp.send("Browser.getVersion");
              } finally {
                await cdp.detach();
              }
            }),
      );

      current.update({
        browserVersion: identity.product,
        browserRevision: identity.revision,
        browserUserAgent: identity.userAgent,
      });
      if ((input.engine === "kitesurf") !== (identity.revision === "@kitesurf"))
        return yield* new LabError({
          code: "configuration",
          message:
            "The provider returned a different browser engine. This run cannot be benchmarked.",
        });

      if (input.liveView && "getReadOnlyLiveView" in session)
        yield* current
          .measure(
            "setup",
            "Connect read-only Live View",
            provider(
              session.getReadOnlyLiveView(Effect.void, { mode: "tab", expiresInMs: 300_000 }),
            ),
          )
          .pipe(
            Effect.tap((view) =>
              Effect.sync(() => {
                liveViewUrl = Redacted.value(view.devtoolsFrontendUrl);
              }),
            ),
            Effect.catch(() =>
              Effect.sync(() => {
                notice = "Live View is unavailable. Enable screenshots to see action snapshots.";
              }),
            ),
          );
      else if (input.liveView)
        notice =
          "Kitesurf has no persistent Live View. Enable screenshots to see action snapshots.";
      yield* executeTask(
        input,
        selectedModel.model,
        selectedModel.apiKey,
        selectedModel.apiUrl,
        selectedModel.apiType,
        config.jevApiKey,
      ).pipe(Effect.provideService(Browser, browser));
      current.update({ finishedAt: current.now() });
      yield* browser.capture(true).pipe(Effect.catch(() => Effect.void));
    });

    const work = Effect.gen(function* () {
      for (const ordinal of [1, 2]) {
        const result = yield* attempt(ordinal).pipe(
          Effect.provideService(Trace, current),
          Effect.scoped,
          Effect.result,
        );

        if (result._tag === "Success") return;
        // Only host preparation can retry, on a fresh browser after confirmed closure.
        // The ready boundary is set before every model, decision or task action.
        if (
          ordinal === 2 ||
          current.snapshot().readyAt !== null ||
          result.failure.code !== "browser"
        )
          return yield* result.failure;
      }
    }).pipe(
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: "4 minutes",
        orElse: () =>
          Effect.fail(
            new LabError({ code: "browser", message: "The four-minute run deadline was reached." }),
          ),
      }),
      Effect.onExit(() => {
        if (current.snapshot().finishedAt === null) current.update({ finishedAt: current.now() });

        return current.measure("cleanup", "Close browser", closeReference()).pipe(
          Effect.match({
            onSuccess: () => current.update({ cleanup: "closed" }),
            onFailure: () => {
              current.update({
                cleanup: "failed",
                status: "failed",
                message: "Browser cleanup failed. Close the browser to retry cleanup.",
              });
            },
          }),
        );
      }),
    );

    const child = yield* Effect.forkChild(work);

    running = child;

    const result = yield* Fiber.await(child).pipe(
      Effect.ensuring(
        Fiber.interrupt(child).pipe(
          Effect.andThen(
            Effect.sync(() => {
              running = undefined;
            }),
          ),
        ),
      ),
    );

    running = undefined;
    if (Exit.isFailure(result)) {
      const error = Cause.findErrorOption(result.cause);

      current.update({
        status: Cause.hasInterrupts(result.cause) ? "cancelled" : "failed",
        message: Option.isSome(error)
          ? error.value.message
          : Cause.hasInterrupts(result.cause)
            ? "Stopped. No actions will be replayed."
            : "The run failed unexpectedly. Inspect its trace.",
      });
    }
    current.update({ elapsed: current.now() });
    const report = current.snapshot();

    patch({ active: null, report }, "settled");

    return report;
  });

  return { snapshot, run, stop, close };
};
