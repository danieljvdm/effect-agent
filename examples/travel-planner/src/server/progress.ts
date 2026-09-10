import { Clock, Context, Effect, Exit, Layer, Option, Ref } from "effect";

import { type PlannerProgress, type PlannerSettings, type PlannerError } from "../domain.ts";

export const emptyProgress: PlannerProgress = {
  submissionId: null,
  attemptId: null,
  revision: 0,
  text: "",
  tools: [],
};

export interface ProgressWriter {
  readonly text: (delta: string) => Effect.Effect<void>;
  readonly newResponse: Effect.Effect<void>;
  readonly tool: (
    id: string,
    label: string,
    state: "running" | "complete" | "failed",
  ) => Effect.Effect<void>;
  readonly finish: Effect.Effect<void>;
}

/** An attempt owns its admitted settings and writer; later joined inputs cannot replace either. */
export class PlannerAttempt extends Context.Service<
  PlannerAttempt,
  {
    readonly settings: Effect.Effect<PlannerSettings, PlannerError>;
    readonly progress: ProgressWriter;
  }
>()("travel-planner/PlannerAttempt") {}

/** Disposable, bounded UI projection. Canonical records remain the authority after reconnect. */
export class ProgressStore extends Context.Service<
  ProgressStore,
  {
    readonly read: Effect.Effect<PlannerProgress>;
    readonly begin: (submissionId: string, attemptId: string) => Effect.Effect<ProgressWriter>;
  }
>()("travel-planner/ProgressStore") {
  static readonly layer = Layer.effect(
    ProgressStore,
    Effect.gen(function* () {
      const state = yield* Ref.make(emptyProgress);

      const begin = Effect.fn("ProgressStore.begin")(function* (
        submissionId: string,
        attemptId: string,
      ) {
        const startedAt = yield* Clock.currentTimeMillis;

        yield* Ref.update(state, (previous) => ({
          ...emptyProgress,
          submissionId,
          attemptId,
          startedAt,
          revision: previous.revision + 1,
        }));

        const update = (f: (previous: PlannerProgress) => PlannerProgress) =>
          Ref.update(state, (previous) =>
            previous.attemptId === attemptId && previous.submissionId === submissionId
              ? { ...f(previous), revision: previous.revision + 1 }
              : previous,
          );

        return {
          text: (delta) =>
            update((previous) => ({ ...previous, text: (previous.text + delta).slice(0, 4000) })),
          newResponse: update((previous) => ({ ...previous, text: "" })),
          tool: (id, label, status) =>
            Effect.flatMap(Clock.currentTimeMillis, (now) =>
              update((previous) => {
                const boundedId = id.slice(0, 256);
                const existing = previous.tools.find((tool) => tool.id === boundedId);

                const tool =
                  existing?.completedAt !== undefined
                    ? existing
                    : {
                        id: boundedId,
                        label: label.slice(0, 240),
                        state: status,
                        startedAt: existing?.startedAt ?? now,
                        ...(status === "running" ? {} : { completedAt: now }),
                      };

                return {
                  ...previous,
                  tools: [...previous.tools.filter((tool) => tool.id !== boundedId), tool].slice(
                    -24,
                  ),
                };
              }),
            ),
          finish: Effect.flatMap(Clock.currentTimeMillis, (now) =>
            update((previous) => ({
              ...previous,
              completedAt: previous.completedAt ?? now,
              tools: previous.tools.map((tool) =>
                tool.state === "running" ? { ...tool, state: "failed", completedAt: now } : tool,
              ),
            })),
          ),
        } satisfies ProgressWriter;
      });

      return { read: Ref.get(state), begin };
    }),
  );
}

export const withToolProgress = <A, E, R>(
  writer: ProgressWriter,
  id: string,
  label: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  writer.tool(id, label, "running").pipe(
    Effect.andThen(effect),
    Effect.onExit((exit) => writer.tool(id, label, Exit.isSuccess(exit) ? "complete" : "failed")),
  );

export const pageProgressLabel = (url: string): string => {
  if (!URL.canParse(url)) return "Reading travel details";
  const host = new URL(url).hostname;

  if (host === "airbnb.com" || host.endsWith(".airbnb.com")) return "Reading an Airbnb listing";
  if (host === "vrbo.com" || host.endsWith(".vrbo.com")) return "Reading a Vrbo listing";
  if (host === "booking.com" || host.endsWith(".booking.com"))
    return "Reading a Booking.com listing";

  return "Reading travel details";
};

/** Direct tool consumers can omit the application's optional live observer. */
export const trackTool = <A, E, R>(
  id: string,
  label: string,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.serviceOption(PlannerAttempt).pipe(
    Effect.flatMap((attempt) =>
      Option.isSome(attempt) ? withToolProgress(attempt.value.progress, id, label, effect) : effect,
    ),
  );
