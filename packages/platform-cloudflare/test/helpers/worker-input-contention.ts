import { Effect } from "effect";

/** Owner-local controls for the single retained-input claim race. */
export const workerInputContentions = new Map<
  string,
  {
    callerFiber: number | undefined;
    inserted: boolean;
    paused: boolean;
    admitted: boolean;
    acquired: number;
    released: number;
    readonly callerRelease: Promise<void>;
    readonly admissionRelease: Promise<void>;
    readonly releaseCaller: () => void;
    readonly releaseAdmission: () => void;
  }
>();

export const armWorkerInputContention = (thread: string): void => {
  let releaseCaller!: () => void;
  let releaseAdmission!: () => void;

  const callerRelease = new Promise<void>((resolve) => {
    releaseCaller = resolve;
  });

  const admissionRelease = new Promise<void>((resolve) => {
    releaseAdmission = resolve;
  });

  workerInputContentions.set(thread, {
    callerFiber: undefined,
    inserted: false,
    paused: false,
    admitted: false,
    acquired: 0,
    released: 0,
    callerRelease,
    admissionRelease,
    releaseCaller,
    releaseAdmission,
  });
};

export const observeWorkerInputDelivery = Effect.fnUntraced(function* (
  thread: string,
  point: string,
) {
  const control = workerInputContentions.get(thread);

  if (control === undefined) return;
  if (point === "message-delivery:insert:after" && control.callerFiber === (yield* Effect.fiberId))
    control.inserted = true;
  if (point === "message-delivery:admission:after" && !control.admitted) {
    yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        control.admitted = true;
        control.acquired++;
      }),
      () => Effect.promise(() => control.admissionRelease),
      () =>
        Effect.sync(() => {
          control.released++;
        }),
    );
  }
});

/** The guarded insertion has released its store permit before this hook. */
export const pauseWorkerInputInsertion = Effect.fnUntraced(function* (
  thread: string,
  location: string,
) {
  const control = workerInputContentions.get(thread);

  if (
    location !== "maintenance:mutation:finished" ||
    control === undefined ||
    !control.inserted ||
    control.paused ||
    control.callerFiber !== (yield* Effect.fiberId)
  )
    return;
  control.paused = true;
  yield* Effect.promise(() => control.callerRelease);
});
