import {
  runWorkOrder,
  type ImplementationWorkspace,
  type WorkOrderMission,
  type WorkOrderReport,
} from "@effect-agent/example-pr-work-orders";
import { Context, Effect } from "effect";

import { authenticateDelivery } from "./authenticate.ts";
import { constructWorkOrder } from "./construct.ts";
import { IngressPolicy, type IngressError, type PlatformDelivery } from "./contracts.ts";
import { parseDispatchTarget } from "./parse-event.ts";
import { presentFailure, presentSuccess } from "./presentation.ts";
import { DurableAttemptStore, replaySnapshot } from "./store.ts";

export class WorkOrderImplementer extends Context.Service<
  WorkOrderImplementer,
  {
    readonly run: (
      mission: WorkOrderMission,
      workspace: ImplementationWorkspace,
    ) => Effect.Effect<WorkOrderReport, unknown>;
  }
>()("@effect-agent/example-pr-work-order-ingress/WorkOrderImplementer") {}

const describeError = (
  error: IngressError,
): { readonly errorTag: string; readonly detail: string } => {
  if ("reason" in error && typeof error.reason === "string") {
    return { errorTag: error._tag, detail: error.reason.slice(0, 2_048) };
  }
  if ("detail" in error && typeof error.detail === "string") {
    return { errorTag: error._tag, detail: error.detail.slice(0, 2_048) };
  }
  return { errorTag: error._tag, detail: error._tag };
};

export const handleWorkOrderDelivery = Effect.fn("handleWorkOrderDelivery")(function* (
  delivery: PlatformDelivery,
) {
  const policy = yield* IngressPolicy;
  yield* authenticateDelivery(delivery, policy);
  const target = yield* parseDispatchTarget(delivery, policy);
  const order = yield* constructWorkOrder(target, policy, delivery.deliveryId);
  const store = yield* DurableAttemptStore;
  const claim = yield* store.claim(order);
  if (claim._tag !== "claimed") {
    return yield* replaySnapshot(claim.snapshot);
  }
  const implementer = yield* WorkOrderImplementer;
  return yield* runWorkOrder({
    order,
    implement: implementer.run,
  }).pipe(
    Effect.matchEffect({
      onSuccess: (result) =>
        store
          .complete(order, { _tag: "result", result })
          .pipe(Effect.andThen(presentSuccess(order, result)), Effect.as(result)),
      onFailure: (error) =>
        store
          .complete(order, { _tag: "failure", ...describeError(error) })
          .pipe(Effect.andThen(presentFailure(order, error)), Effect.andThen(Effect.fail(error))),
    }),
  );
});
