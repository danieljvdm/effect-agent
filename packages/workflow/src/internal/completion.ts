import { DurableDeferred } from "effect/workflow";

import { WorkflowSettlementReference } from "../WorkflowDispatch.ts";

/** Native deferred results contain only canonical identities, never a second agent result. */
export const workflowCompletion = (name: string) =>
  DurableDeferred.make(name, { success: WorkflowSettlementReference });
