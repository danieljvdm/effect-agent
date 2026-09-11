import { Effect } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { AsyncResult, Atom, AtomHttpApi, Reactivity } from "effect/unstable/reactivity";

import { runtime } from "./auth/client";
import { FundingApi, type GrantFunding, type RevokeFunding } from "./funding-domain";
import { sessionAtom } from "./state";

export class FundingClient extends AtomHttpApi.Service<FundingClient>()(
  "travel-planner/FundingClient",
  {
    api: FundingApi,
    runtime,
    baseUrl: "",
    httpClient: FetchHttpClient.layer,
  },
) {}

// Explicit account headers fence cookie changes across tabs, just like the planner RPC client.
export const fundingPageAtom = Atom.make<string | undefined>(undefined);

export const fundingStatusAtom = Atom.make((get) => {
  const session = get(sessionAtom);

  return AsyncResult.isSuccess(session)
    ? get(statusFor(session.value.subjectId))
    : AsyncResult.initial();
});

const statusFor = Atom.family((subjectId: string) =>
  FundingClient.query("funding", "status", {
    headers: { "x-elsewhere-account": subjectId },
    reactivityKeys: ["funding"],
  }),
);

export const fundingDirectoryAtom = Atom.make((get) => {
  const session = get(sessionAtom);
  const status = get(fundingStatusAtom);
  const after = get(fundingPageAtom);

  return AsyncResult.isSuccess(session) && AsyncResult.isSuccess(status) && status.value.admin
    ? get(
        FundingClient.query("funding", "list", {
          query: after === undefined ? {} : { after },
          headers: { "x-elsewhere-account": session.value.subjectId },
          reactivityKeys: ["funding"],
        }),
      )
    : AsyncResult.initial();
});

export const changeFundingAtom = FundingClient.runtime.fn<
  | { action: "grant"; input: typeof GrantFunding.Type }
  | { action: "revoke"; input: typeof RevokeFunding.Type }
>()(
  Effect.fnUntraced(function* (request, get) {
    const session = get(sessionAtom);

    if (!AsyncResult.isSuccess(session) || session.waiting) return yield* Effect.interrupt;
    const client = yield* FundingClient;
    const headers = { "x-elsewhere-account": session.value.subjectId };

    return yield* Reactivity.mutation(
      request.action === "grant"
        ? client.funding.grant({ payload: request.input, headers }).pipe(Effect.asVoid)
        : client.funding.revoke({ payload: request.input, headers }),
      ["funding", `openai-connection:${session.value.subjectId}`],
    );
  }),
);

export const refreshFundingAtom = FundingClient.runtime.fn<void>()(
  Effect.fnUntraced(function* (_, get) {
    get.set(changeFundingAtom, Atom.Reset);
    yield* Reactivity.invalidate(["funding"]);
  }),
);
