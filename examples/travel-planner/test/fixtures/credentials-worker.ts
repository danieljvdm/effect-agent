import { Effect, Layer, Redacted, Schema } from "effect";
import { DurableObject } from "effect-cf";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { PlannerError, TripSiteStore } from "../../src/domain.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import {
  credentialForOwner,
  credentialSourceLayer,
  CredentialFailpoint,
} from "../../src/server/credentials.ts";
import { plannerOwner } from "../../src/server/tenancy.ts";
import { FixtureBrowserLive } from "./browser.ts";
import { ownerEmail, fixtureSubject } from "./identity.ts";
import { FixtureModel } from "./models.ts";
import fixtureWorker from "./worker.ts";

let failurePoint = "";
let failureMode = "failure";

const failures = Layer.succeed(CredentialFailpoint, {
  hit: (point) =>
    Effect.suspend(() => {
      if (failurePoint !== point) return Effect.void;
      failurePoint = "";
      if (failureMode === "defect") return Effect.die("fixture preference defect");
      if (failureMode === "interrupt") return Effect.interrupt;

      return Effect.fail(
        new PlannerError({ code: "storage", message: "Injected preference failure." }),
      );
    }),
});

const sites = Layer.succeed(TripSiteStore, {
  publish: () =>
    Effect.fail(new PlannerError({ code: "publication", message: "Unused fixture publication." })),
  load: () => Effect.succeed(null),
});

/** The only raw-row access is in this isolated test bundle. */
export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(FixtureModel, "fixture-script-v1", "Test model", FixtureBrowserLive).pipe(
    Layer.provideMerge(failures),
  ),
) {
  fetch(request: Request): Promise<Response> {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const sql = yield* SqlClient;

        if (request.method === "PUT") {
          const value = yield* Effect.promise(() => request.text());

          yield* sql`INSERT INTO travel_model_credentials (id, value) VALUES (1, ${value})
          ON CONFLICT(id) DO UPDATE SET value = excluded.value`;
        }
        const rows = yield* sql`SELECT value FROM travel_model_credentials WHERE id = 1`;

        return Response.json(
          yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ value: Schema.String })))(
            rows,
          ),
        );
      }),
    );
  }
}

export default {
  async fetch(request: Request, env: Cloudflare.Env & { readonly PLANNER_TOKEN?: string }) {
    const url = new URL(request.url);

    if (url.pathname === "/__test/credentials") {
      if (
        !env.PLANNER_TOKEN ||
        request.headers.get("authorization") !== `Bearer ${env.PLANNER_TOKEN}`
      )
        return new Response("Unauthorized", { status: 401 });
      if (url.searchParams.has("point")) {
        failurePoint = url.searchParams.get("point") ?? "";
        failureMode = url.searchParams.get("mode") ?? "failure";

        return new Response("Armed");
      }

      const owner = await Effect.runPromise(
        plannerOwner(fixtureSubject(request.headers.get("x-test-email") ?? ownerEmail)),
      );

      if (url.searchParams.has("resolve")) {
        const resolved = await Effect.runPromise(
          credentialForOwner(owner).pipe(
            Effect.provide(credentialSourceLayer(env)),
            Effect.match({
              onFailure: (error) => ({ error: error.message }),
              onSuccess: (key) => ({ lastFour: Redacted.value(key).slice(-4) }),
            }),
          ),
        );

        return Response.json(resolved);
      }
      const response = await env.ACCOUNT_THREADS.getByName(owner).fetch(request);

      return new Response(await response.arrayBuffer(), {
        status: response.status,
        headers: response.headers,
      });
    }

    return fixtureWorker.fetch(request, env);
  },
};
