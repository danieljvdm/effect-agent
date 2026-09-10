import { Effect, Layer, Schema } from "effect";

import { AccessError, adminEmail, Email } from "../../src/access-domain.ts";
import { PlannerError, TripSiteStore } from "../../src/domain.ts";
import type { AccessEnvironment } from "../../src/server/access-auth.ts";
import { makeTravelPlannerThread, plannerApplication } from "../../src/server/cloudflare.ts";
import { TripFailpoint } from "../../src/server/trips.ts";
import { makeWorker } from "../../src/worker.ts";
import { FixtureBrowserLive } from "./browser.ts";
import { FixtureModel, advanceFixtureProgress, fixtureProgressStatus } from "./models.ts";

interface TestEnvironment extends AccessEnvironment {
  readonly PLANNER_TOKEN?: string;
}

const fixtureAuthorized = (request: Request, env: TestEnvironment) =>
  Boolean(env.PLANNER_TOKEN) &&
  request.headers.get("authorization") === `Bearer ${env.PLANNER_TOKEN}`;

// Only this bundled test entrypoint accepts a bearer fixture and caller-selected identity.
const worker = makeWorker(
  Effect.fn("Fixture.authenticate")(function* (request: Request, env: TestEnvironment) {
    if (!fixtureAuthorized(request, env))
      return yield* new AccessError({
        code: "unauthorized",
        message: "Missing fixture authentication.",
      });

    const email = yield* Schema.decodeUnknownEffect(Email)(
      request.headers.get("x-test-email") ?? adminEmail,
    ).pipe(
      Effect.mapError(
        () => new AccessError({ code: "unauthorized", message: "Invalid fixture identity." }),
      ),
    );

    return { email, isAdmin: email === adminEmail };
  }),
);

let failAt = "";
let failureMode = "failure";

const sites = Layer.succeed(TripSiteStore, {
  publish: ({ trip }) =>
    Effect.succeed({
      tripId: trip.id,
      revision: trip.revision,
      path: `/trips/${trip.id}/${trip.revision}`,
      commitId: `fixture-commit-${trip.revision}`,
      publishedAt: "2026-09-09T00:00:00Z",
    }),
  load: () => Effect.succeed(null),
}).pipe(
  Layer.provideMerge(
    Layer.succeed(TripFailpoint, {
      hit: (point) =>
        Effect.suspend(() => {
          if (point !== failAt) return Effect.void;
          failAt = "";
          if (failureMode === "defect") return Effect.die("fixture defect");
          if (failureMode === "interruption") return Effect.interrupt;

          return Effect.fail(
            new PlannerError({ code: "storage", message: "Injected storage failure." }),
          );
        }),
    }),
  ),
);

export class TravelPlannerThread extends makeTravelPlannerThread(
  sites,
  plannerApplication(FixtureModel, "fixture-script-v1", "Test model", FixtureBrowserLive),
) {}

export default {
  fetch(request: Request, env: Cloudflare.Env & TestEnvironment): Response | Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/__test/progress") {
      if (!fixtureAuthorized(request, env)) return new Response("Unauthorized", { status: 401 });
      if (request.method === "POST") advanceFixtureProgress();

      return Response.json(fixtureProgressStatus());
    }

    if (url.pathname === "/__test/failpoint") {
      if (!fixtureAuthorized(request, env)) return new Response("Unauthorized", { status: 401 });
      failAt = url.searchParams.get("point") ?? "";
      failureMode = url.searchParams.get("mode") ?? "failure";

      return new Response("Armed");
    }

    return worker.fetch(request, env);
  },
};
