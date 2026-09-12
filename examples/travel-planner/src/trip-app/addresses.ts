import { Effect, Option, Schema } from "effect";

import { AppSiteName, AppSiteRegistration, PlannerError, type TripApp } from "../domain.ts";
import { TripFailpoint } from "../server/trips.ts";
import { AppBuildBucket } from "./bucket.ts";

const unavailable = () =>
  new PlannerError({ code: "unavailable", message: "The trip app address is unavailable." });

/** This prefix is host-only metadata, never an asset path or generated Worker binding. */
export const appAddressKey = (hostname: string) => `app-addresses/auth-v1/${hostname}.json`;

export const appNameFromHost = (hostname: string, domain: string) => {
  const suffix = `-trip.${domain}`;
  const name = hostname.endsWith(suffix) ? hostname.slice(0, -suffix.length) : "";

  return Schema.is(AppSiteName)(name) && name.length + 5 <= 63 ? name : null;
};

/** The stable suffix separates equally named trips without exposing an owner identity. */
export const tripAppHostname = (title: string, appId: string, domain: string) => {
  const slug =
    title
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-|-$/g, "")
      .slice(0, 45)
      .replace(/-$/, "") || "my-trip";

  return `${slug}-${appId.slice(0, 12)}-trip.${domain}`;
};

export const readTripAppAddress = Effect.fn("readTripAppAddress")(
  function* (hostname: string) {
    const bucket = yield* AppBuildBucket;
    const found = yield* bucket.get(appAddressKey(hostname));

    if (Option.isNone(found)) return null;
    const object = found.value;

    yield* Effect.addFinalizer(() =>
      object.bodyUsed
        ? Effect.void
        : Effect.tryPromise({ try: () => object.body.cancel(), catch: () => undefined }).pipe(
            Effect.ignore,
          ),
    );
    if (object.size > 4096) return yield* unavailable();

    const entry = yield* Schema.decodeEffect(Schema.fromJsonString(AppSiteRegistration))(
      yield* object.text,
    ).pipe(Effect.mapError(unavailable));

    if (entry.hostname !== hostname) return yield* unavailable();

    return entry;
  },
  Effect.scoped,
  Effect.catchTag("R2OperationError", unavailable),
);

/**
 * Register both names before returning or starting a build. Conditional immutable writes
 * and readback make a lost acknowledgement retryable; collisions never change ownership.
 * The gateway checks the owner repository again, so an interrupted creation grants no data.
 */
export const publishTripAppAddress = Effect.fn("publishTripAppAddress")(
  function* (owner: string, app: TripApp, domain: string) {
    const canonical = yield* Effect.try({
      try: () => new URL(app.url),
      catch: unavailable,
    });

    if (
      canonical.protocol !== "https:" ||
      canonical.port !== "" ||
      canonical.username !== "" ||
      canonical.password !== "" ||
      canonical.pathname !== "/" ||
      canonical.search !== "" ||
      canonical.hash !== "" ||
      appNameFromHost(canonical.hostname, domain) === null
    )
      return yield* unavailable();
    const storage = yield* AppBuildBucket;
    const failpoint = yield* TripFailpoint;

    for (const hostname of new Set([canonical.hostname, `${app.id}-trip.${domain}`])) {
      const entry = yield* Schema.decodeEffect(AppSiteRegistration)({
        version: 1,
        owner,
        appId: app.id,
        tripId: app.tripId,
        hostname,
      }).pipe(Effect.mapError(unavailable));

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(AppSiteRegistration))(
        entry,
      ).pipe(Effect.mapError(unavailable));

      const existing = yield* readTripAppAddress(hostname);

      if (existing === null) {
        yield* failpoint.hit("app-address:before-put");
        yield* storage.put(appAddressKey(hostname), encoded, {
          onlyIf: { etagDoesNotMatch: "*" },
          httpMetadata: { contentType: "application/json" },
        });
        yield* failpoint.hit("app-address:after-put");
      }
      const registered = existing ?? (yield* readTripAppAddress(hostname));

      if (
        registered === null ||
        registered.owner !== owner ||
        registered.appId !== app.id ||
        registered.tripId !== app.tripId
      )
        return yield* new PlannerError({
          code: "conflict",
          message: "This trip app address is already reserved. Its owner has not changed.",
        });
    }
  },
  Effect.catchTag("R2OperationError", unavailable),
);
