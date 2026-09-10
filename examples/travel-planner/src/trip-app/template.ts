/** Editable consumer source. Importing this module performs no I/O. */
export const TRIP_APP_TEMPLATE_FILES: Readonly<Record<string, string>> = {
  "package.json": `{
  "name": "elsewhere-trip",
  "private": true,
  "workspaces": [
    "packages/*"
  ],
  "type": "module",
  "scripts": {
    "dev": "vp dev",
    "build": "vp build && vp build --config vite.server.config.ts",
    "build:web": "vp build",
    "build:server": "vp build --config vite.server.config.ts",
    "check": "vp check"
  },
  "devDependencies": {
    "@cloudflare/workers-types": "5.20260825.1",
    "@types/react": "19.2.17",
    "@types/react-dom": "19.2.3",
    "@vitejs/plugin-react": "6.0.4",
    "typescript": "7.0.2",
    "vite": "npm:@voidzero-dev/vite-plus-core@0.3.0",
    "vite-plus": "0.3.0"
  },
  "packageManager": "bun@1.4.0"
}
`,
  "tsconfig.json": `{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noEmit": true,
    "skipLibCheck": true,
    "allowImportingTsExtensions": true,
    "lib": ["ES2022", "DOM", "DOM.Iterable"]
  },
  "include": ["packages", "vite.config.ts", "vite.server.config.ts"]
}
`,
  ".gitignore": `node_modules/
dist/
.vite/
`,
  "vite.config.ts": `import { defineConfig } from "vite-plus";
import react from "@vitejs/plugin-react";
export default defineConfig({
  root: "packages/web",
  base: "./",
  plugins: [react()],
  build: { outDir: "../../dist/web", emptyOutDir: true },
  lint: {
    ignorePatterns: ["**/node_modules/**", "dist/**"],
    options: { typeAware: true, typeCheck: true },
  },
  fmt: { ignorePatterns: ["**/node_modules/**", "dist/**"] },
});
`,
  "vite.server.config.ts": `import { defineConfig } from "vite-plus";
export default defineConfig({
  ssr: { noExternal: true, external: ["cloudflare:workers", "cloudflare:workflows"] },
  build: {
    ssr: "packages/server/src/index.ts",
    outDir: "dist/server",
    emptyOutDir: true,
    rolldownOptions: { external: ["cloudflare:workers", "cloudflare:workflows", "node:async_hooks"], output: { entryFileNames: "index.js" } },
  },
});
`,
  "packages/contracts/package.json": `{
  "name": "@trip/contracts",
  "private": true,
  "type": "module",
  "exports": {
    ".": "./src/index.ts"
  },
  "dependencies": {
    "effect": "4.0.0-rc.112"
  }
}
`,
  "packages/server/package.json": `{
  "name": "@trip/server",
  "private": true,
  "type": "module",
  "dependencies": {
    "@trip/contracts": "workspace:*",
    "effect": "4.0.0-rc.112",
    "effect-cf": "0.40.0"
  }
}
`,
  "packages/web/package.json": `{
  "name": "@trip/web",
  "private": true,
  "type": "module",
  "dependencies": {
    "@effect/atom-react": "4.0.0-rc.112",
    "@trip/contracts": "workspace:*",
    "@types/leaflet": "1.9.21",
    "effect": "4.0.0-rc.112",
    "leaflet": "1.9.4",
    "react": "19.2.8",
    "react-dom": "19.2.8"
  }
}
`,
  "packages/contracts/src/index.ts": `import { Schema } from "effect";
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/unstable/httpapi";

const Label = Schema.String.check(Schema.isMaxLength(240));
const Text = Schema.String.check(Schema.isMaxLength(4000));
const DateLabel = Schema.NullOr(Schema.String.check(Schema.isPattern(/^\\d{4}-\\d{2}-\\d{2}$/)));
const Link = Schema.NullOr(Schema.String.check(Schema.isPattern(/^https:\\/\\/[^\\s]+$/)));
export const Place = Schema.Struct({
  id: Label,
  label: Label,
  latitude: Schema.Number.check(Schema.isBetween({ minimum: -90, maximum: 90 })),
  longitude: Schema.Number.check(Schema.isBetween({ minimum: -180, maximum: 180 })),
  kind: Schema.Literals(["stay", "restaurant", "activity", "sight", "transport"]),
  url: Link,
});
export type Place = typeof Place.Type;
export const Trip = Schema.Struct({
  title: Label,
  destination: Label,
  summary: Text,
  startDate: DateLabel,
  endDate: DateLabel,
  travelers: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30 })),
  days: Schema.Array(
    Schema.Struct({
      title: Label,
      activities: Schema.Array(Label),
      date: Schema.optionalKey(DateLabel),
    }),
  ),
  stays: Schema.Array(Schema.Struct({ id: Label, name: Label, location: Label, url: Link })),
  places: Schema.Array(Place),
});
export type Trip = typeof Trip.Type;
export class TripUnavailable extends Schema.TaggedError<TripUnavailable>()("TripUnavailable", {
  message: Schema.String,
}) {}
export const TripApi = HttpApi.make("trip-app").add(
  HttpApiGroup.make("trip").add(
    HttpApiEndpoint.get("get", "/api/trip", {
      success: Trip,
      error: TripUnavailable.pipe(HttpApiSchema.status(503)),
    }),
  ),
);
`,
  "packages/server/src/index.ts": `import { Effect, Layer, Schema } from "effect";
import { ServiceBinding, Worker } from "effect-cf";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { HttpApiBuilder } from "effect/unstable/httpapi";
import { Trip, TripApi, TripUnavailable } from "@trip/contracts";

class TripData extends ServiceBinding.Service<TripData>()("TripData", { binding: "TRIP_DATA" }) {}
const unavailable = () =>
  new TripUnavailable({ message: "This trip is temporarily unavailable. Please try again." });
export const readTrip = Effect.gen(function* () {
  const service = yield* TripData;
  // The host fixes this binding to one authorized trip; no caller IDs or cookies are forwarded.
  const response = yield* service.fetch("https://trip-data/api/trip").pipe(Effect.mapError(unavailable));
  if (!response.ok) return yield* unavailable();
  const json = yield* Effect.tryPromise({ try: () => response.json(), catch: unavailable });
  return yield* Schema.decodeUnknownEffect(Trip)(json).pipe(Effect.mapError(unavailable));
});
const handlers = HttpApiBuilder.group(TripApi, "trip", Effect.fn(function* (group) {
  const service = yield* TripData;
  return group.handle("get", () => readTrip.pipe(Effect.provideService(TripData, service)));
})).pipe(Layer.provide(TripData.layer));
const routes = HttpApiBuilder.layer(TripApi).pipe(
  Layer.provide(handlers),
  Layer.provide(HttpServer.layerServices),
);
const application = routes.pipe(Layer.provideMerge(HttpRouter.layer));

// The trusted host serves dist/web and routes /api/* to this Effect Worker.
export default Worker.make(application, {
  fetch: Effect.gen(function* () {
    const request = yield* Worker.NativeRequest;
    const url = new URL(request.url);
    if (url.pathname !== "/api/trip" || url.search !== "") return new Response("Not found", { status: 404 });
    const router = yield* HttpRouter.HttpRouter;
    return yield* router.asHttpEffect();
  }),
});
`,
  "packages/web/index.html": `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <meta name="referrer" content="no-referrer" />
    <title>Your trip · Elsewhere</title>
    <link rel="icon" type="image/svg+xml" href="/favicon.svg" />
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
`,
  "packages/web/src/state.ts": `import { FetchHttpClient } from "effect/unstable/http";
import { AtomHttpApi } from "effect/unstable/reactivity";
import { TripApi } from "@trip/contracts";
export class TripClient extends AtomHttpApi.Service<TripClient>()("TripClient", {
  api: TripApi,
  httpClient: FetchHttpClient.layer,
}) {}
export const tripAtom = TripClient.query("trip", "get", {});
`,
  "packages/web/public/favicon.svg": `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" rx="8" fill="#e5eada"/><path d="M10 22 23 9M11 9h12v12" fill="none" stroke="#30483e" stroke-width="3"/></svg>`,
  "packages/web/src/vite-env.d.ts": `/// <reference types="vite/client" />
`,
  "packages/web/src/main.tsx": `import { createRoot } from "react-dom/client";
import { RegistryProvider } from "@effect/atom-react";
import { App } from "./App.tsx";
import "./styles.css";
const root = document.getElementById("root");
if (root)
  createRoot(root).render(
    <RegistryProvider>
      <App />
    </RegistryProvider>,
  );
`,
  "packages/web/src/TripMap.tsx": `import type { Place } from "@trip/contracts";
/** Replace this module to add a map. Coordinates must come from the trip's trusted data. */
export function TripMap(_props: { readonly places: readonly Place[] }) {
  return null;
}
`,
  "packages/web/src/App.tsx": `import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import { tripAtom } from "./state.ts";
import { TripMap } from "./TripMap.tsx";

export function App() {
  const result = useAtomValue(tripAtom);
  if (!AsyncResult.isSuccess(result))
    return (
      <main className="shell">
        <a className="brand" href="./">
          elsewhere ↗︎
        </a>
        <section className="card" role="status">
          <h1>
            {AsyncResult.isFailure(result) ? "Your trip couldn't load" : "Getting your trip ready…"}
          </h1>
          {AsyncResult.isFailure(result) && <a href="./">Try again</a>}
        </section>
      </main>
    );
  const trip = result.value;
  return (
    <main className="shell">
      <header>
        <a className="brand" href="./">
          elsewhere ↗︎
        </a>
        <span>A LITTLE PLAN FOR A GREAT TRIP</span>
      </header>
      <section className="hero">
        <p className="eyebrow">YOUR NEXT CHAPTER · {trip.destination}</p>
        <h1>{trip.title}</h1>
        <p className="summary">{trip.summary}</p>
        <div className="facts">
          <span>
            {trip.startDate ?? "Dates flexible"}
            {trip.endDate ? " → " + trip.endDate : ""}
          </span>
          <span>
            {trip.travelers} {trip.travelers === 1 ? "traveler" : "travelers"}
          </span>
        </div>
      </section>
      <TripMap places={trip.places} />
      <section aria-labelledby="days-title">
        <div className="section-heading">
          <h2 id="days-title">Room to explore</h2>
          <span>YOUR ITINERARY</span>
        </div>
        <div className="days">
          {trip.days.map((day, index) => (
            <article className="card" key={index}>
              <p className="eyebrow">
                DAY {index + 1}
                {day.date ? " · " + day.date : ""}
              </p>
              <h3>{day.title}</h3>
              <ol>
                {day.activities.map((activity, i) => (
                  <li key={i}>{activity}</li>
                ))}
              </ol>
            </article>
          ))}
        </div>
        {trip.days.length === 0 && <p>Your day-by-day ideas will appear here as you plan.</p>}
      </section>
      {trip.stays.length > 0 && (
        <section aria-labelledby="stays-title">
          <div className="section-heading">
            <h2 id="stays-title">Somewhere to settle in</h2>
            <span>STAY IDEAS</span>
          </div>
          <div className="days">
            {trip.stays.map((stay) => (
              <article className="card" key={stay.id}>
                <h3>{stay.name}</h3>
                <p>{stay.location}</p>
                {stay.url && (
                  <a className="source" href={stay.url} target="_blank" rel="noopener noreferrer">
                    View stay ↗
                  </a>
                )}
              </article>
            ))}
          </div>
        </section>
      )}
      <footer>
        A plan, with room for the unexpected. Confirm prices, availability, and opening times before
        booking.
      </footer>
    </main>
  );
}
`,
  "packages/web/src/styles.css": `:root {
  font-family: ui-sans-serif, system-ui, sans-serif;
  color: #2d4439;
  background: #fafbf6;
  font-synthesis: none;
}
* {
  box-sizing: border-box;
}
body {
  margin: 0;
}
a {
  color: inherit;
}
button,
input,
textarea {
  font: inherit;
  font-size: 16px;
}
.shell {
  max-width: 1080px;
  margin: auto;
  padding: 28px 28px 40px;
}
header,
.section-heading {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 20px;
}
header > span,
.section-heading > span,
.eyebrow {
  font-size: 11px;
  letter-spacing: 1.7px;
  color: #6b7d64;
}
.brand {
  font-size: 26px;
  font-weight: 750;
  letter-spacing: -1px;
  text-decoration: none;
}
.hero {
  padding: 64px 0 40px;
  max-width: 850px;
}
h1 {
  font-size: clamp(36px, 6vw, 66px);
  line-height: 1.06;
  letter-spacing: -2px;
  margin: 18px 0;
  font-weight: 600;
}
h2 {
  font-size: 25px;
  font-weight: 550;
}
h3 {
  font-size: 21px;
  font-weight: 550;
  margin: 12px 0;
}
.summary {
  max-width: 660px;
  font-size: 18px;
  line-height: 1.7;
  color: #65745e;
}
.facts {
  display: flex;
  gap: 10px;
  flex-wrap: wrap;
  margin-top: 24px;
}
.facts span {
  border: 1px solid #dce3d2;
  border-radius: 40px;
  padding: 9px 15px;
  font-size: 13px;
  background: #f0f3e9;
}
.days {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 270px), 1fr));
  gap: 16px;
}
.card {
  background: #fff;
  border: 1px solid #e0e6d8;
  border-radius: 16px;
  padding: 24px;
}
section {
  margin-bottom: 30px;
}
ol {
  padding-left: 21px;
}
li {
  padding: 8px 0;
  line-height: 1.6;
  color: #66735f;
}
.source {
  display: inline-block;
  margin-top: 16px;
  text-underline-offset: 4px;
}
footer {
  padding: 30px 0;
  color: #76826d;
  font-size: 12px;
  line-height: 1.7;
}
.map-places {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  list-style: none;
  padding: 0;
}
.map-places li {
  padding: 0;
  font-size: 13px;
}
:focus-visible {
  outline: 2px solid #527443;
  outline-offset: 4px;
}
@media (max-width: 600px) {
  .shell {
    padding: 20px 18px;
  }
  .hero {
    padding-top: 35px;
  }
  header > span,
  .section-heading > span {
    display: none;
  }
  .card {
    padding: 20px;
  }
  .summary {
    font-size: 16px;
  }
  .days {
    grid-template-columns: 1fr;
  }
}
`,
  "README.md": `# Your editable trip app

This is real source: change packages/web/src/App.tsx for layouts, styles.css for the visual design,
packages/server/src/index.ts for server behavior, and packages/contracts/src/index.ts for shared schemas/API.
Run vp install, vp check, and vp run build. Bun 1.4.0 and Vite+ 0.3.0 are pinned.
The web output is dist/web; the bundled Worker entry is dist/server/index.js.
vp dev previews the web UI; it requires the host's same-origin /api/trip route to load trip data.

GET /api/trip returns the shared Trip schema. Its server reads only the host-provided TRIP_DATA.fetch
binding at https://trip-data/api/trip. The trusted host owns authentication, binds one trip, serves
assets, and handles deployment. This app never accepts an owner/trip selector or receives credentials.
Keep deployment configuration outside this editable repo. All trip responses are private/no-store.

The host supplies stays and places as arrays, including empty arrays when there is no verified data.
Places have id, label, latitude, longitude, kind, and nullable HTTPS url. Never infer or invent
coordinates from a place name. Dates may be null. Itinerary day dates are optional.

The initial TripMap.tsx intentionally renders nothing. Applying the map upgrade replaces actual
source with a Leaflet 1.9.4 map (https://leafletjs.com/download.html). It plots only supplied coordinates,
has a textual linked place list, and connects places in array order. Connections are itinerary order,
not verified driving routes or directions. There is no frontend geocoding. Map data comes from
OpenStreetMap tiles with visible attribution. Respect https://operations.osmfoundation.org/policies/tiles/;
use another tile provider if your traffic requires it. Leaflet is pinned in the web package up front.
`,
};

/** Ordinary source edits enabling a map with scoped, supplied coordinates. */
export const TRIP_APP_MAP_FILES: Readonly<Record<string, string>> = {
  "packages/web/src/TripMap.tsx": `import { useEffect, useRef } from "react";
import * as L from "leaflet";
import type { Place } from "@trip/contracts";
import "leaflet/dist/leaflet.css";
import "./map.css";

export function TripMap({ places }: { readonly places: readonly Place[] }) {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!container.current || places.length === 0) return;
    const map = L.map(container.current, { scrollWheelZoom: false });
    L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>',
      referrerPolicy: "strict-origin-when-cross-origin",
    }).addTo(map);
    const coordinates: L.LatLngTuple[] = places.map((place) => [place.latitude, place.longitude]);
    places.forEach((place, index) => {
      // Leaflet string popups are HTML: use textContent for every planner-supplied label.
      const label = document.createElement("span");
      label.textContent = index + 1 + ". " + place.label;
      L.circleMarker(coordinates[index]!, {
        radius: 9,
        color: "#3e603a",
        weight: 3,
        fillColor: "#f8faf2",
        fillOpacity: 1,
      })
        .addTo(map)
        .bindPopup(label);
    });
    if (coordinates.length > 1)
      L.polyline(coordinates, { color: "#607c4c", weight: 3, dashArray: "6 7" }).addTo(map);
    map.fitBounds(L.latLngBounds(coordinates), { padding: [32, 32], maxZoom: 13 });
    const observer = new ResizeObserver(() => map.invalidateSize());
    observer.observe(container.current);
    return () => {
      observer.disconnect();
      map.remove();
    };
  }, [places]);
  return (
    <section className="card trip-map" aria-labelledby="map-title">
      <h2 id="map-title">Your journey</h2>
      {places.length === 0 ? (
        <p>Add places in the planner to see them on the map</p>
      ) : (
        <>
          <p>Dashed lines connect your itinerary in order, not verified driving directions.</p>
          <div
            ref={container}
            className="journey-map"
            role="region"
            aria-label="Interactive map of your trip places"
          />
          <ol className="map-places">
            {places.map((place, index) => (
              <li key={place.id}>
                {index + 1}.{" "}
                {place.url ? (
                  <a href={place.url} target="_blank" rel="noopener noreferrer">
                    {place.label}
                  </a>
                ) : (
                  place.label
                )}{" "}
                · {place.kind}
              </li>
            ))}
          </ol>
        </>
      )}
    </section>
  );
}
`,
  "packages/web/src/map.css": `.journey-map {
  height: 420px;
  max-height: 65dvh;
  min-height: 260px;
  border-radius: 12px;
  isolation: isolate;
}
.journey-map .leaflet-control-attribution {
  font-size: 11px;
  background: #fffffff0;
}
.journey-map .leaflet-control-attribution a {
  color: #245a35;
}
.map-places {
  display: flex;
  flex-wrap: wrap;
  gap: 12px;
  list-style: none;
  padding: 0;
}
.map-places li {
  font-size: 13px;
}
@media (max-width: 600px) {
  .journey-map {
    height: 330px;
  }
}
`,
};
