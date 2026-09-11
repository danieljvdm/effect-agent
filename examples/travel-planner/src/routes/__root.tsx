import { RegistryProvider, useAtomMount } from "@effect/atom-react";
import { createRootRoute, HeadContent, Scripts } from "@tanstack/react-router";

import { sessionObservation } from "../auth/client";

import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      {
        name: "viewport",
        content:
          "width=device-width, initial-scale=1, viewport-fit=cover, interactive-widget=resizes-content",
      },
      { name: "description", content: "A little help planning your next great trip." },
      { title: "Elsewhere · Your travel planner" },
    ],
    links: [
      { rel: "stylesheet", href: appCss },
      { rel: "icon", type: "image/svg+xml", href: "/favicon.svg" },
    ],
  }),
  shellComponent: Document,
  notFoundComponent: () => (
    <main className="empty">
      <h1>This place is off the map.</h1>
      <a href="/">Back to your trips</a>
    </main>
  ),
});

function Document({ children }: { readonly children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <meta name="referrer" content="no-referrer" />
        <HeadContent />
      </head>
      <body>
        <RegistryProvider>
          <SessionObservation />
          {children}
        </RegistryProvider>
        <Scripts />
      </body>
    </html>
  );
}

function SessionObservation() {
  useAtomMount(sessionObservation);

  return null;
}
