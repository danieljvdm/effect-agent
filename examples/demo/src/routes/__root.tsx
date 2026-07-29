import { RegistryProvider } from "@effect/atom-react";
import { createRootRoute, HeadContent, Scripts } from "@tanstack/react-router";

import appCss from "../styles.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      {
        name: "description",
        content: "A browser test bench for the Effect Agent Phase 0 runtime.",
      },
      { title: "Effect Agent · Phase 0 bench" },
    ],
    links: [{ rel: "stylesheet", href: appCss }],
  }),
  notFoundComponent: () => (
    <main className="grid min-h-svh place-items-center p-6">
      <p className="font-mono text-sm text-muted-foreground">404 · route not found</p>
    </main>
  ),
  shellComponent: RootDocument,
});

function RootDocument({ children }: { readonly children: React.ReactNode }) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        <RegistryProvider>{children}</RegistryProvider>
        <Scripts />
      </body>
    </html>
  );
}
