import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Login } from "../auth/login";

export const Route = createFileRoute("/auth/github/callback")({
  component: () => (
    <ClientOnly>
      <Login callback />
    </ClientOnly>
  ),
});
