import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Login } from "../auth/login";

export const Route = createFileRoute("/login")({
  component: () => (
    <ClientOnly>
      <Login />
    </ClientOnly>
  ),
});
