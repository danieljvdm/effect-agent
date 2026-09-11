import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Login, LoginLoading } from "../auth/login";

export const Route = createFileRoute("/login")({
  component: () => (
    <ClientOnly fallback={<LoginLoading step="session" />}>
      <Login />
    </ClientOnly>
  ),
});
