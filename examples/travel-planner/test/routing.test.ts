import { createMemoryHistory, createRouter } from "@tanstack/react-router";
import { expect, it } from "vite-plus/test";

import { routeTree } from "../src/routeTree.gen";

const setup = (path: string) => {
  const history = createMemoryHistory({ initialEntries: [path] });

  const router = createRouter({
    routeTree,
    history,
    isServer: false,
    origin: "https://planner.test",
  });

  return { router, history };
};

it("keeps conversation addresses across direct loads, new trips, and browser history", async () => {
  const { router, history } = setup("/conversations/saved-conversation");

  await router.load();
  expect(router.state.matches.at(-1)?.params).toEqual({ conversationId: "saved-conversation" });
  await router.navigate({ to: "/" });
  const freshPath = history.location.pathname;

  expect(freshPath).toMatch(/^\/conversations\/[a-f0-9-]{36}$/);
  history.back();
  await router.load();
  expect(history.location.pathname).toBe("/conversations/saved-conversation");
  history.forward();
  await router.load();
  expect(history.location.pathname).toBe(freshPath);

  const reloaded = setup(freshPath).router;

  await reloaded.load();
  expect(reloaded.state.location.pathname).toBe(freshPath);
  expect(reloaded.state.matches.at(-1)?.params).toEqual({
    conversationId: freshPath.split("/").at(-1),
  });
  await router.navigate({ to: "/" });
  expect(history.location.pathname).not.toBe(freshPath);
});

it("rejects malformed conversation URLs and leaves valid unsaved conversations addressable", async () => {
  for (const id of ["bad%20id", "bad%2Fid", "x".repeat(241)]) {
    const { router } = setup(`/conversations/${id}`);

    await router.load();
    expect(router.state.matches.some((match) => match.status === "notFound")).toBe(true);
  }

  const { router } = setup("/conversations/unfinished");

  await router.load();
  expect(router.state.matches.at(-1)?.status).toBe("success");
  expect(router.state.matches.at(-1)?.params).toEqual({ conversationId: "unfinished" });
});
