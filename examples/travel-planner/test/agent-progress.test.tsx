import { Schema } from "effect";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vite-plus/test";

import { AgentProgress } from "../src/components/agent-progress.tsx";
import { PlannerProgress } from "../src/domain.ts";

const progress = (text = "", running = true) =>
  Schema.decodeUnknownSync(PlannerProgress)({
    submissionId: "submission",
    attemptId: "attempt",
    revision: 2,
    text,
    tools: [
      { id: "first", label: "Reading a listing", state: "complete" },
      { id: "second", label: "Searching the web", state: running ? "running" : "complete" },
    ],
  });

it("keeps the step count in the trigger while current tool details remain expandable", () => {
  for (const running of [true, false]) {
    const html = renderToStaticMarkup(
      <AgentProgress progress={progress("", running)} active busy />,
    );

    const summary = html.match(/<summary>([\s\S]*?)<\/summary>/)?.[1];

    expect(summary).toContain("2 steps so far");
    expect(summary).not.toContain("Searching the web");
    expect(html).toContain("Searching the web");
    expect(html).not.toContain("Thinking through your trip");
  }
});

it("shows the supplied public update without a duplicate thinking status", () => {
  const html = renderToStaticMarkup(
    <AgentProgress progress={{ ...progress("I found a lakeside stay."), tools: [] }} active busy />,
  );

  expect(html).toContain("I found a lakeside stay.");
  expect(html).toContain('aria-label="Response in progress"');
  expect(html).not.toContain("Thinking through your trip");
  expect(html).not.toContain("<summary>");
});

it("reserves the thinking fallback for busy turns with no text or steps", () => {
  expect(renderToStaticMarkup(<AgentProgress progress={null} active={false} busy />)).toContain(
    "Thinking through your trip",
  );
  expect(renderToStaticMarkup(<AgentProgress progress={null} active={false} busy={false} />)).toBe(
    "",
  );

  const settled = renderToStaticMarkup(
    <AgentProgress progress={progress("Saved response", false)} active={false} busy={false} />,
  );

  expect(settled).toContain("2 steps in this response");
  expect(settled).not.toContain("Saved response");
  expect(settled).not.toContain("Thinking through your trip");
});

it("shows unfinished searches neutrally while preserving success and explicit failure labels", () => {
  const snapshot = progress();

  for (const [state, label] of [
    ["complete", "Done"],
    ["incomplete", "Not completed"],
    ["failed", "Couldn&#x27;t finish"],
  ] as const) {
    const html = renderToStaticMarkup(
      <AgentProgress
        progress={{
          ...snapshot,
          tools: [
            { id: "search", label: "Searching the web", state, startedAt: 0, completedAt: 1000 },
          ],
        }}
        active={false}
        busy={false}
      />,
    );

    expect(html).toContain(label);
    expect(html.includes("Couldn")).toBe(state === "failed");
    expect(html.includes("✓")).toBe(state === "complete");
    expect(html).not.toContain("spinning");
  }
});
