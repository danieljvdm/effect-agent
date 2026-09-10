import { Schema } from "effect";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vite-plus/test";

import { ResearchScoutCard, ResearchScoutDetails } from "../src/components/research-scout-card.tsx";
import { ResearchScoutActivity } from "../src/domain.ts";

const scout = (state: ResearchScoutActivity["state"], text = "") =>
  Schema.decodeUnknownSync(ResearchScoutActivity)({
    id: state,
    state,
    title: "Lakeside stays",
    task: "Find a quiet stay near the lake.",
    progress: {
      submissionId: "submission",
      attemptId: "attempt",
      revision: 1,
      text,
      tools: [
        {
          id: "search",
          label: "Searching the web",
          state: state === "idle" ? "complete" : "running",
        },
      ],
    },
    activity: [
      {
        id: "result",
        kind: "tool",
        text: "Listing research returned",
        timestamp: "2026-09-09T19:00:00.000Z",
        details: [{ label: "Result", text: "<script>Untrusted source</script>", truncated: false }],
      },
    ],
  });

it("summarizes background scouts with stable counts and no current-tool label in the dock", () => {
  const html = renderToStaticMarkup(
    <ResearchScoutCard scouts={[scout("active"), scout("idle"), scout("failed")]} />,
  );

  expect(html).toContain("1 of 3 finished · 1 working · 1 needs attention");
  expect(html).toContain("View research activity");
  expect(html).not.toContain("Searching the web");
  expect(renderToStaticMarkup(<ResearchScoutCard scouts={[]} />)).toBe("");
});

it("keeps finished public findings and escaped canonical details available together", () => {
  const html = renderToStaticMarkup(
    <ResearchScoutDetails scout={scout("idle", "I found a quiet cabin by the lake.")} />,
  );

  expect(html).toContain("Finished");
  expect(html).toContain("Find a quiet stay near the lake.");
  expect(html).toContain("I found a quiet cabin by the lake.");
  expect(html).toContain("1 step in this response");
  expect(html).toContain("Listing research returned");
  expect(html).toContain("&lt;script&gt;Untrusted source&lt;/script&gt;");
  expect(html).not.toContain("<script>");
  expect(html).not.toContain("Thinking through your trip");
});

it("distinguishes failed work from unavailable updates without inventing progress", () => {
  const failed = renderToStaticMarkup(<ResearchScoutDetails scout={scout("failed")} />);
  const unavailable = renderToStaticMarkup(<ResearchScoutDetails scout={scout("unavailable")} />);

  expect(failed).toContain("This scout couldn&#x27;t finish.");
  expect(unavailable).toContain("Updates are temporarily unavailable.");
  expect(unavailable).toContain("Last seen running");
  expect(unavailable).not.toContain("Interrupted");
  expect(unavailable).not.toContain("This scout couldn&#x27;t finish.");
  expect(failed).not.toContain("Thinking through your trip");
  expect(unavailable).not.toContain("Thinking through your trip");
});
