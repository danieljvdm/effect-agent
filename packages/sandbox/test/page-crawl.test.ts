import {
  PageCrawlError,
  PageCrawlLimits,
  PageCrawlRecord,
  PageCrawlRequest,
} from "@effect-agent/sandbox/PageCrawl";
import { SandboxImplementation } from "@effect-agent/sandbox/Sandbox";
import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

const implementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "test-page-crawl",
});

describe("PageCrawl schemas", () => {
  it("round-trips bounded requests, records, and expected failures", () => {
    const request = PageCrawlRequest.make({
      startUrl: "https://docs.example.com/start",
      purposes: ["search", "ai-input"],
      limits: PageCrawlLimits.make({
        maxPages: 3,
        maxDepth: 2,
        maxPageBytes: 1_024,
        maxTotalBytes: 3_072,
        deadlineMillis: 30_000,
      }),
    });

    const record = PageCrawlRecord.make({
      url: "https://docs.example.com/page",
      status: "completed",
      markdown: "# Page",
      metadata: {
        status: 200,
        url: "https://docs.example.com/page",
        title: "Page",
      },
    });

    const failure = {
      _tag: "PageCrawlLimitError",
      implementation,
      limit: "total-bytes",
      maximum: 4,
      observed: 5,
      message: "The crawl exceeded its total byte limit",
    };

    expect(
      Schema.decodeSync(PageCrawlRequest)(Schema.encodeSync(PageCrawlRequest)(request)),
    ).toEqual(request);
    expect(Schema.decodeSync(PageCrawlRecord)(Schema.encodeSync(PageCrawlRecord)(record))).toEqual(
      record,
    );
    expect(Schema.decodeUnknownSync(PageCrawlError)(failure)).toMatchObject(failure);
  });

  it("rejects invalid targets, purposes, limits, statuses, and metadata", () => {
    const valid = {
      startUrl: "https://docs.example.com/start",
      purposes: ["search"],
      limits: {
        maxPages: 3,
        maxDepth: 2,
        maxPageBytes: 1_024,
        maxTotalBytes: 3_072,
        deadlineMillis: 30_000,
      },
    };

    for (const input of [
      { ...valid, startUrl: "http://docs.example.com/start" },
      { ...valid, startUrl: "https://user:secret@docs.example.com/start" },
      { ...valid, purposes: [] },
      { ...valid, purposes: ["search", "search"] },
      { ...valid, purposes: ["unknown"] },
      { ...valid, limits: { ...valid.limits, maxPages: 101 } },
      { ...valid, limits: { ...valid.limits, maxDepth: 0 } },
      { ...valid, limits: { ...valid.limits, maxPageBytes: 0 } },
      { ...valid, limits: { ...valid.limits, maxTotalBytes: 64 * 1024 * 1024 + 1 } },
      { ...valid, limits: { ...valid.limits, deadlineMillis: 10 * 60_000 + 1 } },
    ]) {
      expect(Schema.decodeUnknownExit(PageCrawlRequest)(input)._tag).toBe("Failure");
    }

    for (const input of [
      { url: "https://docs.example.com/", status: "unknown" },
      {
        url: "https://docs.example.com/",
        status: "completed",
        metadata: { status: 99, url: "https://docs.example.com/" },
      },
      {
        url: "https://docs.example.com/",
        status: "completed",
        metadata: { status: 200, url: "http://docs.example.com/" },
      },
    ]) {
      expect(Schema.decodeUnknownExit(PageCrawlRecord)(input)._tag).toBe("Failure");
    }
  });
});
