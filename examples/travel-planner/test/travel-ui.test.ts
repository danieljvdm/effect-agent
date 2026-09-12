import { Schema } from "effect";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vite-plus/test";

import { MessageText } from "../src/components/message-text.tsx";
import { TravelCards } from "../src/components/travel/travel-cards.tsx";
import { TravelContent, TravelUrl } from "../src/travel-content.ts";

it("renders existing photo replies as Markdown without admitting HTML or executable URLs", () => {
  const html = renderToStaticMarkup(
    createElement(MessageText, {
      text: "### A lakeside stay\n\n![Deck by the lake](https://a0.muscache.com/listing.jpg)\n\n- **Private hot tub**\n\n[Listing](https://www.airbnb.com/rooms/123)\n\n<script>alert(1)</script>\n\n[Unsafe](javascript:alert) ![Local](https://127.0.0.1/private.png)",
    }),
  );

  expect(html).toContain("<h3");
  expect(html).toContain("<strong");
  expect(html).toContain('src="https://a0.muscache.com/listing.jpg"');
  expect(html).toContain('alt="Deck by the lake"');
  expect(html).toContain('referrerPolicy="no-referrer"');
  expect(html).toContain('href="https://www.airbnb.com/rooms/123"');
  expect(html).not.toContain("<script");
  expect(html).not.toContain("javascript:");
  expect(html).not.toContain("127.0.0.1");
});

it("renders structured travel options with sources and unknown-price states, escaping model text", () => {
  const content = Schema.decodeSync(TravelContent)({
    title: "A few possibilities",
    items: [
      {
        kind: "stay",
        name: "<script>Cabin</script>",
        location: "Lake Tahoe",
        url: "https://www.airbnb.com/rooms/123",
        photos: [],
        highlights: ["Hot tub"],
        price: null,
        note: null,
      },
      {
        kind: "flight",
        airline: "Example airline",
        origin: "SFO",
        destination: "LIS",
        departure: null,
        arrival: null,
        duration: null,
        stops: null,
        price: null,
        url: "https://www.example.com/flights",
        note: "Check the airline for dates.",
      },
      {
        kind: "itinerary",
        title: "A slow weekend",
        days: [
          {
            label: "By the water",
            date: "2026-10-12",
            activities: [{ title: "Lakeside walk", time: "Morning", description: null, url: null }],
          },
        ],
      },
      {
        kind: "place",
        category: "restaurant",
        name: "A lakeside cafe",
        location: "Tahoe",
        description: "A quiet lunch",
        url: "https://www.example.com/cafe",
        photo: null,
      },
    ],
  });

  const html = renderToStaticMarkup(createElement(TravelCards, { content }));

  expect(html).toContain("Check dates &amp; price");
  expect(html).toContain("Fare to check");
  expect(html).toContain("Lakeside walk");
  expect(html).toContain("A lakeside cafe");
  expect(html).toContain('href="https://www.airbnb.com/rooms/123"');
  expect(html).not.toContain("<script");
  expect(html).toContain("&lt;script&gt;Cabin&lt;/script&gt;");
});

it("rejects unsafe links and empty display payloads before rendering", () => {
  for (const url of [
    "javascript:alert(1)",
    "data:image/svg+xml,bad",
    "http://example.com/image.jpg",
    "https://user:secret@example.com/a",
    "https://127.0.0.1/a",
    "https://localhost/a",
    "https://localhost./a",
    "https://machine.internal./a",
    "https://example.com:8443/a",
  ])
    expect(Schema.is(TravelUrl)(url)).toBe(false);
  expect(Schema.is(TravelUrl)("https://a0.muscache.com/picture.jpg?im_w=720")).toBe(true);
  expect(Schema.is(TravelContent)({ title: "Empty", items: [] })).toBe(false);
});
