import { expect, it } from "vite-plus/test";

import { responseTextPreview } from "../src/server/response-stream.ts";

it("previews only the top-level message incrementally across escaped strings and emoji", () => {
  const preview = responseTextPreview();

  const chunks = [
    '{"content":{"message":"NESTED_SECRET","note":"NOTE_SECRET"},"mes',
    'sage":"A ',
    '\\"quiet',
    '\\" stay\\',
    "nTahoe ",
    "\\uD83",
    "D\\uDE80",
    '.","other":"OTHER_SECRET"}',
  ];

  const deltas = chunks.map(preview);

  expect(deltas[0]).toBe("");
  expect(deltas[1]).toBe("A ");
  expect(deltas.filter((delta) => delta.length > 0).length).toBeGreaterThan(2);
  expect(deltas.join("")).toBe('A "quiet" stay\nTahoe 🚀.');
  expect(deltas.join("")).not.toContain("SECRET");
});

it("bounds preview text and input, and stops malformed streams without throwing", () => {
  const bounded = responseTextPreview();

  expect(bounded(`{"message":"${"x".repeat(4_100)}`)).toBe("x".repeat(4_000));
  expect(bounded('more","content":null}')).toBe("");
  const oversized = responseTextPreview();

  expect(oversized(`{"content":{"note":"${"x".repeat(24 * 1_024)}`)).toBe("");
  expect(oversized('"},"message":"MUST_NOT_RESUME"}')).toBe("");
  const malformed = responseTextPreview();

  expect(malformed('{"message":"Visible')).toBe("Visible");
  expect(() => malformed('\\q broken"}')).not.toThrow();
  expect(malformed('{"message":"MUST_NOT_RESUME"}')).toBe("");
  const invalidMessage = responseTextPreview();

  expect(() =>
    invalidMessage('{"message":{"message":"NESTED_SECRET"},"notes":"NOTE_SECRET"}'),
  ).not.toThrow();
  expect(invalidMessage(" ")).toBe("");
});
