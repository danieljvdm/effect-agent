import { Effect } from "effect";

import type { ChangedFile } from "./diff.ts";

// ---------------------------------------------------------------------------
// Changeset fingerprinting: dedupe re-reviews of an UNCHANGED effective diff.
// Repositories that auto-merge the base branch into open pull requests fire
// `synchronize` on every base update; the head SHA moves but the three-dot
// changeset the reviewer reads is byte-identical. The fingerprint hashes the
// (ignore-filtered) changeset together with a prompt signature — everything
// that shapes the review — so a rebase with no content change skips, while a
// real change, a conflict resolution, or a guidance change reviews again.
//
// The reviewer is deployment class E and owns no storage: the fingerprint is
// embedded in the posted review body as an invisible HTML comment, so the
// published review itself is the deduplication state.
// ---------------------------------------------------------------------------

const MARKER_PREFIX = "<!-- effect-agent-pr-review fingerprint=sha256:";
const MARKER_SUFFIX = " -->";
const MARKER_PATTERN = /<!-- effect-agent-pr-review fingerprint=sha256:([0-9a-f]{64}) -->/g;

/** Render the invisible review-body marker for one fingerprint. */
export const renderFingerprintMarker = (fingerprint: string): string =>
  `${MARKER_PREFIX}${fingerprint}${MARKER_SUFFIX}`;

/** The rendered marker length is fixed; publication reserves room for it. */
export const FINGERPRINT_MARKER_LENGTH = renderFingerprintMarker("0".repeat(64)).length;

/** Extract the last fingerprint marker in one review body, if any. */
export const extractFingerprint = (body: string): string | undefined => {
  let last: string | undefined;
  for (const match of body.matchAll(MARKER_PATTERN)) {
    last = match[1];
  }
  return last;
};

/** WebCrypto SHA-256; unavailable crypto is a defect, not an expected failure. */
const sha256Hex = (text: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
    return Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  });

const FIELD = "\u0000";
const RECORD = "\u0001";
const SECTION = "\u0002";

/**
 * Canonical changeset encoding: sorted by path so provider ordering never
 * matters, with every review-relevant field of every file.
 */
const canonicalChangeset = (files: ReadonlyArray<ChangedFile>): string =>
  files
    .map(
      (file) =>
        `${file.path}${FIELD}${file.status}${FIELD}${String(file.additions)}${FIELD}${String(file.deletions)}${FIELD}${file.patch ?? ""}${FIELD}${file.reviewBaseContent ?? ""}${FIELD}${file.reviewHeadContent ?? ""}`,
    )
    .sort()
    .join(RECORD);

/**
 * Fingerprint one review's complete input surface: the (already
 * ignore-filtered) changeset plus the caller's prompt signature — the
 * rendered instructions and any review-shaping options the instructions do
 * not carry.
 */
export const computeChangesetFingerprint = (
  files: ReadonlyArray<ChangedFile>,
  signature: string,
): Effect.Effect<string> => sha256Hex(`${canonicalChangeset(files)}${SECTION}${signature}`);
