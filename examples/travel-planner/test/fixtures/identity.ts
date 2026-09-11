import { createHash } from "node:crypto";

import { Schema } from "effect";
export const ownerEmail = "owner@example.com";
export const Email = Schema.String.check(Schema.isPattern(/^[^\s@]+@[^\s@]+\.[^\s@]+$/));

// Caller-selected identities exist only in bundled tests. Production uses Auth-created UUIDs.
export const fixtureSubject = (label: string) =>
  createHash("sha256")
    .update(label)
    .digest("hex")
    .slice(0, 32)
    .replace(/(.{8})(.{4})(.{4})(.{4})(.{12})/, "$1-$2-$3-$4-$5");

export const fixtureSession = (label = ownerEmail) => ({
  subjectId: fixtureSubject(label),
  displayName: label,
});

export const fixtureOwner = (label = ownerEmail) => `account-${fixtureSubject(label)}`;
