import { Context, Effect, Layer, Redacted, Schema } from "effect";

import { type ChangedFile } from "./diff.ts";
import { anchorViolation } from "./render.ts";
import {
  FindingCategory,
  FindingSeverity,
  type ReviewFinding,
  ReviewVerdict,
} from "./review-agent.ts";
import { GitCommitSha } from "./review-state.ts";
import { type ReviewRunOutcome } from "./run.ts";
import { type PullRequestMetadata } from "./source.ts";

const Fingerprint = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const HANDOFF_SIGNATURE_DOMAIN = "effect-agent-pr-review/handoff-v1\u0000";

export const ReviewFindingId = Fingerprint.pipe(
  Schema.brand("@effect-agent/pr-review/ReviewFindingId"),
);
export type ReviewFindingId = typeof ReviewFindingId.Type;

/**
 * One host-selected, anchor-validated finding handed to a distinct
 * implementation Agent. Suggestions remain untrusted evidence: neither this
 * schema nor authentication turns them into an authorized patch.
 */
export class ReviewHandoffFinding extends Schema.Class<ReviewHandoffFinding>(
  "@effect-agent/pr-review/ReviewHandoffFinding",
)({
  id: ReviewFindingId,
  path: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThan(0)),
  severity: FindingSeverity,
  category: Schema.optionalKey(FindingCategory),
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
  suggestion: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(2_000))),
}) {}

/** Immutable, exact-head review evidence suitable for a remediation boundary. */
export class ReviewHandoff extends Schema.Class<ReviewHandoff>(
  "@effect-agent/pr-review/ReviewHandoff",
)({
  version: Schema.Literal(1),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  baseRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  headRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  reviewedHeadSha: GitCommitSha,
  profileFingerprint: Fingerprint,
  reviewFingerprint: Fingerprint,
  verdict: ReviewVerdict,
  findings: Schema.Array(ReviewHandoffFinding).check(Schema.isMaxLength(20)),
}) {}

export class ReviewHandoffEnvelope extends Schema.Class<ReviewHandoffEnvelope>(
  "@effect-agent/pr-review/ReviewHandoffEnvelope",
)({
  version: Schema.Literal(1),
  handoff: ReviewHandoff,
  signature: Fingerprint,
}) {}

export class ReviewHandoffBuildFailure extends Schema.TaggedError<ReviewHandoffBuildFailure>()(
  "ReviewHandoffBuildFailure",
  {
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class ReviewHandoffAuthenticationFailure extends Schema.TaggedError<ReviewHandoffAuthenticationFailure>()(
  "ReviewHandoffAuthenticationFailure",
  {
    operation: Schema.Literals(["sign", "verify"]),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

const authenticationFailure = (
  operation: "sign" | "verify",
  cause: unknown,
): ReviewHandoffAuthenticationFailure =>
  ReviewHandoffAuthenticationFailure.make({
    operation,
    reason: String(cause).slice(0, 2_048) || "handoff authentication failed",
  });

const sha256Hex = (text: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const digest = await globalThis.crypto.subtle.digest("SHA-256", new TextEncoder().encode(text));
    return Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  });

const encodeHandoff = (handoff: ReviewHandoff): Effect.Effect<string> =>
  Effect.sync(() => Schema.encodeSync(Schema.fromJsonString(ReviewHandoff))(handoff));

const signatureBytes = (signature: string): ArrayBuffer => {
  const pairs = signature.match(/../g) ?? [];
  const buffer = new ArrayBuffer(pairs.length);
  const bytes = new Uint8Array(buffer);
  for (let index = 0; index < pairs.length; index += 1) {
    bytes[index] = Number.parseInt(pairs[index] ?? "", 16);
  }
  return buffer;
};

const importHmacKey = (secret: Redacted.Redacted<string>, operation: "sign" | "verify") =>
  Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.importKey(
        "raw",
        new TextEncoder().encode(Redacted.value(secret)),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["sign", "verify"],
      ),
    catch: (cause) => authenticationFailure(operation, cause),
  });

/** SHA-256 identity of the complete schema-encoded handoff. */
export const computeReviewHandoffDigest = (handoff: ReviewHandoff): Effect.Effect<string> =>
  encodeHandoff(handoff).pipe(Effect.flatMap(sha256Hex));

const findingMaterial = Schema.Struct({
  repository: Schema.String,
  pullRequestNumber: Schema.Int,
  reviewedHeadSha: GitCommitSha,
  path: Schema.String,
  startLine: Schema.Int,
  endLine: Schema.Int,
  severity: FindingSeverity,
  category: Schema.optionalKey(FindingCategory),
  title: Schema.String,
  body: Schema.String,
  suggestion: Schema.optionalKey(Schema.String),
});

const toHandoffFinding = Effect.fn("ReviewHandoff.toHandoffFinding")(function* (
  metadata: Pick<PullRequestMetadata, "repository" | "number" | "headSha">,
  finding: ReviewFinding,
) {
  const material = {
    repository: metadata.repository,
    pullRequestNumber: metadata.number,
    reviewedHeadSha: metadata.headSha,
    path: finding.path,
    startLine: finding.startLine,
    endLine: finding.endLine,
    severity: finding.severity,
    ...(finding.category === undefined ? {} : { category: finding.category }),
    title: finding.title,
    body: finding.body,
    ...(finding.suggestion === undefined ? {} : { suggestion: finding.suggestion }),
  };
  const encoded = Schema.encodeSync(Schema.fromJsonString(findingMaterial))(material);
  const id = Schema.decodeSync(ReviewFindingId)(yield* sha256Hex(encoded));
  return Object.freeze(
    ReviewHandoffFinding.make({
      id,
      path: finding.path,
      startLine: finding.startLine,
      endLine: finding.endLine,
      severity: finding.severity,
      ...(finding.category === undefined ? {} : { category: finding.category }),
      title: finding.title,
      body: finding.body,
      ...(finding.suggestion === undefined ? {} : { suggestion: finding.suggestion }),
    }),
  );
});

const reviewFingerprintMaterial = Schema.Struct({
  reviewedHeadSha: GitCommitSha,
  profileFingerprint: Fingerprint,
  summary: Schema.String,
  verdict: ReviewVerdict,
  findings: Schema.Array(ReviewHandoffFinding),
});

/**
 * Convert a settled review plus its exact source snapshot into bounded,
 * host-selected remediation evidence. Incomplete coverage and invalid
 * blocking/important anchors fail closed instead of being delegated.
 */
export const makeReviewHandoff = Effect.fn("makeReviewHandoff")(function* (input: {
  readonly outcome: ReviewRunOutcome;
  readonly metadata: PullRequestMetadata;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly profileFingerprint: string;
}) {
  if (input.outcome.coverage.status !== "complete") {
    return yield* ReviewHandoffBuildFailure.make({
      reason: "review coverage is incomplete; remediation requires a complete host-owned review",
    });
  }
  const reviewedHeadSha = yield* Schema.decodeUnknownEffect(GitCommitSha)(
    input.metadata.headSha,
  ).pipe(
    Effect.mapError(() =>
      ReviewHandoffBuildFailure.make({ reason: "the reviewed head is not a valid commit SHA" }),
    ),
  );
  const profileFingerprint = yield* Schema.decodeUnknownEffect(Fingerprint)(
    input.profileFingerprint,
  ).pipe(
    Effect.mapError(() =>
      ReviewHandoffBuildFailure.make({ reason: "the review profile fingerprint is invalid" }),
    ),
  );
  const candidates = input.outcome.activeFindings.filter(
    (finding) => finding.severity === "blocking" || finding.severity === "important",
  );
  const invalid = candidates.find((finding) => anchorViolation(finding, input.files) !== undefined);
  if (invalid !== undefined) {
    return yield* ReviewHandoffBuildFailure.make({
      reason: `selected finding '${invalid.title}' is not anchored to the reviewed changeset`,
    });
  }
  const findings = yield* Effect.forEach(candidates, (finding) =>
    toHandoffFinding({ ...input.metadata, headSha: reviewedHeadSha }, finding),
  );
  const fingerprintJson = Schema.encodeSync(Schema.fromJsonString(reviewFingerprintMaterial))({
    reviewedHeadSha,
    profileFingerprint,
    summary: input.outcome.review.summary,
    verdict: input.outcome.review.verdict,
    findings,
  });
  const reviewFingerprint = Schema.decodeSync(Fingerprint)(yield* sha256Hex(fingerprintJson));
  const handoff = ReviewHandoff.make({
    version: 1,
    repository: input.metadata.repository,
    pullRequestNumber: input.metadata.number,
    baseRef: input.metadata.baseRef,
    headRef: input.metadata.headRef,
    reviewedHeadSha,
    profileFingerprint,
    reviewFingerprint,
    verdict: input.outcome.review.verdict,
    findings,
  });
  Object.freeze(handoff.findings);
  return Object.freeze(handoff);
});

/** HMAC-authenticated transport boundary for review-to-remediation handoffs. */
export class ReviewHandoffAuthenticator extends Context.Service<
  ReviewHandoffAuthenticator,
  {
    readonly sign: (
      handoff: ReviewHandoff,
    ) => Effect.Effect<ReviewHandoffEnvelope, ReviewHandoffAuthenticationFailure>;
    readonly verify: (
      envelope: unknown,
    ) => Effect.Effect<ReviewHandoff, ReviewHandoffAuthenticationFailure>;
  }
>()("@effect-agent/pr-review/ReviewHandoffAuthenticator") {}

export const webCryptoReviewHandoffAuthenticatorLayer = (secret: Redacted.Redacted<string>) =>
  Layer.effect(
    ReviewHandoffAuthenticator,
    Effect.gen(function* () {
      const sign = Effect.fn("ReviewHandoffAuthenticator.sign")(function* (handoff: ReviewHandoff) {
        const encoded = yield* encodeHandoff(handoff).pipe(
          Effect.mapError((cause) => authenticationFailure("sign", cause)),
        );
        const key = yield* importHmacKey(secret, "sign");
        const signature = yield* Effect.tryPromise({
          try: () =>
            globalThis.crypto.subtle.sign(
              "HMAC",
              key,
              new TextEncoder().encode(`${HANDOFF_SIGNATURE_DOMAIN}${encoded}`),
            ),
          catch: (cause) => authenticationFailure("sign", cause),
        });
        const hex = Array.from(new Uint8Array(signature))
          .map((byte) => byte.toString(16).padStart(2, "0"))
          .join("");
        return Object.freeze(ReviewHandoffEnvelope.make({ version: 1, handoff, signature: hex }));
      });

      const verify = Effect.fn("ReviewHandoffAuthenticator.verify")(function* (envelope: unknown) {
        const decoded = yield* Schema.decodeUnknownEffect(ReviewHandoffEnvelope)(envelope).pipe(
          Effect.mapError((cause) => authenticationFailure("verify", cause)),
        );
        const encoded = yield* encodeHandoff(decoded.handoff).pipe(
          Effect.mapError((cause) => authenticationFailure("verify", cause)),
        );
        const key = yield* importHmacKey(secret, "verify");
        const valid = yield* Effect.tryPromise({
          try: () =>
            globalThis.crypto.subtle.verify(
              "HMAC",
              key,
              signatureBytes(decoded.signature),
              new TextEncoder().encode(`${HANDOFF_SIGNATURE_DOMAIN}${encoded}`),
            ),
          catch: (cause) => authenticationFailure("verify", cause),
        });
        if (!valid) {
          return yield* ReviewHandoffAuthenticationFailure.make({
            operation: "verify",
            reason: "handoff signature is invalid",
          });
        }
        for (const finding of decoded.handoff.findings) Object.freeze(finding);
        Object.freeze(decoded.handoff.findings);
        return Object.freeze(decoded.handoff);
      });

      return ReviewHandoffAuthenticator.of({ sign, verify });
    }),
  );
