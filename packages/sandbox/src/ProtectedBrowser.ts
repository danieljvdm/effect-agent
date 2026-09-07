import { Context, Effect, Layer, Schema, Scope, type Redacted } from "effect";

import {
  InteractiveBrowserHost,
  InteractiveBrowserTargetUrl,
  type InteractiveBrowserPolicy,
} from "./InteractiveBrowser.ts";

/** Canonical HTTPS origin, including a non-default port. Host grants match exactly. */
export const CredentialOrigin = Schema.String.check(
  Schema.makeFilter(
    (value) => value.startsWith("https://") && Schema.is(InteractiveBrowserHost)(value.slice(8)),
  ),
);

export const BrowserReference = Schema.String.check(Schema.isUUID());
export const CredentialKind = Schema.Literals(["login", "card"]);

export const CredentialFieldRole = Schema.Literals([
  "username",
  "password",
  "card-name",
  "card-number",
  "card-expiry",
  "card-expiry-month",
  "card-expiry-year",
  "card-security-code",
]);

const Label = Schema.String.check(Schema.isMaxLength(200));
const LinkUrl = InteractiveBrowserTargetUrl.check(Schema.isStartsWith("https://"));

export class CredentialTarget extends Schema.Class<CredentialTarget>("CredentialTarget")({
  topOrigin: CredentialOrigin,
  frameOrigin: CredentialOrigin,
  recipientOrigin: CredentialOrigin,
  document: BrowserReference,
  frame: BrowserReference,
  form: BrowserReference,
}) {}

export class ProtectedBrowserControl extends Schema.Class<ProtectedBrowserControl>(
  "ProtectedBrowserControl",
)({
  ref: BrowserReference,
  target: CredentialTarget,
  role: Schema.Union([
    CredentialFieldRole,
    Schema.Literals([
      "text",
      "select",
      "radio",
      "checkbox",
      "submit",
      "link",
      "button",
      "unsupported",
    ]),
  ]),
  label: Label,
  /** Current native radio/checkbox state; field values are never included. */
  checked: Schema.optionalKey(Schema.Boolean),
  /** Resolved HTTPS destination for a native link; target.recipientOrigin is its origin. */
  url: Schema.optionalKey(LinkUrl),
}) {}

export class ProtectedBrowserObservation extends Schema.Class<ProtectedBrowserObservation>(
  "ProtectedBrowserObservation",
)({
  document: BrowserReference,
  topOrigin: CredentialOrigin,
  text: Schema.String.check(Schema.isMaxLength(64 * 1024)),
  controls: Schema.Array(ProtectedBrowserControl).check(Schema.isMaxLength(64)),
  truncated: Schema.Boolean,
  observation: Schema.Literals(["before-exposure", "approved-after-exposure"]),
}) {}

export class CredentialOfferMetadata extends Schema.Class<CredentialOfferMetadata>(
  "CredentialOfferMetadata",
)({
  label: Label,
  brand: Schema.optionalKey(Label),
  lastFour: Schema.optionalKey(Schema.String.check(Schema.isPattern(/^\d{4}$/))),
  /** Plaintext metadata disclosed only when the host authorizes listing this offer. */
  billingAddress: Schema.optionalKey(
    Schema.Struct({
      name: Schema.optionalKey(Label),
      line1: Schema.optionalKey(Label),
      line2: Schema.optionalKey(Label),
      city: Schema.optionalKey(Label),
      region: Schema.optionalKey(Label),
      postalCode: Schema.optionalKey(Label),
      country: Schema.optionalKey(Label),
    }),
  ),
}) {}

export class CredentialOffer extends Schema.Class<CredentialOffer>("CredentialOffer")({
  ref: BrowserReference,
  kind: CredentialKind,
  metadata: CredentialOfferMetadata,
}) {}

export class ListCredentialOffers extends Schema.Class<ListCredentialOffers>(
  "ListCredentialOffers",
)({
  kind: CredentialKind,
  target: BrowserReference,
}) {}

export class UseCredential extends Schema.Class<UseCredential>("UseCredential")({
  offer: BrowserReference,
  fields: Schema.Array(Schema.Struct({ ref: BrowserReference, role: CredentialFieldRole })).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(8),
  ),
  submit: Schema.optionalKey(BrowserReference),
}) {}

export class ProtectedBrowserNavigate extends Schema.Class<ProtectedBrowserNavigate>(
  "ProtectedBrowserNavigate",
)({
  url: Schema.String.check(Schema.isMaxLength(8192)),
}) {}

export class ProtectedBrowserClick extends Schema.Class<ProtectedBrowserClick>(
  "ProtectedBrowserClick",
)({
  ref: BrowserReference,
}) {}

/**
 * Non-secret text for an ordinary text control or native single select in the current observation.
 * Never pass credential material. Credential roles, including username, require useCredential.
 * Selects match a unique enabled option value, then a unique exact trimmed label. Empty text clears.
 */
export class ProtectedBrowserFill extends Schema.Class<ProtectedBrowserFill>(
  "ProtectedBrowserFill",
)(
  Schema.Struct({
    ref: BrowserReference,
    value: Schema.String.check(Schema.isMaxLength(8192)),
  }).pipe(Schema.annotate({ parseOptions: { onExcessProperty: "error" } })),
) {}

export const CredentialDispatch = Schema.Literals([
  "not-dispatched",
  "possibly-dispatched",
  "dispatched",
]);

export const CredentialMilestone = Schema.Literals([
  "none",
  "partial-fill",
  "filled",
  "submission-dispatched",
]);

export const ProtectedObservationState = Schema.Literals([
  "before-exposure",
  "protected",
  "approved-after-exposure",
  "closed",
]);

export const ProtectedCleanup = Schema.Literals(["not-requested", "confirmed", "unconfirmed"]);

const Evidence = {
  dispatch: CredentialDispatch,
  milestone: CredentialMilestone,
  observation: ProtectedObservationState,
  cleanup: ProtectedCleanup,
};

/** No foreign message/cause or page data may enter this failure. Closure does not undo dispatch. */
export class ProtectedBrowserError extends Schema.TaggedError<ProtectedBrowserError>()(
  "ProtectedBrowserError",
  {
    reason: Schema.Literals([
      "denied",
      "missing-credential",
      "stale-reference",
      "unsupported",
      "needs-attention",
      "busy",
      "closed",
      "limit",
      "timeout",
      "provider",
      "resolver",
      "outcome-unknown",
      "observation-blocked",
    ]),
    ...Evidence,
  },
) {}

export class CredentialUseResult extends Schema.Class<CredentialUseResult>("CredentialUseResult")({
  ...Evidence,
  authentication: Schema.Literal("unverified"),
}) {}

const SecretText = Schema.Redacted(Schema.NonEmptyString.check(Schema.isMaxLength(1024)));

/** Host-only material. Never encode it into Tool results, checkpoints, or storage. */
export const LoginCredential = Schema.TaggedStruct("LoginCredential", {
  username: SecretText,
  password: SecretText,
});

/** Security codes are transient, never persisted. Dummy tests are not PCI compliance evidence. */
export const CardCredential = Schema.TaggedStruct("CardCredential", {
  name: SecretText,
  number: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{12,19}$/))),
  expiry: SecretText,
  expiryMonth: Schema.Redacted(Schema.String.check(Schema.isPattern(/^(0[1-9]|1[0-2])$/))),
  expiryYear: Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{4}$/))),
  securityCode: Schema.optionalKey(
    Schema.Redacted(Schema.String.check(Schema.isPattern(/^\d{3,4}$/))),
  ),
});

export const BrowserCredentialMaterial = Schema.Union([LoginCredential, CardCredential]);
export type BrowserCredentialMaterial = typeof BrowserCredentialMaterial.Type;

export class CredentialAccessError extends Schema.TaggedError<CredentialAccessError>()(
  "CredentialAccessError",
  {
    reason: Schema.Literals(["denied", "missing-credential", "needs-attention", "resolver"]),
  },
) {}

export interface CredentialAccessRequest {
  readonly caller: Redacted.Redacted<string>;
  readonly kind: typeof CredentialKind.Type;
  readonly target: CredentialTarget;
}

export interface CredentialUseAuthorization extends CredentialAccessRequest {
  readonly key: Redacted.Redacted<string>;
  readonly roles: ReadonlyArray<typeof CredentialFieldRole.Type>;
  readonly submit: boolean;
}

/** Exact current action proposed to host authority; no ordinary fill value or secret is included. */
export const ProtectedBrowserAction = Schema.Union([
  Schema.TaggedStruct("Navigate", { url: ProtectedBrowserNavigate.fields.url }),
  Schema.TaggedStruct("Fill", {
    ref: BrowserReference,
    target: CredentialTarget,
    role: Schema.Literals(["text", "select"]),
  }),
  Schema.TaggedStruct("Click", {
    ref: BrowserReference,
    target: CredentialTarget,
    role: Schema.Literals(["button", "radio", "checkbox"]),
  }),
  Schema.TaggedStruct("Click", {
    ref: BrowserReference,
    target: CredentialTarget,
    role: Schema.Literal("link"),
    url: LinkUrl,
  }),
  Schema.TaggedStruct("Submit", { ref: BrowserReference, target: CredentialTarget }),
]);

export type ProtectedBrowserAction = typeof ProtectedBrowserAction.Type;

/**
 * Trust only these current observation origins. The top origin must be included. Other frames
 * supply neither text nor usable refs. This does not expand network policy or credential grants.
 */
export class CredentialObservationGrant extends Schema.Class<CredentialObservationGrant>(
  "CredentialObservationGrant",
)({
  decision: Schema.Literal("trust-recipient-no-credential-echo"),
  origins: Schema.Array(CredentialOrigin).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(16),
    Schema.isUnique(),
  ),
}) {}

export const CredentialObservationDecision = Schema.Union([
  Schema.Literals(["trust-recipient-no-credential-echo", "deny"]),
  CredentialObservationGrant,
]);

/**
 * Host-only vault and grant port. Derive caller from the authorized invocation, never Tool input.
 * Recheck account ownership, current grants, merchant/frame/recipient pairing, and any side
 * effects caused by filling. Listing authorizes metadata only. Grant checks never widen network
 * policy. Implementations must not log material or put it in defects/traces.
 */
export class BrowserCredentialAccess extends Context.Service<
  BrowserCredentialAccess,
  {
    readonly caller: Effect.Effect<Redacted.Redacted<string>, CredentialAccessError>;
    readonly list: (request: CredentialAccessRequest) => Effect.Effect<
      ReadonlyArray<{
        readonly key: Redacted.Redacted<string>;
        readonly metadata: CredentialOfferMetadata;
      }>,
      CredentialAccessError
    >;
    readonly authorize: (
      request: CredentialUseAuthorization,
    ) => Effect.Effect<void, CredentialAccessError>;
    readonly resolve: (
      request: CredentialUseAuthorization,
    ) => Effect.Effect<BrowserCredentialMaterial, CredentialAccessError>;
    /**
     * Optional host authority for every ordinary navigate/fill/click, mandatory for Submit.
     * Recheck caller ownership, user intent, current submission/continuation grants for ALL prior
     * exposures, and this exact target. Exposures may come from separate credential frames/forms.
     * Without this hook, ordinary actions retain their observation gate and Submit is unsupported.
     * Observation trust alone never authorizes a new native submit. Called again on each action;
     * approvals are not cached. The pass revalidates caller and target after this hook returns.
     */
    readonly authorizeAction?: (request: {
      readonly caller: Redacted.Redacted<string>;
      readonly action: ProtectedBrowserAction;
      readonly exposures: ReadonlyArray<CredentialTarget>;
    }) => Effect.Effect<void, CredentialAccessError>;
    /**
     * Explicitly trust all observed destinations not to echo credentials, including transformed
     * or delayed echoes. Called for every observation and non-secret action after exposure.
     * The string grant trusts all current observed origins. Return CredentialObservationGrant
     * to select an explicit subset instead. This is recipient trust, not submission authority,
     * a universal secrecy guarantee, or a login verifier.
     */
    readonly observation: (request: {
      readonly caller: Redacted.Redacted<string>;
      readonly topOrigin: typeof CredentialOrigin.Type;
      readonly frameOrigins: ReadonlyArray<typeof CredentialOrigin.Type>;
      readonly exposures: ReadonlyArray<CredentialTarget>;
    }) => Effect.Effect<typeof CredentialObservationDecision.Type, CredentialAccessError>;
  }
>()("@effect-agent/sandbox/BrowserCredentialAccess") {}

/** A private ephemeral pass. No selectors, JavaScript, screenshots, viewer, or provider identity. */
export interface ProtectedBrowserHandle {
  readonly navigate: (
    request: ProtectedBrowserNavigate,
  ) => Effect.Effect<void, ProtectedBrowserError>;
  readonly observe: Effect.Effect<ProtectedBrowserObservation, ProtectedBrowserError>;
  readonly click: (request: ProtectedBrowserClick) => Effect.Effect<void, ProtectedBrowserError>;
  /** Uses the same lock, budgets, current-target checks and post-exposure grant as other actions. */
  readonly fill: (request: ProtectedBrowserFill) => Effect.Effect<void, ProtectedBrowserError>;
  readonly listCredentialOffers: (
    request: ListCredentialOffers,
  ) => Effect.Effect<ReadonlyArray<CredentialOffer>, ProtectedBrowserError>;
  readonly useCredential: (
    request: UseCredential,
  ) => Effect.Effect<CredentialUseResult, ProtectedBrowserError>;
  readonly close: Effect.Effect<typeof ProtectedCleanup.Type>;
}

export class ProtectedBrowser extends Context.Service<
  ProtectedBrowser,
  {
    readonly open: (
      policy: InteractiveBrowserPolicy,
    ) => Effect.Effect<
      ProtectedBrowserHandle,
      ProtectedBrowserError,
      Scope.Scope | BrowserCredentialAccess
    >;
  }
>()("@effect-agent/sandbox/ProtectedBrowser") {}

/** Build once in an execution/Attempt Layer. Acquires lazily and shares one pass across Tools. */
export class ProtectedBrowserSession extends Context.Service<
  ProtectedBrowserSession,
  {
    readonly get: Effect.Effect<ProtectedBrowserHandle, ProtectedBrowserError>;
  }
>()("@effect-agent/sandbox/ProtectedBrowserSession") {
  static layer(policy: InteractiveBrowserPolicy) {
    return Layer.effect(this)(
      Effect.gen(function* () {
        const browser = yield* ProtectedBrowser;
        const access = yield* BrowserCredentialAccess;
        const scope = yield* Effect.scope;

        const cached = yield* Effect.cached(
          browser
            .open(policy)
            .pipe(
              Effect.provideService(BrowserCredentialAccess, access),
              Effect.provideService(Scope.Scope, scope),
            ),
        );

        return {
          get: Effect.suspend(() =>
            scope.state._tag === "Closed"
              ? Effect.fail(
                  new ProtectedBrowserError({
                    reason: "closed",
                    dispatch: "not-dispatched",
                    milestone: "none",
                    observation: "closed",
                    cleanup: "not-requested",
                  }),
                )
              : cached,
          ),
        };
      }),
    );
  }
}
