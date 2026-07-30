import { Context, Duration, Schema, Stream } from "effect";

const BoundedName = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedPath = Schema.NonEmptyString.check(Schema.isMaxLength(4 * 1024));
const BoundedArgument = Schema.String.check(Schema.isMaxLength(32 * 1024));
const BoundedOutputText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const PositiveNumber = Schema.Finite.check(Schema.isGreaterThan(0));
const MaxOutputBytes = PositiveInt.check(Schema.isLessThanOrEqualTo(16 * 1024 * 1024));
const BoundedArguments = Schema.Array(BoundedArgument).check(Schema.isMaxLength(256));
const BoundedEnvironmentNames = Schema.Array(BoundedName).check(Schema.isMaxLength(128));
const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);
const FiniteNonNegativeDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && !Duration.isNegative(duration),
    { expected: "a finite non-negative duration" },
  ),
);

/** A requested runtime image or executable environment. */
export class SandboxRuntime extends Schema.Class<SandboxRuntime>("SandboxRuntime")({
  kind: Schema.Literals(["container", "microvm", "wasm", "unisolated-process"]),
  identity: BoundedName,
}) {}

/** The implementation and its isolation posture that executed a request. */
export class SandboxImplementation extends Schema.Class<SandboxImplementation>(
  "SandboxImplementation",
)({
  isolation: Schema.Literals(["isolated", "unisolated"]),
  identity: BoundedName,
}) {}

/** A host path made available to the requested runtime. */
export class SandboxMount extends Schema.Class<SandboxMount>("SandboxMount")({
  source: BoundedPath,
  target: BoundedPath,
  access: Schema.Literals(["read-only", "read-write"]),
}) {}

/** Explicitly denies all workload network access. */
export class NetworkDisabled extends Schema.TaggedClass<NetworkDisabled>()("NetworkDisabled", {}) {}

/** Explicitly permits connections only to the named destination and port allowlists. */
export class NetworkAllowlist extends Schema.TaggedClass<NetworkAllowlist>()("NetworkAllowlist", {
  domains: Schema.Array(BoundedName).check(Schema.isMaxLength(256)),
  ports: Schema.Array(PositiveInt.check(Schema.isLessThanOrEqualTo(65_535))).check(
    Schema.isMaxLength(256),
  ),
}) {}

/** Network access policy supplied to a sandbox implementation. */
export const SandboxNetworkPolicy = Schema.Union([NetworkDisabled, NetworkAllowlist]);
export type SandboxNetworkPolicy = typeof SandboxNetworkPolicy.Type;

/** Names of ambient variables that may be copied into the workload environment. */
export class SandboxEnvironment extends Schema.Class<SandboxEnvironment>("SandboxEnvironment")({
  allow: BoundedEnvironmentNames,
}) {}

/** Limits that a sandbox implementation must either enforce or reject. */
export class SandboxLimits extends Schema.Class<SandboxLimits>("SandboxLimits")({
  cpuCores: Schema.optionalKey(PositiveNumber.check(Schema.isLessThanOrEqualTo(1_024))),
  memoryBytes: Schema.optionalKey(
    PositiveInt.check(Schema.isLessThanOrEqualTo(1024 * 1024 * 1024 * 1024)),
  ),
  maxOutputBytes: MaxOutputBytes,
  maxWallTime: FinitePositiveDuration,
}) {}

/** A secret reference; raw secret values are never part of a sandbox request. */
export class SandboxSecretHandle extends Schema.Class<SandboxSecretHandle>("SandboxSecretHandle")({
  id: BoundedName,
  purpose: Schema.NonEmptyString.check(Schema.isMaxLength(2 * 1024)),
}) {}

/** Rules for copying selected workload output out of an isolated runtime. */
export class SandboxArtifactRule extends Schema.Class<SandboxArtifactRule>("SandboxArtifactRule")({
  path: BoundedPath,
  maxBytes: MaxOutputBytes,
}) {}

/** Metadata for an artifact that a sandbox implementation released after applying its rules. */
export class SandboxArtifact extends Schema.Class<SandboxArtifact>("SandboxArtifact")({
  path: BoundedPath,
  bytes: Schema.Natural,
  digest: BoundedName,
  mediaType: Schema.optionalKey(BoundedName),
}) {}

/** Schema-first command execution request. Command and arguments are intentionally separate. */
export class SandboxRequest extends Schema.Class<SandboxRequest>("SandboxRequest")({
  runtime: SandboxRuntime,
  command: BoundedPath,
  args: BoundedArguments,
  cwd: BoundedPath,
  environment: SandboxEnvironment,
  mounts: Schema.Array(SandboxMount).check(Schema.isMaxLength(64)),
  network: SandboxNetworkPolicy,
  limits: SandboxLimits,
  secretHandles: Schema.Array(SandboxSecretHandle).check(Schema.isMaxLength(64)),
  artifactRules: Schema.Array(SandboxArtifactRule).check(Schema.isMaxLength(64)),
}) {}

/** Accounting that a sandbox implementation was able to observe for one execution. */
export class SandboxResourceUse extends Schema.Class<SandboxResourceUse>("SandboxResourceUse")({
  wallTime: FiniteNonNegativeDuration,
  stdoutBytes: Schema.Natural,
  stderrBytes: Schema.Natural,
  cpuMillis: Schema.optionalKey(Schema.Natural),
  memoryBytes: Schema.optionalKey(Schema.Natural),
}) {}

const SandboxEventBase = {
  eventVersion: Schema.Literal(1),
  implementation: SandboxImplementation,
};

/** Signals that the implementation has accepted the request and started its workload. */
export class SandboxStarted extends Schema.TaggedClass<SandboxStarted>()("SandboxStarted", {
  ...SandboxEventBase,
  runtime: SandboxRuntime,
}) {}

/** A bounded text fragment from stdout or stderr. Sandbox output is sensitive by default. */
export class SandboxOutput extends Schema.TaggedClass<SandboxOutput>()("SandboxOutput", {
  ...SandboxEventBase,
  stream: Schema.Literals(["stdout", "stderr"]),
  text: BoundedOutputText,
  bytes: Schema.Natural,
}) {}

/** The terminal process status and observed bounded resource usage. */
export class SandboxExited extends Schema.TaggedClass<SandboxExited>()("SandboxExited", {
  ...SandboxEventBase,
  exitCode: Schema.Int,
  resourceUse: SandboxResourceUse,
  artifacts: Schema.Array(SandboxArtifact),
}) {}

/** Versioned execution event stream. */
export const SandboxEvent = Schema.Union([SandboxStarted, SandboxOutput, SandboxExited]);
export type SandboxEvent = typeof SandboxEvent.Type;

/** The runtime could not start the requested process. */
export class SandboxSpawnError extends Schema.TaggedErrorClass<SandboxSpawnError>()(
  "SandboxSpawnError",
  {
    implementation: SandboxImplementation,
    command: BoundedPath,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The process ended unsuccessfully after starting. */
export class SandboxExitError extends Schema.TaggedErrorClass<SandboxExitError>()(
  "SandboxExitError",
  {
    implementation: SandboxImplementation,
    exitCode: Schema.Int,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The wall-clock execution limit elapsed and the owned process was interrupted. */
export class SandboxTimeoutError extends Schema.TaggedErrorClass<SandboxTimeoutError>()(
  "SandboxTimeoutError",
  {
    implementation: SandboxImplementation,
    maxWallTime: FinitePositiveDuration,
  },
) {}

/** A stdout or stderr stream exceeded its configured bounded-output limit. */
export class SandboxOutputLimitError extends Schema.TaggedErrorClass<SandboxOutputLimitError>()(
  "SandboxOutputLimitError",
  {
    implementation: SandboxImplementation,
    stream: Schema.Literals(["stdout", "stderr"]),
    limit: PositiveInt,
    observed: PositiveInt,
  },
) {}

/** The selected implementation cannot honestly enforce one requested security or resource feature. */
export class SandboxUnsupportedRequestError extends Schema.TaggedErrorClass<SandboxUnsupportedRequestError>()(
  "SandboxUnsupportedRequestError",
  {
    implementation: SandboxImplementation,
    feature: Schema.Literals([
      "runtime",
      "mounts",
      "network",
      "cpu-limit",
      "memory-limit",
      "secret-handles",
      "artifacts",
    ]),
    message: Schema.String,
  },
) {}

/** Expected sandbox execution failures. Interruption remains an Effect interruption, never a fabricated exit. */
export const SandboxError = Schema.Union([
  SandboxSpawnError,
  SandboxExitError,
  SandboxTimeoutError,
  SandboxOutputLimitError,
  SandboxUnsupportedRequestError,
]);
export type SandboxError = typeof SandboxError.Type;

/**
 * Scoped command/code execution capability. Implementations must identify their isolation posture
 * in every emitted event; an `unisolated` implementation is never a security sandbox.
 */
export class Sandbox extends Context.Service<
  Sandbox,
  {
    readonly execute: (request: SandboxRequest) => Stream.Stream<SandboxEvent, SandboxError>;
  }
>()("@effect-agent/sandbox/Sandbox") {}

/** Type helper for implementations that preserve the public Sandbox service contract. */
export type SandboxExecute = (
  request: SandboxRequest,
) => Stream.Stream<SandboxEvent, SandboxError, never>;
