import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import {
  type MemoryReader,
  type MemoryWriter,
  MemoryKey,
  type MemoryStorageError,
  type MemoryOperationConflict,
  type MemoryMutationFailure,
} from "@effect-agent/core/MemoryStore";
import * as Protocol from "@effect-agent/core/RememberingStore";
import type { Effect } from "effect";
import { Context, Schema } from "effect";
import type { AiError, LanguageModel } from "effect/unstable/ai";
import { expect, it } from "vite-plus/test";

import * as Remembering from "../src/Remembering.ts";

const Tenant = Schema.String.pipe(Schema.brand("remembering/Tenant"));
const identity = Schema.Struct({ tenant: Tenant });
const Messages = MemoryNamespace.define({ name: "app/messages", version: 1, identity });
const Profiles = MemoryNamespace.define({ name: "app/profiles", version: 1, identity });
const ProfilesV2 = MemoryNamespace.define({ name: "app/profiles", version: 2, identity });
const messages = Messages.make({ tenant: Tenant.make("tenant") });
const profiles = Profiles.make({ tenant: Tenant.make("tenant") });

type Intent = Protocol.Intent<typeof messages, typeof profiles>;

const intent = Protocol.Intent.make({
  version: 1,
  id: "delivery/processor-v1",
  invocationId: "trusted-invocation",
  source: {
    key: MemoryKey.make({ namespace: messages, id: "message" }),
    locator: "message://message",
    revision: "opaque",
    position: { authorityGeneration: "authority", sequence: 1 },
  },
  target: MemoryKey.make({ namespace: profiles, id: "person" }),
});

class Caller extends Context.Service<Caller, {}>()("test/Caller") {}
class Source extends Context.Service<Source, {}>()("test/Source") {}
class Merge extends Context.Service<Merge, {}>()("test/Merge") {}
class Cleanup extends Context.Service<Cleanup, {}>()("test/Cleanup") {}
class Storage extends Context.Service<Storage, {}>()("test/Storage") {}
class Decode extends Context.Service<Decode, {}>()("test/Decode") {}
class Encode extends Context.Service<Encode, {}>()("test/Encode") {}
class SourceError extends Schema.TaggedError<SourceError>()("SourceError", {}) {}
class MergeError extends Schema.TaggedError<MergeError>()("MergeError", {}) {}
class CleanupError extends Schema.TaggedError<CleanupError>()("CleanupError", {}) {}
class StorageError extends Schema.TaggedError<StorageError>()("StorageError", {}) {}

type Value = { readonly text: string };
declare const codec: Schema.Codec<Value, Value, Decode, Encode>;
declare const store: Protocol.Store<StorageError, Storage>;
declare const limits: Remembering.Limits;
declare const loadSource: (
  intent: Intent,
) => Effect.Effect<Remembering.SourceSnapshot | Protocol.Invalidation | null, SourceError, Source>;
declare const extract: (
  snapshot: Remembering.SourceSnapshot,
  intent: Intent,
) => Effect.Effect<
  Protocol.Extracted<Value> | null,
  AiError.AiError,
  LanguageModel.LanguageModel | Caller
>;
declare const merge: (
  input: Remembering.MergeInput<Value, typeof messages, typeof profiles>,
) => Effect.Effect<Remembering.Decision, MergeError, Merge>;
declare const cleanup: (
  input: Remembering.CleanupInput<Value, typeof messages, typeof profiles>,
) => Effect.Effect<Remembering.Decision, CleanupError, Cleanup>;

const compose = () =>
  Remembering.make({ proposal: codec, loadSource, extract, merge, cleanup }).advance({
    intent,
    store,
    limits,
    extractionEnabled: true,
  });

const admission = () => Remembering.admit(store, intent);
const invalidation = (event: Protocol.Invalidation) => Remembering.invalidate(store, event);

type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;
type Assert<A extends true> = A;
type Result = Effect.Success<ReturnType<typeof compose>>;
type Prepared = Extract<Result["progress"], { readonly _tag: "Prepared" }>;
type Proofs = [
  Assert<Equal<typeof intent.source.key.namespace, typeof messages>>,
  Assert<Equal<typeof intent.target.namespace, typeof profiles>>,
  Assert<
    Equal<
      Effect.Success<ReturnType<typeof compose>>["intent"]["source"]["key"]["namespace"],
      typeof messages
    >
  >,
  Assert<
    Equal<
      Effect.Success<ReturnType<typeof compose>>["intent"]["target"]["namespace"],
      typeof profiles
    >
  >,
  Assert<
    Equal<
      Effect.Services<ReturnType<typeof compose>>,
      | Caller
      | Source
      | Merge
      | Cleanup
      | Storage
      | Decode
      | Encode
      | LanguageModel.LanguageModel
      | MemoryReader
      | MemoryWriter
      | Protocol.MutationFailpoint
    >
  >,
  Assert<
    Equal<
      Effect.Error<ReturnType<typeof compose>>,
      | SourceError
      | MergeError
      | CleanupError
      | StorageError
      | AiError.AiError
      | Schema.SchemaError
      | Protocol.AdmissionError
      | Protocol.CheckpointError
      | Protocol.ProcessingError
      | Protocol.MutationFailure
      | MemoryStorageError
      | MemoryOperationConflict
      | MemoryMutationFailure
    >
  >,
  Assert<
    Equal<Effect.Services<ReturnType<typeof admission>>, Storage | Protocol.MutationFailpoint>
  >,
  Assert<
    Equal<
      Effect.Error<ReturnType<typeof admission>>,
      StorageError | Protocol.AdmissionError | Protocol.MutationFailure
    >
  >,
  Assert<
    Equal<Effect.Services<ReturnType<typeof invalidation>>, Storage | Protocol.MutationFailpoint>
  >,
  Assert<Equal<Prepared["command"]["key"]["namespace"], typeof profiles>>,
  Assert<Equal<NonNullable<Prepared["applied"]>["key"]["namespace"], typeof profiles>>,
  Assert<Equal<NonNullable<Result["suppression"]>["source"]["namespace"], typeof messages>>,
];

const negative = () => {
  const accept = (_intent: Intent) => undefined;

  accept(
    // @ts-expect-error Same identity does not make source and destination namespaces interchangeable.
    Protocol.Intent.make({
      ...intent,
      target: MemoryKey.make({ namespace: messages, id: "person" }),
    }),
  );
  accept(
    // @ts-expect-error A new namespace version is a different application contract.
    Protocol.Intent.make({
      ...intent,
      target: MemoryKey.make({
        namespace: ProfilesV2.make({ tenant: Tenant.make("tenant") }),
        id: "person",
      }),
    }),
  );
  // @ts-expect-error Unbranded tenant strings are not host-bound typed identity.
  Profiles.make({ tenant: "tenant" });
};

it("retains schema/model/caller/store errors and requirements and both namespace identities", () => {
  const proofs: Proofs = [true, true, true, true, true, true, true, true, true, true, true, true];

  expect(proofs.every(Boolean)).toBe(true);
  expect(typeof negative).toBe("function");
  expect(
    Protocol.comparePosition(
      { authorityGeneration: "a", sequence: 9 },
      { authorityGeneration: "b", sequence: 1 },
    ),
  ).toBeUndefined();
  expect(
    Protocol.comparePosition(
      { authorityGeneration: "a", sequence: 9 },
      { authorityGeneration: "a", sequence: 1 },
    ),
  ).toBe(1);
});
