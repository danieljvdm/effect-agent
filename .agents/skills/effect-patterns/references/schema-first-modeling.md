# Effect Schema-First Modeling

Use this when adding or changing domain data, identifiers, service method
inputs or results, expected errors, persisted records, or transport DTOs.

## Schema Is The Source

Define application data with Effect Schema first. Export the schema value and
derive its decoded TypeScript type from `.Type`.

```ts
import { Schema } from "effect";

export const ArtifactId = Schema.NonEmptyString.pipe(
  Schema.brand("@acme/ArtifactId"),
);
export type ArtifactId = typeof ArtifactId.Type;

export const GenerateImageInput = Schema.Struct({
  productId: ArtifactId,
  prompt: Schema.String,
  referenceIds: Schema.Array(ArtifactId),
});
export type GenerateImageInput = typeof GenerateImageInput.Type;

export const GenerateImageResult = Schema.Struct({
  imageIds: Schema.Array(ArtifactId),
});
export type GenerateImageResult = typeof GenerateImageResult.Type;
```

Keep the schema and derived type under the same exported name. The value owns
the fields, validation, and encoded form; the type follows it without a
parallel interface or handwritten structural alias.

Use:

- `Schema.Struct` for ordinary record-shaped domain and service contracts.
- `Schema.Class` when validated construction, methods, or class identity are
  useful.
- `Schema.TaggedStruct` or `Schema.TaggedClass` for discriminated data unions.
- `Schema.TaggedErrorClass` for expected domain and service failures.
- `typeof Model.Type` for the decoded application type and
  `typeof Model.Encoded` only when code explicitly handles the encoded form.

Interfaces remain appropriate for runtime capabilities that contain functions,
resources, or other behavior rather than serializable application data.

## Branded Identifiers

Give each semantically distinct identifier its own branded schema. Reuse that
schema for every field and method parameter carrying the identifier.

```ts
export const ProductId = Schema.NonEmptyString.pipe(
  Schema.brand("@acme/ProductId"),
);
export type ProductId = typeof ProductId.Type;

export const ReferenceId = Schema.NonEmptyString.pipe(
  Schema.brand("@acme/ReferenceId"),
);
export type ReferenceId = typeof ReferenceId.Type;
```

Place runtime checks for identifier syntax on the underlying schema before
branding it. `Schema.brand` provides nominal distinction; the underlying
schema provides validation.

During a change, inventory every added or modified `id`, `*Id`, and `*Ids`
field. Each one should resolve to its semantic branded schema rather than a
plain primitive.

## Service Contracts

Export schemas for every data-bearing service input and result, including
intermediate results that do not yet cross a network boundary.

```ts
export const GenerateInput = Schema.Struct({
  productId: ProductId,
  prompt: Schema.String,
});
export type GenerateInput = typeof GenerateInput.Type;

export const GenerateResult = Schema.Struct({
  artifactId: ArtifactId,
});
export type GenerateResult = typeof GenerateResult.Type;

export class Generator extends Context.Service<
  Generator,
  {
    readonly generate: (
      input: GenerateInput,
    ) => Effect.Effect<GenerateResult, GenerateError>;
  }
>()("@acme/Generator") {}
```

Service interfaces refer to schema-derived types. Schema values stay available
for decoding, encoding, persistence, fixtures, and later transport contracts
without redefining the model.

## Boundaries

Decode `unknown` input with the owning schema at the system boundary. Pass the
decoded type through internal services and encode with the same schema when
writing to an external representation.

```ts
const decodeGenerateInput =
  Schema.decodeUnknownEffect(GenerateInput);

const encodeGenerateResult =
  Schema.encodeEffect(GenerateResult);
```

Keep refinements, optionality, defaults, transformations, and collection
members in the schema so runtime behavior and TypeScript inference evolve
together.

## Completion Check

Before completing a change, inspect every added or modified domain model,
identifier, service input, service result, expected error, persisted record,
and DTO:

- Every data contract has an exported Effect Schema value.
- Every corresponding TypeScript type is inferred from `.Type` or supplied by
  a schema class.
- Every identifier field uses the correct branded identifier schema.
- Every external `unknown` input is decoded at its owning boundary.
- No interface or structural type duplicates fields already owned by a schema.
