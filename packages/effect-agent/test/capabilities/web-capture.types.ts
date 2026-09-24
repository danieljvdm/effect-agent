import type { Layer } from "effect";
import { Context, Effect, Schema, SchemaGetter } from "effect";
import type { PageCapture } from "effect-agent/page-capture";
import * as WebCapture from "effect-agent/web-capture";
import type { Tool } from "effect/unstable/ai";

const PricingSchema = Schema.Struct({
  plans: Schema.Array(Schema.Struct({ name: Schema.String, monthlyUsd: Schema.Number })),
});

class ExtractionDecoderService extends Context.Service<
  ExtractionDecoderService,
  { readonly normalize: (value: string) => string }
>()("@effect-agent/capabilities/test/ExtractionDecoderService") {}

const ServicePricingSchema = Schema.Struct({
  plans: Schema.Array(
    Schema.Struct({
      name: Schema.String.pipe(
        Schema.decode({
          decode: SchemaGetter.transformEffect((value) =>
            Effect.map(ExtractionDecoderService, (service) => service.normalize(value)),
          ),
          encode: SchemaGetter.transform((value) => value),
        }),
      ),
      monthlyUsd: Schema.Number,
    }),
  ),
});

type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends <T>() => T extends Right ? 1 : 2 ? true : false;

type LayerContext<L> = L extends Layer.Layer<infer _Out, infer _Error, infer R> ? R : never;

const typedExtract = WebCapture.makeExtract("typed_extract", {
  description: "typed",
  urls: ["docs.example.com"],
  schema: PricingSchema,
});

type ExtractSuccessIsDecoded = Equal<
  Tool.Success<typeof typedExtract.tool>,
  typeof PricingSchema.Type
>;

const typedServiceExtract = WebCapture.makeExtract("typed_service_extract", {
  description: "typed",
  urls: ["docs.example.com"],
  schema: ServicePricingSchema,
});

type ServiceExtractLayerRequirements = LayerContext<typeof typedServiceExtract.handlers>;
type ServiceExtractKeepsDecoderRequirement = Equal<
  ServiceExtractLayerRequirements,
  PageCapture | ExtractionDecoderService
>;
type ServiceExtractToolKeepsDecoderRequirement = Equal<
  Tool.HandlerServices<typeof typedServiceExtract.tool>,
  ExtractionDecoderService
>;

export const verifyExtractionDecoderAndSuccessInference = () => {
  const extractSuccessProof: ExtractSuccessIsDecoded = true;
  const decoderRequirementProof: ServiceExtractKeepsDecoderRequirement = true;
  const decoderInvocationProof: ServiceExtractToolKeepsDecoderRequirement = true;

  void (extractSuccessProof && decoderRequirementProof && decoderInvocationProof);
};
