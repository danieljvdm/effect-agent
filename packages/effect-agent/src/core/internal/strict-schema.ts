import { Schema, SchemaParser, SchemaTransformation } from "effect";

/** Keep strict parsing on nested values even when an outer parser uses its defaults. */
export const strictSchema = <S extends Schema.Top>(schema: S) =>
  Schema.declareConstructor<S["Type"], S["Encoded"]>()(
    [schema],
    ([codec]) =>
      (value, _ast, options) =>
        SchemaParser.decodeUnknownEffect(codec)(value, { ...options, onExcessProperty: "error" }),
    {
      toCodecJson: ([codec]) =>
        Schema.link<S["Encoded"]>()(codec, SchemaTransformation.passthrough()),
    },
  );
