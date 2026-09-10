import { JSONParser } from "@streamparser/json";
import { Schema } from "effect";

/** Disposable preview only. The native tool boundary validates the complete response. */
export const responseTextPreview = () => {
  const parser = new JSONParser({
    paths: ["$.message"],
    keepStack: false,
    emitPartialTokens: true,
    emitPartialValues: true,
  });

  let text = "";
  let emitted = 0;
  let bytes = 0;
  let stopped = false;

  parser.onValue = ({ value }) => {
    const decoded = Schema.decodeUnknownOption(Schema.String)(value);

    if (decoded._tag === "Some") text = decoded.value.slice(0, 4000);
  };
  parser.onError = () => {
    stopped = true;
  };

  return (chunk: string): string => {
    bytes += new TextEncoder().encode(chunk).byteLength;
    if (stopped || bytes > 24 * 1024) return "";
    parser.write(chunk);
    const delta = text.slice(emitted);

    emitted = text.length;

    return delta;
  };
};
