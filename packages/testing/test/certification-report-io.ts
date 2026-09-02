import { CertificationReport } from "@effect-agent/thread/testing";
import { NodeFileSystem } from "@effect/platform-node";
import { Effect, FileSystem, Schema } from "effect";

/**
 * Shared by the Node certification runners: write the Schema-encoded certificate when
 * regeneration is requested through `EFFECT_AGENT_CERTIFICATION_OUT=<directory>`.
 * A no-op otherwise, so ordinary test runs never touch the working tree.
 */
export const maybeWriteReport = (slug: string, report: CertificationReport) =>
  Effect.gen(function* () {
    const out = process.env["EFFECT_AGENT_CERTIFICATION_OUT"];

    if (out === undefined || out === "") return;
    const encoded = yield* Schema.encodeEffect(CertificationReport)(report).pipe(Effect.orDie);
    const fs = yield* FileSystem.FileSystem;

    yield* fs.makeDirectory(out, { recursive: true }).pipe(Effect.orDie);
    yield* fs
      .writeFileString(`${out}/${slug}.json`, `${JSON.stringify(encoded, null, 2)}\n`)
      .pipe(Effect.orDie);
  }).pipe(Effect.provide(NodeFileSystem.layer));
