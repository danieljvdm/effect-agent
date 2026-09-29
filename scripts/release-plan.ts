import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Config, Console, Effect, Exit, FileSystem } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { proveGatedRelease } from "./release-ci.ts";
import { hasUnpublishedRelease, readWorkspacePackages } from "./release-publish.ts";

// Registry failures fail the plan. A missing gate proof only selects the gates again.
const program = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* Config.String("GITHUB_WORKSPACE");
  const packages = yield* readWorkspacePackages(root);
  const publish = yield* hasUnpublishedRelease(packages.map((pkg) => pkg.manifest));

  const proof = publish
    ? yield* proveGatedRelease(
        root,
        yield* Config.String("GITHUB_SHA"),
        yield* Config.String("GITHUB_TOKEN"),
      ).pipe(Effect.timeout("45 seconds"), Effect.exit)
    : Exit.void;

  const gated = publish && Exit.isSuccess(proof);

  const summary = !publish
    ? "All public versions are already published; nothing to release.\n"
    : gated
      ? "The release tree passed the live gates on its version PR; publishing without repeating them.\n"
      : `The release tree has no reusable gate evidence (${Exit.isFailure(proof) ? Cause.pretty(proof.cause).split("\n")[0] : "unknown"}); running the live gates.\n`;

  yield* Console.log(summary.trim());
  yield* fs.writeFileString(
    yield* Config.String("GITHUB_OUTPUT"),
    `publish=${publish}\ngated=${gated}\n`,
    { flag: "a" },
  );
  yield* fs.writeFileString(yield* Config.String("GITHUB_STEP_SUMMARY"), summary, { flag: "a" });
});

if (import.meta.main)
  NodeRuntime.runMain(program.pipe(Effect.provide([NodeServices.layer, FetchHttpClient.layer])));
