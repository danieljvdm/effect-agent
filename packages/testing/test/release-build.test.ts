import { createHash } from "node:crypto";

import { NodeServices } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, Exit, FileSystem, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { checkReleasePackages } from "../../../scripts/check-release-packages.ts";
import {
  downloadReleaseBuild,
  ReleaseBuild,
  restoreReleaseBuild,
  writeReleaseBuild,
} from "../../../scripts/release-build.ts";
import { readCommand } from "../../../scripts/release-ci.ts";

layer(NodeServices.layer)((it) => {
  it.effect(
    "transfers exact package bytes through authenticated artifacts and rejects stale or corrupt evidence before installation",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "release-build-test-" });

        const git = (...args: ReadonlyArray<string>) =>
          readCommand(root, "git", args).pipe(Effect.map((text) => text.trim()));

        const write = Effect.fn(function* (path: string, content: string) {
          yield* fs.makeDirectory(`${root}/${path.slice(0, path.lastIndexOf("/"))}`, {
            recursive: true,
          });
          yield* fs.writeFileString(`${root}/${path}`, content);
        });

        yield* git("init", "--initial-branch=main");
        yield* git("config", "user.name", "Artifact test");
        yield* git("config", "user.email", "artifact@example.invalid");
        yield* git("config", "commit.gpgsign", "false");
        yield* git("config", "core.hooksPath", `${root}/.git/hooks`);
        yield* fs.writeFileString(`${root}/.gitignore`, "dist\n.release-build\n*.zip\n");
        yield* fs.writeFileString(`${root}/package.json`, JSON.stringify({ catalog: {} }));
        yield* write(
          "packages/effect-agent/package.json",
          JSON.stringify({
            name: "effect-agent",
            version: "0.1.0-beta.100",
            files: ["dist"],
            exports: { ".": "./src/index.ts" },
          }),
        );
        yield* write("packages/effect-agent/src/index.ts", "export const answer = 42;\n");
        yield* git("add", ".");
        yield* git("commit", "-m", "Source");
        const parent = yield* git("rev-parse", "HEAD");

        yield* git("commit", "--allow-empty", "-m", "Candidate");
        const commit = yield* git("rev-parse", "HEAD");

        const sourceManifest = yield* fs.readFileString(
          `${root}/packages/effect-agent/package.json`,
        );

        yield* write("packages/effect-agent/dist/index.mjs", "export const answer = 42;\n");
        yield* write(
          "packages/effect-agent/dist/index.d.mts",
          "export declare const answer = 42;\n",
        );
        yield* write("action/dist/index.mjs", "console.log('action');\n");
        const artifact = `${root}/.release-build/build.json`;

        yield* writeReleaseBuild(root, artifact, 42, 1);
        const original = yield* fs.readFileString(artifact);
        const build = yield* Schema.decodeEffect(Schema.fromJsonString(ReleaseBuild))(original);
        const expected = { runId: 42, runAttempt: 1, commit, parents: [parent] };

        // Authenticate the entire zip, not just per-file checksums supplied inside it.
        yield* readCommand(`${root}/.release-build`, "zip", [
          "-q",
          `${root}/artifact.zip`,
          "build.json",
        ]);
        const archive = yield* fs.readFile(`${root}/artifact.zip`);
        const digest = `sha256:${createHash("sha256").update(archive).digest("hex")}`;
        let scenario = "success";
        const requests: Array<string> = [];

        const client = HttpClient.make((request, url) => {
          requests.push(url.href);
          let response: Response;

          if (url.hostname === "signed-storage.example") {
            expect(request.headers.authorization).toBeUndefined();
            response = new Response(
              Uint8Array.from(scenario === "corrupt" ? archive.slice(1) : archive).buffer,
            );
          } else if (url.pathname.endsWith("/zip")) {
            expect(request.headers.authorization).toBe("Bearer read-only-test-token");
            response = new Response(null, {
              status: 302,
              headers: { location: "https://signed-storage.example/build" },
            });
          } else {
            expect(request.headers.authorization).toBe("Bearer read-only-test-token");
            response = Response.json({
              total_count: scenario === "missing" ? 0 : 1,
              artifacts:
                scenario === "missing"
                  ? []
                  : [
                      {
                        id: 77,
                        name: "release-build-42-1",
                        expired: scenario === "expired",
                        digest,
                        workflow_run: {
                          id: 42,
                          head_sha: scenario === "wrong-head" ? parent : commit,
                        },
                      },
                    ],
            });
          }

          return Effect.succeed(HttpClientResponse.fromWeb(request, response));
        });

        const download = downloadReleaseBuild(root, 42, 1, commit, "read-only-test-token").pipe(
          Effect.provideService(HttpClient.HttpClient, client),
        );

        yield* download;
        expect(yield* fs.readFileString(artifact)).toBe(original);
        for (scenario of ["corrupt", "missing", "expired", "wrong-head"]) {
          expect(Exit.isFailure(yield* Effect.exit(download))).toBe(true);
          expect(yield* fs.readFileString(artifact)).toBe(original);
        }
        expect(
          requests.some((url) =>
            url.includes("/actions/runs/42/artifacts?name=release-build-42-1"),
          ),
        ).toBe(true);

        yield* write("packages/effect-agent/dist/stale.mjs", "stale");
        yield* write("packages/effect-agent/dist/index.mjs", "wrong");
        yield* restoreReleaseBuild(root, artifact, expected);
        expect(yield* fs.readFileString(`${root}/packages/effect-agent/dist/index.mjs`)).toBe(
          "export const answer = 42;\n",
        );
        expect(yield* fs.exists(`${root}/packages/effect-agent/dist/stale.mjs`)).toBe(false);
        yield* checkReleasePackages(root);
        expect(yield* fs.readFileString(`${root}/packages/effect-agent/package.json`)).toBe(
          sourceManifest,
        );

        for (const altered of [
          { ...build, runId: 41 },
          { ...build, runAttempt: 2 },
          { ...build, tree: parent },
          { ...build, commit: parent },
          { ...build, parents: [] },
          { ...build, files: [] },
          { ...build, files: [...build.files, ...build.files] },
          { ...build, files: build.files.map((file) => ({ ...file, content: "Y29ycnVwdA==" })) },
          ...[
            "../escape",
            "packages/effect-agent/package.json",
            "packages/effect-agent/dist/../../src/index.ts",
          ].map((path) => ({ ...build, files: [{ ...build.files[0], path }] })),
        ]) {
          yield* fs.writeFileString(artifact, JSON.stringify(altered));
          expect(
            Exit.isFailure(yield* Effect.exit(restoreReleaseBuild(root, artifact, expected))),
          ).toBe(true);
          expect(yield* fs.readFileString(`${root}/packages/effect-agent/dist/index.mjs`)).toBe(
            "export const answer = 42;\n",
          );
        }
        yield* fs.writeFileString(artifact, original);
        yield* write("packages/effect-agent/src/index.ts", "export const answer = 43;\n");
        expect(
          Exit.isFailure(yield* Effect.exit(restoreReleaseBuild(root, artifact, expected))),
        ).toBe(true);
        yield* git("add", ".");
        yield* git("commit", "-m", "Changed source");
        expect(
          Exit.isFailure(yield* Effect.exit(restoreReleaseBuild(root, artifact, expected))),
        ).toBe(true);
      }),
    30_000,
  );
});
