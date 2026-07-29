import { expect, layer } from "@effect/vitest";

import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

const Dependencies = Schema.Record(Schema.String, Schema.String);
const PackageManifest = Schema.Struct({
  name: Schema.optional(Schema.String),
  version: Schema.optional(Schema.String),
  workspaces: Schema.optional(Schema.Array(Schema.String)),
  catalog: Schema.optional(Dependencies),
  dependencies: Schema.optional(Dependencies),
  devDependencies: Schema.optional(Dependencies),
  optionalDependencies: Schema.optional(Dependencies),
  overrides: Schema.optional(Dependencies),
  peerDependencies: Schema.optional(Dependencies),
});
type PackageManifest = typeof PackageManifest.Type;

// Vite+ runs this package test from packages/testing; Bun is the pinned test runtime in CI and locally.
const repositoryRoot = "../..";
const packageNames = ["core", "engine", "testing"] as const;
const exampleNames = ["demo"] as const;
const effectTestPackageNames = ["engine", "testing"] as const;
const productionPackageNames = ["core", "engine"] as const;
const dependencySections = [
  "dependencies",
  "devDependencies",
  "optionalDependencies",
  "peerDependencies",
] as const;
const forbiddenScaffoldDependencies = new Set([
  "@cloudflare/workers-types",
  "@types/react",
  "@types/react-dom",
  "@vitejs/plugin-react",
  "react",
  "react-dom",
  "wrangler",
]);

const readManifest = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const contents = yield* fs.readFileString(path);
    return yield* Schema.decodeEffect(Schema.fromJsonString(PackageManifest))(contents);
  });

const readDirectory = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readDirectory(path);
  });

const exists = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.exists(path);
  });

const manifestDependencies = (manifest: PackageManifest): ReadonlyArray<string> =>
  dependencySections.flatMap((section) => Object.keys(manifest[section] ?? {}));

const effectDependencies = (manifest: PackageManifest): ReadonlyArray<string> =>
  dependencySections
    .map((section) => manifest[section]?.effect)
    .filter((version): version is string => version !== undefined);

layer(NodeServices.layer)("Phase 0 toolchain", (it) => {
  it.effect("executes an Effect program through the Vite+ test runner", () =>
    Effect.sync(() => {
      expect("ready").toBe("ready");
    }),
  );

  it.effect("keeps framework packages separate from leaf example workspaces", () =>
    Effect.gen(function* () {
      const [activePackages, activeExamples, rootEntries, rootManifest] = yield* Effect.all([
        readDirectory(`${repositoryRoot}/packages`),
        readDirectory(`${repositoryRoot}/examples`),
        readDirectory(repositoryRoot),
        readManifest(`${repositoryRoot}/package.json`),
      ]);

      expect([...activePackages].sort()).toEqual([...packageNames].sort());
      expect([...activeExamples].sort()).toEqual([...exampleNames].sort());
      expect(rootManifest.workspaces).toEqual(["packages/*", "examples/*"]);
      expect(rootEntries).not.toContain("apps");
      expect(rootEntries).not.toContain("wrangler.toml");
      expect(rootEntries).not.toContain("wrangler.json");
      expect(rootEntries).not.toContain("wrangler.jsonc");
    }),
  );

  it.effect("keeps browser and deployment scaffolds out of framework manifests", () =>
    Effect.gen(function* () {
      const manifests = [yield* readManifest(`${repositoryRoot}/package.json`)];

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        manifests.push(manifest);
      }

      for (const manifest of manifests) {
        const dependencies = manifestDependencies(manifest);

        expect(
          dependencies.filter((dependency) => forbiddenScaffoldDependencies.has(dependency)),
        ).toEqual([]);
        expect(dependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
          false,
        );
      }
    }),
  );

  it.effect("keeps the demo as an inward-consuming, provider-free leaf workspace", () =>
    Effect.gen(function* () {
      const demo = yield* readManifest(`${repositoryRoot}/examples/demo/package.json`);
      const dependencies = manifestDependencies(demo);

      expect(demo.name).toBe("@effect-agent/example-demo");
      expect(demo.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/engine"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect/atom-react"]).toBe("catalog:");
      expect(demo.dependencies?.["@tanstack/react-start"]).toBe("catalog:");
      expect(demo.dependencies?.["@base-ui/react"]).toBe("catalog:");
      expect(demo.dependencies?.react).toBe("catalog:");
      expect(demo.dependencies?.effect).toBe("catalog:");
      expect(dependencies).not.toContain("wrangler");
      expect(dependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(false);

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        expect(manifestDependencies(manifest)).not.toContain(demo.name);
      }
    }),
  );

  it.effect(
    "pins Effect test helpers and the single Vite+ Vitest runtime through the root catalog",
    () =>
      Effect.gen(function* () {
        const rootManifest = yield* readManifest(`${repositoryRoot}/package.json`);
        const vitePlusManifest = yield* readManifest(
          `${repositoryRoot}/node_modules/vite-plus/package.json`,
        );
        const effectVersion = rootManifest.catalog?.effect;

        expect(effectVersion).toMatch(/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/);
        expect(rootManifest.catalog?.["@effect/platform-node"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/vitest"]).toBe(effectVersion);
        expect(rootManifest.catalog?.vitest).toBe(vitePlusManifest.dependencies?.vitest);
        expect(rootManifest.overrides?.vitest).toBe("catalog:");
        expect(rootManifest.devDependencies?.effect).toBe("catalog:");
        expect(manifestDependencies(rootManifest)).not.toContain("vitest");

        for (const packageName of packageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          expect(effectDependencies(manifest)).toEqual(["catalog:"]);
          expect(manifestDependencies(manifest)).not.toContain("vitest");
        }

        for (const packageName of effectTestPackageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          expect(manifest.devDependencies?.["@effect/vitest"]).toBe("catalog:");
        }

        for (const packageName of productionPackageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          expect(manifestDependencies(manifest)).not.toContain("@effect-agent/testing");
        }
      }),
  );

  it.effect("matches an initialized local Effect checkout to the catalog pin", () =>
    Effect.gen(function* () {
      const rootManifest = yield* readManifest(`${repositoryRoot}/package.json`);
      const effectVersion = rootManifest.catalog?.effect;
      const effectSource = `${repositoryRoot}/repos/effect`;
      const effectPackagePath = `${effectSource}/packages/effect/package.json`;

      // CI deliberately leaves source submodules uninitialized; installed packages remain authoritative there.
      if (!(yield* exists(effectPackagePath))) {
        return;
      }

      const effectPackage = yield* readManifest(effectPackagePath);
      const tagsAtHead = yield* Effect.gen(function* () {
        const child = yield* ChildProcess.make(
          "git",
          ["-C", effectSource, "tag", "--points-at", "HEAD"],
          { stderr: "pipe", stdout: "pipe" },
        );
        const [output, exitCode] = yield* Effect.all([
          Stream.mkString(Stream.decodeText(child.all)),
          child.exitCode,
        ]);

        expect(exitCode).toBe(0);
        return output.split("\n").filter(Boolean);
      });

      expect(effectPackage.name).toBe("effect");
      expect(effectPackage.version).toBe(effectVersion);
      expect(tagsAtHead).toContain(`effect@${effectVersion}`);
    }),
  );
});
