import { expect, layer } from "@effect/vitest";

import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

const Dependencies = Schema.Record(Schema.String, Schema.String);
const PackageManifest = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  version: Schema.optionalKey(Schema.String),
  workspaces: Schema.optionalKey(Schema.Array(Schema.String)),
  catalog: Schema.optionalKey(Dependencies),
  dependencies: Schema.optionalKey(Dependencies),
  devDependencies: Schema.optionalKey(Dependencies),
  optionalDependencies: Schema.optionalKey(Dependencies),
  overrides: Schema.optionalKey(Dependencies),
  peerDependencies: Schema.optionalKey(Dependencies),
});
type PackageManifest = typeof PackageManifest.Type;

// Vite+ runs this package test from packages/testing; Bun is the pinned test runtime in CI and locally.
const repositoryRoot = "../..";
const packageNames = [
  "capabilities",
  "core",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
] as const;
const exampleNames = ["demo", "providers", "repo-ops"] as const;
const effectTestPackageNames = [
  "capabilities",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
] as const;
const productionPackageNames = [
  "capabilities",
  "core",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
] as const;
// Phase 6: only these two packages may carry Cloudflare dependencies, and only
// the types-only package plus the in-workerd test harness — wrangler and
// application scaffolds stay banned everywhere.
const cloudflarePackageNames: ReadonlyArray<string> = ["platform-cloudflare", "storage-cloudflare"];
const allowedCloudflareToolchainDependencies = new Set([
  "@cloudflare/vitest-pool-workers",
  "@cloudflare/workers-types",
]);
// Phase 6 exit gate "Agent/core/engine packages import no Cloudflare platform
// types", audited at the manifest layer: only the two Cloudflare packages may
// depend on @cloudflare/* or the Durable Object SqlClient, in ANY dependency
// section. Everything inward of them must stay platform-clean so the semantic
// coordinator never gains a conditional platform branch (deployment spec §3.1).
const inwardPackageNames = [
  "capabilities",
  "core",
  "engine",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-memory",
  "storage-sqlite",
] as const;
const cloudflareOnlyDependencies = new Set(["@effect/sql-sqlite-do"]);
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
const providerAdapterDependencies = ["@effect/ai-openai", "@effect/ai-anthropic"] as const;

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

layer(NodeServices.layer)("workspace toolchain", (it) => {
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
      const rootDependencies = manifestDependencies(
        yield* readManifest(`${repositoryRoot}/package.json`),
      );
      expect(
        rootDependencies.filter((dependency) => forbiddenScaffoldDependencies.has(dependency)),
      ).toEqual([]);
      expect(rootDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        const dependencies = manifestDependencies(manifest);
        const cloudflareAllowance = cloudflarePackageNames.includes(packageName)
          ? allowedCloudflareToolchainDependencies
          : new Set<string>();

        expect(
          dependencies.filter(
            (dependency) =>
              forbiddenScaffoldDependencies.has(dependency) && !cloudflareAllowance.has(dependency),
          ),
        ).toEqual([]);
        expect(
          dependencies.filter(
            (dependency) =>
              dependency.startsWith("@cloudflare/") && !cloudflareAllowance.has(dependency),
          ),
        ).toEqual([]);
      }
    }),
  );

  it.effect("keeps Cloudflare platform dependencies out of inward framework manifests", () =>
    Effect.gen(function* () {
      // The P6 gate at the manifest layer: core/engine/capabilities/session and
      // the non-Cloudflare storage adapters carry no @cloudflare/* dependency and
      // no @effect/sql-sqlite-do in any dependency section — the Durable Object
      // SqlClient and the Cloudflare types stay confined to storage-cloudflare
      // and platform-cloudflare (import-level: storage-cloudflare never imports
      // the cloudflare:workers runtime module; platform-cloudflare alone does).
      for (const packageName of inwardPackageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        const dependencies = manifestDependencies(manifest);
        expect(
          dependencies.filter(
            (dependency) =>
              dependency.startsWith("@cloudflare/") || cloudflareOnlyDependencies.has(dependency),
          ),
        ).toEqual([]);
      }

      // The confinement side: every workspace consumer of the Durable Object
      // SqlClient is one of the two Cloudflare packages, catalog-pinned.
      for (const packageName of cloudflarePackageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        expect(manifest.dependencies?.["@effect/sql-sqlite-do"]).toBe("catalog:");
      }
    }),
  );

  it.effect("keeps provider adapters catalog-pinned and confined to leaf examples", () =>
    Effect.gen(function* () {
      const root = yield* readManifest(`${repositoryRoot}/package.json`);
      const demo = yield* readManifest(`${repositoryRoot}/examples/demo/package.json`);
      const providers = yield* readManifest(`${repositoryRoot}/examples/providers/package.json`);
      const repoOps = yield* readManifest(`${repositoryRoot}/examples/repo-ops/package.json`);
      const demoDependencies = manifestDependencies(demo);
      const providerDependencies = manifestDependencies(providers);
      const repoOpsDependencies = manifestDependencies(repoOps);

      expect(demo.name).toBe("@effect-agent/example-demo");
      expect(demo.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/engine"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(demo.dependencies?.["@effect/atom-react"]).toBe("catalog:");
      expect(demo.dependencies?.["@tanstack/react-start"]).toBe("catalog:");
      expect(demo.dependencies?.["@base-ui/react"]).toBe("catalog:");
      expect(demo.dependencies?.react).toBe("catalog:");
      expect(demo.dependencies?.effect).toBe("catalog:");
      expect(root.catalog?.["@effect/ai-openai"]).toBe(root.catalog?.effect);
      expect(root.catalog?.["@effect/ai-anthropic"]).toBe(root.catalog?.effect);
      expect(demoDependencies).not.toContain("wrangler");
      expect(demoDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      expect(providers.name).toBe("@effect-agent/example-providers");
      expect(providers.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(providers.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(providers.dependencies?.effect).toBe("catalog:");
      expect(providers.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(providers.dependencies?.["@effect/ai-anthropic"]).toBe("catalog:");
      expect(providerDependencies).not.toContain("wrangler");
      expect(providerDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      // P7: the repo-ops evidence auditor is the third leaf example workspace.
      expect(repoOps.name).toBe("@effect-agent/example-repo-ops");
      expect(repoOps.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(repoOps.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(repoOps.dependencies?.effect).toBe("catalog:");
      expect(repoOps.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(repoOpsDependencies).not.toContain("wrangler");
      expect(repoOpsDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        expect(manifestDependencies(manifest)).not.toContain(demo.name);
        expect(manifestDependencies(manifest)).not.toContain(providers.name);
        expect(manifestDependencies(manifest)).not.toContain(repoOps.name);
        for (const adapter of providerAdapterDependencies) {
          expect(manifestDependencies(manifest)).not.toContain(adapter);
        }
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
        expect(rootManifest.catalog?.["@effect/platform-browser"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/platform-node"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/sql-sqlite-do"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/sql-sqlite-node"]).toBe(effectVersion);
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
          if (cloudflarePackageNames.includes(packageName)) {
            // P6 WP0 probe outcome (D-P6-7): `vp test` cannot drive the
            // workers pool runner, so the Cloudflare packages run `vitest run`
            // directly against the same catalog-pinned Vitest instance.
            expect(manifest.devDependencies?.vitest).toBe("catalog:");
          } else {
            expect(manifestDependencies(manifest)).not.toContain("vitest");
          }
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
          // No production package ever SHIPS depending on the testing package.
          expect(Object.keys(manifest.dependencies ?? {})).not.toContain("@effect-agent/testing");
          // Exactly two packages may consume it as a devDependency: platform-cloudflare's
          // DC Travel Planner equivalence suite must assemble the SAME fixtures the DN
          // suite runs (P6 plan §6), and storage-cloudflare's in-workerd certification
          // runner executes `certifyDurableAdapters` against the real Durable Object
          // adapters (P7 WP2; the memory/SQLite runners live inside packages/testing
          // because vp's task graph rejects the storage-* → testing dev-edge cycle).
          // Both edges are dev-only and test-only; every other production package stays
          // clean in every dependency section.
          if (packageName !== "platform-cloudflare" && packageName !== "storage-cloudflare") {
            expect(manifestDependencies(manifest)).not.toContain("@effect-agent/testing");
          }
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
