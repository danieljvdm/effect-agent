import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";

import { expect, it } from "vite-plus/test";

// Run the bootstrap that chooses the actual checkout SHAs, before dependencies exist.
const workflow = readFileSync(
  new URL("../../../.github/workflows/performance.yml", import.meta.url),
  "utf8",
);

const script = workflow
  .split("          script: |\n")[1]!
  .split("\n      - ")[0]!
  .replace(/^ {12}/gm, "");

const main = "a".repeat(40);
const released = "b".repeat(40);

const release = (tag_name: string, published_at: string | null, draft = false) => ({
  id: 1,
  tag_name,
  published_at,
  draft,
  prerelease: true,
});

const resolve = async (
  releases: ReturnType<typeof release>[],
  options: { ref?: string; annotated?: boolean; badTag?: boolean } = {},
) => {
  const outputs: Record<string, string> = {};
  const refs: string[] = [];

  const execution: Promise<unknown> = runInNewContext(`(async () => { ${script} })()`, {
    context: {
      repo: { owner: "owner", repo: "repository" },
      ref: options.ref ?? "refs/heads/main",
      sha: main,
    },
    core: {
      setOutput: (key: string, value: string) => {
        outputs[key] = value;
      },
      info: () => {},
    },
    github: {
      paginate: async () => releases,
      rest: {
        repos: { listReleases: "releases" },
        git: {
          getRef: async ({ ref }: { ref: string }) => {
            refs.push(ref);

            return {
              data: {
                object: {
                  type: options.badTag ? "tree" : options.annotated ? "tag" : "commit",
                  sha: released,
                },
              },
            };
          },
          getTag: async () => ({ data: { object: { type: "commit", sha: released } } }),
        },
      },
    },
  });

  await execution;

  return { outputs, refs };
};

it.each([false, true])(
  "compares the newest published umbrella beta tag to main (annotated: %s)",
  async (annotated) => {
    const result = await resolve(
      [
        release("effect-agent@0.1.0-beta.77", "2026-09-11T00:00:00Z", true),
        release("action-v1", "2026-09-11T00:00:00Z"),
        release("@effect-agent/engine@0.1.0-beta.77", "2026-09-11T00:00:00Z"),
        release("effect-agent@0.1.0-beta.75", "2026-09-09T00:00:00Z"),
        release("effect-agent@0.1.0-beta.76", "2026-09-10T00:00:00Z"),
        release("effect-agent@0.1.0-beta.78", null),
      ],
      { annotated },
    );

    expect(result).toEqual({
      outputs: { base_tag: "effect-agent@0.1.0-beta.76", base_sha: released, head_sha: main },
      refs: ["tags/effect-agent@0.1.0-beta.76"],
    });
  },
);

it("fails clearly when no published release or usable commit exists", async () => {
  await expect(resolve([])).rejects.toThrow("No published effect-agent release found");
  const releases = [release("effect-agent@0.1.0-beta.76", "2026-09-10T00:00:00Z")];

  await expect(resolve(releases, { badTag: true })).rejects.toThrow("Invalid comparison commit");
  await expect(resolve(releases, { ref: "refs/heads/changeset-release/main" })).rejects.toThrow(
    "Run this comparison on main",
  );
});
