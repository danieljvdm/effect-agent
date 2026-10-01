import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";

import { selectAll } from "@astrojs/starlight/expressive-code/hast";
import ecTwoSlash from "expressive-code-twoslash";
import ts from "typescript-twoslash";

const productionBuild = process.argv.includes("build");
const snippetsRoot = resolve(import.meta.dirname, "../snippets/travel-planner");

const sourcePlugin: ReturnType<typeof ecTwoSlash> = {
  name: "effect-agent-snippets",
  hooks: {
    preprocessCode({ codeBlock }) {
      const source = /\bsrc="(snippets\/[^"]+)"/.exec(codeBlock.meta)?.[1];

      if (source !== undefined) {
        const [filename, region] = source.split("#");
        const contents = readFileSync(resolve(import.meta.dirname, "..", filename ?? ""), "utf8");

        const code =
          region === undefined
            ? contents
            : contents.split(`// #region ${region}\n`)[1]?.split("// #endregion")[0];

        if (code === undefined) throw new Error(`Missing snippet region: ${source}`);

        if (codeBlock.getLines().length > 0) {
          codeBlock.deleteLines(codeBlock.getLines().map((_, index) => index));
        }
        codeBlock.insertLines(0, code.trimEnd().split("\n"));
      }
    },
    postprocessRenderedBlock({ renderData }) {
      // Type hovers remain available to readers without polluting search snippets.
      for (const popup of selectAll(".twoslash-popup-container", renderData.blockAst)) {
        popup.properties["data-pagefind-ignore"] = "";
      }
    },
  },
};

export const snippetPlugins = [
  sourcePlugin,
  ecTwoSlash({
    cwd: snippetsRoot,
    tsConfigPath: resolve(import.meta.dirname, "../tsconfig.snippets.json"),
    instanceConfigs: { twoslash: { explicitTrigger: true, languages: ["ts", "tsx"] } },
    twoslashOptions: {
      tsModule: ts,
      tsLibDirectory: dirname(ts.getDefaultLibFilePath({})),
      vfsRoot: snippetsRoot,
      cache: productionBuild ? new Map() : false,
      fsCache: productionBuild,
      compilerOptions: {
        target: ts.ScriptTarget.ES2023,
        module: ts.ModuleKind.ESNext,
        moduleResolution: ts.ModuleResolutionKind.Bundler,
        allowImportingTsExtensions: true,
        noEmit: true,
        strict: true,
        typeRoots: [
          resolve(import.meta.dirname, "../../packages/platform-cloudflare/node_modules"),
        ],
        types: [],
      },
    },
  }),
];
