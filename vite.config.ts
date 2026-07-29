import { defineConfig } from "vite-plus";

const generatedPaths = [".agents/**", ".claude/**", "repos/**"];

export default defineConfig({
  staged: {
    "*": "vp fmt --write",
  },
  fmt: {
    ignorePatterns: generatedPaths,
  },
  lint: {
    ignorePatterns: generatedPaths,
    options: {
      typeAware: true,
      typeCheck: true,
    },
    jsPlugins: [{ name: "vite-plus", specifier: "vite-plus/oxlint-plugin" }],
    rules: {
      "vite-plus/prefer-vite-plus-imports": "error",
    },
  },
  pack: {
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
  run: {
    cache: true,
  },
});
