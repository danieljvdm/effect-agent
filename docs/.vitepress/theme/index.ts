import "@fontsource-variable/ibm-plex-sans";
import "@fontsource/ibm-plex-mono/400.css";
import TwoslashFloatingVue from "@shikijs/vitepress-twoslash/client";
import type { Theme } from "vitepress";
import DefaultTheme from "vitepress/theme-without-fonts";

import "@shikijs/vitepress-twoslash/style.css";
import "./custom.css";

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.use(TwoslashFloatingVue, {
      themes: {
        twoslash: {
          flip: true,
          triggers: ["hover", "click"],
          popperTriggers: ["hover"],
        },
      },
    });
  },
} satisfies Theme;
