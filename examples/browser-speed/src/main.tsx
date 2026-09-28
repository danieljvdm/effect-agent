import { RegistryProvider } from "@effect/atom-react";
import { createRoot } from "react-dom/client";
import "@fontsource-variable/ibm-plex-sans";

import "@fontsource/ibm-plex-mono/400.css";

import { App } from "./ui.tsx";

import "./style.css";

const root = document.getElementById("root");

if (root)
  createRoot(root).render(
    <RegistryProvider>
      <App />
    </RegistryProvider>,
  );
