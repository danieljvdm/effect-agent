import { defineRouteMiddleware } from "@astrojs/starlight/route-data";

export const onRequest = defineRouteMiddleware((context) => {
  const { head, entry } = context.locals.starlightRoute;

  if (context.url.pathname === "/agent/404/") return;

  const title =
    entry.data.title === "Yielded Agent" ? "Yielded Agent" : `${entry.data.title} | Yielded Agent`;

  const description =
    entry.data.description ??
    "An agent harness toolkit for TypeScript, built on Effect and Effect AI.";

  const imagePath = entry.filePath.split("/content/docs/")[1]?.replace(/\.mdx?$/, "");

  if (imagePath === undefined) throw new Error(`Unknown documentation source: ${entry.filePath}`);
  const image = new URL(`/agent/social/${imagePath}.png`, "https://yielded.dev").href;
  const alt = `${title}. ${description}`;

  for (const item of head) {
    if (item.tag === "title") item.content = title;
    if (item.tag === "meta" && item.attrs?.property === "og:title") item.attrs.content = title;
    if (item.tag === "meta" && item.attrs?.property === "og:type") item.attrs.content = "website";
    if (item.tag === "meta" && item.attrs?.name === "twitter:card")
      item.attrs.content = "summary_large_image";
  }

  for (const [property, content] of Object.entries({
    "og:image": image,
    "og:image:alt": alt,
    "og:image:type": "image/png",
    "og:image:width": "1200",
    "og:image:height": "630",
  })) {
    head.push({ tag: "meta", attrs: { property, content } });
  }
  for (const [name, content] of Object.entries({
    "twitter:title": title,
    "twitter:description": description,
    "twitter:image": image,
    "twitter:image:alt": alt,
  })) {
    head.push({ tag: "meta", attrs: { name, content } });
  }
});
