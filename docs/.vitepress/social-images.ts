import {
  createCanvas,
  GlobalFonts,
  type Image,
  loadImage,
  type SKRSContext2D,
} from "@napi-rs/canvas";
import { Effect, FileSystem, Path, Schema } from "effect";

export interface SocialPage {
  readonly title: string;
  readonly description: string;
  readonly url: string;
  readonly imagePath: string;
}

class SocialImageError extends Schema.TaggedError<SocialImageError>()("SocialImageError", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

const registerFont = Effect.fn("socialImages.registerFont")(function* (
  filename: string,
  family: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const data = yield* fs.readFile(filename);

  return yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        const key = GlobalFonts.register(Buffer.from(data), family);

        if (key === null) throw new Error(`Cannot register ${filename}`);

        return key;
      },
      catch: (cause) => new SocialImageError({ message: `Cannot load ${family}`, cause }),
    }),
    (key) => Effect.sync(() => GlobalFonts.remove(key)),
  );
});

// Measure actual glyphs so long titles and descriptions wrap without clipping.
// Fail the build if future copy cannot fit, rather than publish an unreadable card.
const fitText = (
  context: SKRSContext2D,
  text: string,
  maxSize: number,
  minSize: number,
  maxLines: number,
  weight: number,
) => {
  for (let size = maxSize; size >= minSize; size -= 2) {
    context.font = `${weight} ${size}px "Social Sans"`;
    const lines: string[] = [];
    let line = "";

    for (const word of text.trim().split(/\s+/)) {
      const candidate = line === "" ? word : `${line} ${word}`;

      if (line !== "" && context.measureText(candidate).width > 1072) {
        lines.push(line);
        line = word;
      } else {
        line = candidate;
      }
    }
    lines.push(line);
    if (
      lines.length <= maxLines &&
      lines.every((value) => context.measureText(value).width <= 1072)
    ) {
      return { lines, lineHeight: size * 1.15 };
    }
  }

  throw new Error(`Social card text is too long: ${text}`);
};

const renderImage = (page: SocialPage, mark: Image) => {
  const canvas = createCanvas(1200, 630);
  const context = canvas.getContext("2d");

  context.fillStyle = "#315be8";
  context.fillRect(0, 0, 1200, 630);
  context.drawImage(mark, 64, 48, 52, 52);
  context.textBaseline = "top";
  context.fillStyle = "#f8f9fb";
  context.font = '500 32px "Social Sans"';
  context.fillText("Effect Agent", 132, 55);

  const url = new URL(page.url);
  const section = url.pathname.split("/")[1];

  context.font = '20px "Social Mono"';
  context.fillStyle = "#dce4ff";
  context.fillText(section ? `${section.toUpperCase()} / DOCUMENTATION` : "DOCUMENTATION", 64, 162);

  context.fillStyle = "#ffffff";
  const title = fitText(context, page.title, 80, 48, 2, 600);

  title.lines.forEach((line, index) => context.fillText(line, 64, 206 + index * title.lineHeight));
  const descriptionY = 206 + title.lines.length * title.lineHeight + 24;
  const description = fitText(context, page.description, 32, 24, 3, 400);

  context.fillStyle = "#e4eaff";
  description.lines.forEach((line, index) =>
    context.fillText(line, 64, descriptionY + index * description.lineHeight),
  );

  context.fillStyle = "#ffffff40";
  context.fillRect(64, 548, 1072, 1);
  context.fillStyle = "#f8f9fb";
  context.font = '20px "Social Mono"';
  context.fillText(`${url.host}${url.pathname === "/" ? "" : url.pathname}`, 64, 574);

  return canvas.toBuffer("image/png");
};

/** Build static PNGs from the same resolved page data used by the HTML metadata. */
export const writeSocialImages = Effect.fn("socialImages.write")(function* (
  srcDir: string,
  outDir: string,
  pages: Iterable<SocialPage>,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  yield* registerFont(
    path.resolve(
      srcDir,
      "../node_modules/@fontsource-variable/ibm-plex-sans/files/ibm-plex-sans-latin-wght-normal.woff2",
    ),
    "Social Sans",
  );
  yield* registerFont(
    path.resolve(
      srcDir,
      "../node_modules/@fontsource/ibm-plex-mono/files/ibm-plex-mono-latin-400-normal.woff2",
    ),
    "Social Mono",
  );
  const markData = yield* fs.readFile(path.join(srcDir, "public/mark.svg"));

  const mark = yield* Effect.tryPromise({
    try: () => loadImage(Buffer.from(markData)),
    catch: (cause) => new SocialImageError({ message: "Cannot load the site mark", cause }),
  });

  for (const page of pages) {
    const png = yield* Effect.try({
      try: () => renderImage(page, mark),
      catch: (cause) => new SocialImageError({ message: `Cannot render ${page.url}`, cause }),
    });

    const filename = path.join(outDir, page.imagePath);

    yield* fs.makeDirectory(path.dirname(filename), { recursive: true });
    yield* fs.writeFile(filename, png);
  }
});
