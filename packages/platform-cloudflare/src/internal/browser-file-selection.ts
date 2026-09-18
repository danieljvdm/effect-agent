import type { CDPSession, Page } from "@cloudflare/puppeteer";
import { Encoding } from "effect";
import type { BrowserSelectFileRequest } from "effect-agent/interactive-browser";

type Element = NonNullable<Awaited<ReturnType<Page["$"]>>>;

// Both calls run this fixed function in Chromium. No caller-provided code or
// filesystem path is evaluated, and none of the file bytes enter diagnostics.
function setFileSelection(element: object, encoded: string, name: string, type: string): void {
  if (
    Reflect.get(element, "type") !== "file" ||
    Reflect.get(element, "disabled") ||
    Reflect.get(element, "webkitdirectory")
  ) {
    throw new Error("The target is not an enabled file input");
  }
  const bytes = Uint8Array.from(atob(encoded), (character) => character.charCodeAt(0));
  const Transfer = Reflect.get(globalThis, "DataTransfer");
  const transfer = new Transfer();

  transfer.items.add(new File([bytes], name, { type }));
  Reflect.set(element, "files", transfer.files);
  const selected = Reflect.get(element, "files");

  if (
    selected?.length !== 1 ||
    selected[0]?.size !== bytes.length ||
    selected[0]?.name !== name ||
    selected[0]?.type !== type
  ) {
    throw new Error("The input did not retain the selected file");
  }
  const dispatch = Reflect.get(element, "dispatchEvent");

  Reflect.apply(dispatch, element, [new Event("input", { bubbles: true })]);
  Reflect.apply(dispatch, element, [new Event("change", { bubbles: true })]);
}

const chooseFile = async (
  session: CDPSession,
  element: Element,
  request: BrowserSelectFileRequest,
  encoded: string,
  signal: AbortSignal,
): Promise<void> => {
  let onOpened: (event: { backendNodeId?: number }) => void = () => {};
  let onAbort: () => void = () => {};
  let timer: ReturnType<typeof setTimeout> | undefined;

  const opened = new Promise<number>((resolve, reject) => {
    onOpened = (event) =>
      event.backendNodeId === undefined
        ? reject(new Error("The chooser has no file input"))
        : resolve(event.backendNodeId);
    onAbort = () => reject(new Error("File selection interrupted"));
    session.on("Page.fileChooserOpened", onOpened);
    signal.addEventListener("abort", onAbort, { once: true });
    timer = setTimeout(() => reject(new Error("No file chooser opened")), 5_000);
    if (signal.aborted) onAbort();
  });

  try {
    const [backendNodeId] = await Promise.all([opened, element.click()]);

    if (signal.aborted) throw new Error("File selection interrupted");
    const resolved = await session.send("DOM.resolveNode", { backendNodeId });

    const objectId = resolved.object.objectId;

    if (objectId === undefined) throw new Error("The chooser has no file input");
    if (signal.aborted) throw new Error("File selection interrupted");

    const result = await session.send("Runtime.callFunctionOn", {
      objectId,
      functionDeclaration: `function(encoded, name, type) { (${setFileSelection.toString()})(this, encoded, name, type); }`,
      arguments: [{ value: encoded }, { value: request.fileName }, { value: request.mediaType }],
      returnByValue: true,
    });

    if (result.exceptionDetails !== undefined)
      throw new Error("The chooser did not select the file");
  } finally {
    clearTimeout(timer);
    session.off("Page.fileChooserOpened", onOpened);
    signal.removeEventListener("abort", onAbort);
    // Detaching the owned session releases all remote object handles, including
    // after timeout, interruption, or a navigation during change handlers.
  }
};

export const prepareFileSelection = async (
  page: Page,
  request: BrowserSelectFileRequest,
  signal: AbortSignal,
) => {
  const encoded = Encoding.encodeBase64(request.bytes);
  const session = request.target === "chooser" ? await page.createCDPSession() : undefined;

  if (session !== undefined) {
    try {
      await session.send("Page.enable");
      await session.send("Page.setInterceptFileChooserDialog", { enabled: true });
    } catch (cause) {
      await session.detach();
      throw cause;
    }
  }

  return {
    validate: async (element: Element): Promise<boolean> =>
      request.target === "chooser" ||
      (await element.evaluate(
        (input) =>
          Reflect.get(input, "type") === "file" &&
          !Reflect.get(input, "disabled") &&
          !Reflect.get(input, "webkitdirectory"),
      )),
    select: (element: Element): Promise<void> =>
      session === undefined
        ? element.evaluate(setFileSelection, encoded, request.fileName, request.mediaType)
        : chooseFile(session, element, request, encoded, signal),
    close: async (): Promise<void> => {
      if (session !== undefined) {
        try {
          await session.send("Page.setInterceptFileChooserDialog", { enabled: false });
        } finally {
          await session.detach();
        }
      }
    },
  };
};
