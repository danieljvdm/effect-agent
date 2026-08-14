import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { PromptInputSubmit } from "./prompt-input";

describe("PromptInputSubmit", () => {
  it("never emits a native form submit control", () => {
    const markup = renderToStaticMarkup(<PromptInputSubmit status="ready" />);

    expect(markup).toContain('type="button"');
    expect(markup).not.toContain('type="submit"');
  });
});
