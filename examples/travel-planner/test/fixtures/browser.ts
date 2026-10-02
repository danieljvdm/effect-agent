import { WebCapture } from "@yielded/agent";
import { Effect } from "effect";
import { Toolkit } from "effect/ai";

import { ReadTravelPage } from "../../src/research.ts";

export const FixtureBrowserLive = Toolkit.make(ReadTravelPage).toLayer({
  read_travel_page: () =>
    Effect.fail(
      new WebCapture.WebCaptureFailure({
        errorTag: "TestBrowser",
        message: "Browser research is replaced in tests.",
      }),
    ),
});
