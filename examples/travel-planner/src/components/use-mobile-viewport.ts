import { useEffect, type RefObject } from "react";

/** Safari's keyboard resizes the visible viewport, independently of CSS viewport units. */
export function useMobileViewport(transcript: RefObject<HTMLDivElement | null>) {
  useEffect(() => {
    const viewport = window.visualViewport;

    if (!viewport) return;
    const mobile = window.matchMedia("(max-width: 700px)");
    const root = document.documentElement;

    const reset = () => {
      root.style.removeProperty("--planner-viewport-height");
      root.style.removeProperty("--planner-viewport-top");
    };

    const update = () => {
      if (!mobile.matches) {
        reset();

        return;
      }
      // Leave pinch zoom to the browser; don't resize the layout around a magnified view.
      if (Math.abs(viewport.scale - 1) > 0.01) return;
      const messages = transcript.current;

      // Measure before shrinking the list: the new gap must not look like a user scroll.
      const atBottom =
        messages && messages.scrollHeight - messages.scrollTop - messages.clientHeight < 80;

      root.style.setProperty("--planner-viewport-height", `${viewport.height}px`);
      root.style.setProperty("--planner-viewport-top", `${viewport.offsetTop}px`);
      if (atBottom) messages.scrollTo({ top: messages.scrollHeight });
    };

    update();
    viewport.addEventListener("resize", update);
    viewport.addEventListener("scroll", update);
    mobile.addEventListener("change", update);

    return () => {
      viewport.removeEventListener("resize", update);
      viewport.removeEventListener("scroll", update);
      mobile.removeEventListener("change", update);
      reset();
    };
  }, [transcript]);
}
