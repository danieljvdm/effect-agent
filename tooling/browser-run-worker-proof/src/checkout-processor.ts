import { processorPage } from "./checkout-pages.ts";

export default {
  fetch(request: Request): Response {
    const page = processorPage(new URL(request.url));

    return page === null
      ? new Response("Not found", { status: 404 })
      : new Response(page, {
          headers: { "content-type": "text/html; charset=utf-8", "cache-control": "no-store" },
        });
  },
};
