/** The RPC integration suite leaves SSR rendering outside its boundary. */
export default {
  fetch: (request: Request) =>
    new Response(
      new URL(request.url).pathname === "/login" ||
        new URL(request.url).pathname === "/auth/github/callback"
        ? '<!doctype html><html><head><link rel="stylesheet" href="/assets/login.css"><link rel="modulepreload" href="/assets/login.js"></head><body>Login fixture<script type="module" src="/assets/login.js"></script></body></html>'
        : "SSR fixture",
      {
        headers: { "content-type": "text/html" },
        status: ["/login", "/auth/github/callback"].includes(new URL(request.url).pathname)
          ? 200
          : 404,
      },
    ),
};
