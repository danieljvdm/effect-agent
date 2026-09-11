/** The RPC integration suite leaves SSR rendering outside its boundary. */
export default {
  fetch: (request: Request) =>
    new Response(
      new URL(request.url).pathname === "/login" ||
        new URL(request.url).pathname === "/auth/github/callback"
        ? "Login fixture"
        : "SSR fixture",
      {
        status: ["/login", "/auth/github/callback"].includes(new URL(request.url).pathname)
          ? 200
          : 404,
      },
    ),
};
