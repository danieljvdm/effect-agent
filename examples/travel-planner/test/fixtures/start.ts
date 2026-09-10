/** The RPC integration suite leaves SSR rendering outside its boundary. */
export default {
  fetch: () => new Response("SSR fixture", { status: 404 }),
};
