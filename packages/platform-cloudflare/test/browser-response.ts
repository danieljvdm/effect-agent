// SDK substitutes do not consume protocol messages. Keep their provider upgrade
// separate from the real WebSocket/Chromium transport contracts.
export const browserResponse = (
  init?: RequestInit,
  sessionId = "c8b9c4b1-d1bf-4663-b4d8-a0b009cc8b99",
) => {
  if (init?.method === "POST") return Response.json({ sessionId });
  const events = new EventTarget();

  return Object.defineProperties(new Response(null), {
    status: { value: 101 },
    webSocket: {
      value: {
        accept: () => {},
        addEventListener: events.addEventListener.bind(events),
        close: () => events.dispatchEvent(new Event("close")),
        send: () => {
          throw new Error("Unexpected protocol message through SDK substitute");
        },
      },
    },
  });
};
