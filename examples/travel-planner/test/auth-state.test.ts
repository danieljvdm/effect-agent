import { Effect, Schema, Stream } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { expect, it, vi } from "vite-plus/test";

import { auth, accountLifetime, sessionObservation, callbackInput } from "../src/auth/client";
import { draftAtom, modelSettingsOpenAtom, openAiConnectionAtom, sessionAtom } from "../src/state";
import * as voiceBrowser from "../src/voice/browser";
import { startVoiceAtom, voiceViewAtom } from "../src/voice/state";

const encodedSession = (subjectId: string) => ({
  sessionId: `session-${subjectId}`,
  subjectId,
  securityRevision: "revision",
  assurance: { method: "email", factors: ["possession"], authenticatedAt: 1_800_000_000_000 },
  issuedAt: 1_800_000_000_000,
  expiresAt: 1_900_000_000_000,
  absoluteExpiresAt: 1_900_000_000_000,
  claims: { displayName: "Fixture traveler" },
});

const packet = Schema.Struct({ id: Schema.Unknown, tag: Schema.String });

it("retires app registries and pending requests on signout/account switch while named auth mutations settle", async () => {
  vi.stubGlobal("location", new URL("https://fixture.test"));
  vi.stubGlobal("window", new EventTarget());
  vi.stubGlobal("document", Object.assign(new EventTarget(), { visibilityState: "visible" }));
  const storage = new Map<string, string>();
  let voiceFinalized = false;

  vi.spyOn(voiceBrowser, "connectBrowserVoice").mockReturnValue(
    Effect.addFinalizer(() =>
      Effect.sync(() => {
        voiceFinalized = true;
      }),
    ).pipe(
      Effect.as({
        events: Stream.make({
          type: "session.started" as const,
          session: { id: "voice-fixture" },
        }).pipe(Stream.concat(Stream.never)),
        send: () => Effect.void,
        silence: Effect.void,
        resume: Effect.void,
      }),
    ),
  );

  vi.stubGlobal("sessionStorage", {
    getItem: (key: string) => storage.get(key) ?? null,
    setItem: (key: string, value: string) => storage.set(key, value),
    get length() {
      return storage.size;
    },
    key: (index: number) => [...storage.keys()][index],
    removeItem: (key: string) => storage.delete(key),
  });
  let session: ReturnType<typeof encodedSession> | null = null;
  let nextSubject = "00000000-0000-0000-0000-000000000001";
  let finish: (() => void) | undefined;
  let cancelled = false;

  vi.stubGlobal(
    "fetch",
    vi.fn<typeof fetch>(async (input, init) => {
      const request = new Request(
        input instanceof Request ? input : new URL(String(input), "https://fixture.test"),
        init,
      );

      const path = new URL(request.url).pathname;

      if (path === "/api/rpc/" || path === "/api/rpc") {
        const body = Schema.decodeUnknownSync(Schema.fromJsonString(packet))(
          (await request.text()).trim(),
        );

        // Observe the signal given to fetch. A copied Request's dependent signal
        // can be garbage-collected while this fixture keeps its response pending.
        const signal = init?.signal ?? (input instanceof Request ? input.signal : undefined);

        if (!signal) throw new Error("Expected an abortable account request");
        signal.addEventListener(
          "abort",
          () => {
            cancelled = true;
          },
          { once: true },
        );

        return new Promise<Response>((resolve) => {
          finish = () =>
            resolve(
              new Response(
                JSON.stringify({
                  _tag: "Exit",
                  requestId: body.id,
                  exit: {
                    _tag: "Success",
                    value: { connected: true, lastFour: "9999", updatedAt: "2026-09-10T00:00:00Z" },
                  },
                }) + "\n",
                { headers: { "content-type": "application/ndjson" } },
              ),
            );
        });
      }
      let value: unknown = session;

      if (path === "/auth/completeEmailSignIn") {
        session = encodedSession(nextSubject);
        value = { completion: { _tag: "Authenticated", session }, returnTarget: "/" };
      }
      if (path === "/auth/signOut") {
        session = null;
        value = { clearCredential: true, invalidation: "revoked" };
      }

      return Response.json({ _tag: "Success", value });
    }),
  );
  const host = AtomRegistry.make();
  const disposeSession = host.mount(auth.session);
  const disposeObservation = host.mount(sessionObservation);
  const disposeLifetime = host.mount(accountLifetime);
  const disposeLogin = host.mount(auth.completeEmailSignIn);
  const disposeLogout = host.mount(auth.signOut);

  const login = async () => {
    host.set(auth.completeEmailSignIn, {
      flowId: crypto.randomUUID(),
      email: "fixture@example.com",
      returnTarget: "/",
      continuationId: "fixture-continuation",
    });
    await vi.waitFor(() =>
      expect(AsyncResult.getOrElse(host.get(auth.session), () => null)?.subjectId).toBe(
        nextSubject,
      ),
    );
    await vi.waitFor(() =>
      expect(AsyncResult.isSuccess(host.get(auth.completeEmailSignIn))).toBe(true),
    );

    return AsyncResult.getOrThrow(host.get(accountLifetime));
  };

  try {
    await vi.waitFor(() => expect(AsyncResult.isSuccess(host.get(auth.session))).toBe(true));
    const first = await login();

    first.registry.set(
      sessionAtom,
      AsyncResult.success({ subjectId: nextSubject, displayName: "First" }),
    );
    first.registry.set(draftAtom, "Private draft");
    storage.set(`travel-voice:v1:${nextSubject}:conversation`, "Private request");
    storage.set("unrelated", "Keep");
    first.registry.set(modelSettingsOpenAtom, true);
    first.registry.mount(openAiConnectionAtom);
    await vi.waitFor(() => expect(finish).toBeDefined());
    first.registry.mount(startVoiceAtom);
    first.registry.mount(voiceViewAtom);
    // Media fixture: the browser adapter is replaced; the real voice Effect scope still runs.
    first.registry.set(startVoiceAtom, { muted: false } as HTMLAudioElement);
    await vi.waitFor(() => expect(first.registry.get(voiceViewAtom).status).toBe("listening"));
    host.set(auth.signOut, undefined);
    await vi.waitFor(() => expect(AsyncResult.isSuccess(host.get(auth.signOut))).toBe(true));
    await vi.waitFor(() =>
      expect(AsyncResult.getOrElse(host.get(auth.session), () => "pending")).toBeNull(),
    );
    await vi.waitFor(() => expect(cancelled).toBe(true));
    await vi.waitFor(() => expect(voiceFinalized).toBe(true));
    expect([...storage]).toEqual([["unrelated", "Keep"]]);
    finish?.();
    const anonymous = AsyncResult.getOrThrow(host.get(accountLifetime));

    expect(anonymous.registry).not.toBe(first.registry);
    expect(anonymous.registry.get(draftAtom)).toBe("");
    expect(anonymous.registry.get(modelSettingsOpenAtom)).toBe(false);
    const sameAccount = await login();

    expect(sameAccount.registry).not.toBe(first.registry);
    expect(sameAccount.registry.get(draftAtom)).toBe("");
    expect(AsyncResult.value(sameAccount.registry.get(openAiConnectionAtom))._tag).toBe("None");
    nextSubject = "00000000-0000-0000-0000-000000000002";
    const other = await login();

    expect(other.registry).not.toBe(sameAccount.registry);
    expect(other.registry.get(draftAtom)).toBe("");
  } finally {
    finish?.();
    disposeLogin();
    disposeLogout();
    disposeObservation();
    disposeSession();
    disposeLifetime();
    host.dispose();
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  }
});

it("consumes callback credentials once and rejects ambiguous callback shapes before any exchange", async () => {
  try {
    for (const query of [
      "?state=s&code=one&code=two",
      "?state=s&code=one&error=access_denied",
      "?code=one",
      "?state=s&error=x&error=y",
    ]) {
      const captured = { __elsewhereCallback: query };
      let removed = false;

      vi.stubGlobal("window", captured);
      vi.stubGlobal("sessionStorage", {
        getItem: () => JSON.stringify({ flowId: "public-flow" }),
        removeItem: () => {
          removed = true;
        },
      });
      const exit = await Effect.runPromiseExit(callbackInput);

      expect(exit._tag).toBe("Failure");
      expect(captured.__elsewhereCallback).toBeUndefined();
      expect(removed).toBe(true);
    }
  } finally {
    vi.unstubAllGlobals();
  }
});
