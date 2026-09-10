import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Effect, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, it } from "vite-plus/test";

import { AccessSession, adminEmail } from "../src/access-domain.ts";
import {
  PlannerProgress,
  PlannerSettings,
  PlannerSnapshot,
  PublishedSite,
  Trip,
} from "../src/domain.ts";
import { fixtureTravelContent } from "./fixtures/models.ts";

const token = "travel-planner-test-owner-token";
const guestEmail = "guest@example.com";
const firstConversation = "lisbon-conversation";
const secondConversation = "kyoto-conversation";
const accessAccountId = "a".repeat(32);
const accessGroupId = "11111111-2222-3333-4444-555555555555";
const accessToken = "fixture-access-api-token";
const accessGroupUrl = `https://api.cloudflare.com/client/v4/accounts/${accessAccountId}/access/groups/${accessGroupId}`;

const GroupUpdate = Schema.Struct({
  include: Schema.Array(Schema.Struct({ email: Schema.Struct({ email: Schema.String }) })),
});

const researchedNote =
  "[Lisbon apartment candidate](https://www.airbnb.com/rooms/1234567890123456789?check_in=2026-10-14&check_out=2026-10-18&adults=2) — A researched lodging option near the planned neighborhood walks, with room to relax between outings. Compare the final total, cancellation policy, accessibility, and exact location before choosing. Dates, availability, and prices still require confirmation; this saved reference is not a booking.";

const RpcExit = Schema.Struct({
  _tag: Schema.Literal("Exit"),
  exit: Schema.Union([
    Schema.Struct({ _tag: Schema.Literal("Success"), value: Schema.Unknown }),
    Schema.Struct({ _tag: Schema.Literal("Failure"), cause: Schema.Unknown }),
  ]),
});

it("isolates conversations while retaining owner trips, native mutations, publication, and recovery", async () => {
  const bundle = await build({
    entryPoints: [join(import.meta.dirname, "fixtures/worker.ts")],
    bundle: true,
    write: false,
    format: "esm",
    target: "es2022",
    platform: "browser",
    conditions: ["workerd", "worker", "browser"],
    external: ["cloudflare:*", "node:*"],
    alias: { "@tanstack/react-start/server-entry": join(import.meta.dirname, "fixtures/start.ts") },
    banner: {
      js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
    },
    logLevel: "silent",
  });

  const output = bundle.outputFiles[0];

  if (output === undefined) throw new Error("No worker bundle");
  const directory = await mkdtemp(join(tmpdir(), "travel-planner-test-"));
  let members = [adminEmail];
  let redirectAccess = false;
  const accessRequests: Array<{ url: string; method: string }> = [];

  const makeRuntime = () =>
    new Miniflare(
      convertV4MiniflareOptions({
        modules: true,
        script: output.text,
        modulesRoot: "/",
        compatibilityDate: "2026-07-01",
        compatibilityFlags: ["nodejs_compat"],
        bindings: {
          PLANNER_TOKEN: token,
          ACCESS_ACCOUNT_ID: accessAccountId,
          ACCESS_GROUP_ID: accessGroupId,
          ACCESS_API_TOKEN: accessToken,
        },
        outboundService: async (request) => {
          accessRequests.push({ url: request.url, method: request.method });
          if (request.url !== accessGroupUrl)
            return new Response("Unexpected outbound destination", { status: 500 });
          expect(request.headers.get("authorization")).toBe(`Bearer ${accessToken}`);
          if (redirectAccess)
            return new Response(null, {
              status: 302,
              headers: { location: "https://untrusted.example/receive-credentials" },
            });
          if (request.method === "PUT") {
            const update = Schema.decodeUnknownSync(GroupUpdate)(await request.json());

            members = update.include.map(({ email }) => email.email);
          } else expect(request.method).toBe("GET");

          return Response.json({
            success: true,
            result: {
              id: accessGroupId,
              uid: accessGroupId,
              name: "effect-agent-travel-planner-invited",
              include: members.map((email) => ({ email: { email } })),
              require: [],
              exclude: [],
              created_at: "2026-09-09T21:00:21Z",
              updated_at: "2026-09-09T21:00:21Z",
            },
            errors: [],
            messages: [],
          });
        },
        durableObjects: { THREADS: { className: "TravelPlannerThread", useSQLite: true } },
        resourcePersistencePath: directory,
      }),
    );

  let runtime = makeRuntime();

  const rpcExit = async (tag: string, payload: unknown, path = "/api/rpc", email = adminEmail) => {
    const response = await runtime.dispatchFetch(`http://planner${path}`, {
      method: "POST",
      redirect: "manual",
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/ndjson",
        "x-test-email": email,
      },
      body: `${JSON.stringify({ _tag: "Request", id: "1", tag, payload: payload ?? null, headers: [] })}\n`,
    });

    const body = await response.text();

    expect({ status: response.status, body }).toMatchObject({ status: 200 });
    expect(response.headers.get("cache-control")).toBe("no-store");
    const lines = body.trim().split("\n");
    const decoded = Schema.decodeUnknownOption(Schema.fromJsonString(RpcExit))(lines[0]);

    if (decoded._tag === "None") throw new Error(body);

    return decoded.value.exit;
  };

  const rpc = async (tag: string, payload: unknown, path = "/api/rpc", email = adminEmail) => {
    const result = await rpcExit(tag, payload, path, email);

    if (result._tag !== "Success") throw new Error(JSON.stringify(result.cause));

    return result.value;
  };

  const snapshot = async (
    path = "/api/rpc",
    conversationId = firstConversation,
    email = adminEmail,
  ) =>
    Schema.decodeUnknownSync(PlannerSnapshot)(
      await rpc("GetPlanner", { conversationId }, path, email),
    );

  const arm = (point: string, mode = "failure") =>
    runtime.dispatchFetch(`http://planner/__test/failpoint?point=${point}&mode=${mode}`, {
      headers: { authorization: `Bearer ${token}` },
    });

  const until = async (
    predicate: (state: PlannerSnapshot) => boolean,
    conversationId = firstConversation,
    email = adminEmail,
  ) => {
    let latest = await snapshot("/api/rpc", conversationId, email);

    for (let attempt = 0; attempt < 200; attempt++) {
      if (predicate(latest)) return latest;
      await Effect.runPromise(Effect.sleep("50 millis"));
      latest = await snapshot("/api/rpc", conversationId, email);
    }
    throw new Error(`Planner did not settle: ${JSON.stringify(latest)}`);
  };

  try {
    for (const path of [
      "/api/rpc",
      "/api/rpc/",
      "/api/access",
      "/api/access/",
      "/api/progress",
      "/api/progress/",
    ]) {
      for (const [headers, expected] of [
        [{}, 401],
        [{ authorization: "Bearer wrong-owner-token" }, 401],
        [{ authorization: `Bearer ${token}`, origin: "https://unrelated.example" }, 403],
      ] as const) {
        const response = await runtime.dispatchFetch(`http://planner${path}`, {
          method: "POST",
          redirect: "manual",
          headers,
        });

        await response.arrayBuffer();
        expect(response.status).toBe(expected);
      }

      const oversized = await runtime.dispatchFetch(`http://planner${path}`, {
        method: "POST",
        redirect: "manual",
        headers: { authorization: `Bearer ${token}` },
        body: "x".repeat(32 * 1024 + 1),
      });

      await oversized.arrayBuffer();
      expect(oversized.status).toBe(413);

      const wrongMethod = await runtime.dispatchFetch(`http://planner${path}`, {
        method: "GET",
        redirect: "manual",
        headers: { authorization: `Bearer ${token}` },
      });

      await wrongMethod.arrayBuffer();
      expect(wrongMethod.status).toBe(405);
    }
    for (const path of ["/api/rpc", "/api/rpc/"])
      expect((await snapshot(path)).messages[0]?.text).toBe("Where do you want to go?");
    for (const email of [adminEmail, guestEmail]) {
      const session = Schema.decodeUnknownSync(AccessSession)(
        await rpc("GetSession", undefined, "/api/access", email),
      );

      expect(session).toEqual({ email, isAdmin: email === adminEmail });
    }
    // A guest must fail authorization before reaching the management API or its Object.
    for (const [tag, payload] of [
      ["GetMembers", undefined],
      ["InviteMember", { email: "another@example.com" }],
      ["RemoveMember", { email: adminEmail }],
    ] as const) {
      const denied = await rpcExit(tag, payload, "/api/access", guestEmail);

      expect(denied._tag).toBe("Failure");
      if (denied._tag !== "Failure") throw new Error("Guest access management was accepted");
      expect(JSON.stringify(denied.cause)).toContain('"code":"forbidden"');
    }
    expect(accessRequests).toEqual([]);
    // Exercise native Fetch from the real owner Object, not a Node fetch substitute.
    expect(await rpc("GetMembers", undefined, "/api/access")).toEqual({
      emails: [adminEmail],
      adminEmail,
    });
    expect(await rpc("InviteMember", { email: guestEmail }, "/api/access")).toEqual({
      emails: [adminEmail, guestEmail],
      adminEmail,
    });
    expect(await rpc("RemoveMember", { email: guestEmail }, "/api/access")).toEqual({
      emails: [adminEmail],
      adminEmail,
    });
    redirectAccess = true;
    const redirected = await rpcExit("GetMembers", undefined, "/api/access");

    expect(redirected._tag).toBe("Failure");
    expect(JSON.stringify(redirected)).toContain('"code":"unavailable"');
    expect(accessRequests.map(({ method }) => method)).toEqual([
      "GET",
      "GET",
      "PUT",
      "GET",
      "PUT",
      "GET",
    ]);
    expect(accessRequests.every(({ url }) => url === accessGroupUrl)).toBe(true);
    redirectAccess = false;
    expect((await snapshot()).messages[0]?.text).toBe("Where do you want to go?");
    await rpc("SendMessage", {
      conversationId: firstConversation,
      message: "Let's go to Lisbon",
      selectedTripId: null,
      requestId: "create-lisbon",
    });
    const saved = await until((state) => state.pending === 0 && state.trips.length === 1);

    expect(saved.trips[0]?.destination).toBe("Lisbon");
    expect(saved.conversationId).toBe(firstConversation);
    expect(saved.trips[0]?.conversationId).toBe(firstConversation);
    expect(saved.activity.some((item) => item.text === "save_trip: completed")).toBe(true);
    const trip = saved.trips[0]!;

    await rpc("SendMessage", {
      conversationId: firstConversation,
      message: "Let's go to Lisbon",
      selectedTripId: null,
      requestId: "create-lisbon",
    });
    expect((await snapshot()).trips).toHaveLength(1);
    await rpc("SendMessage", {
      conversationId: firstConversation,
      message: "Include a relaxed afternoon",
      selectedTripId: trip.id,
      requestId: "revise-lisbon",
    });
    const revised = await until((state) => state.pending === 0 && state.trips[0]?.revision === 2);

    expect(revised.trips[0]?.notes).toContain("Include a relaxed afternoon");
    expect((await rpcExit("PublishTrip", { tripId: trip.id, expectedRevision: 1 }))._tag).toBe(
      "Failure",
    );
    await rpc("SendMessage", {
      conversationId: firstConversation,
      message: "Publish this trip",
      selectedTripId: trip.id,
      requestId: "publish-lisbon",
    });

    const published = await until(
      (state) => state.pending === 0 && state.trips[0]?.published !== null,
    );

    expect(published.activity.some((item) => item.text === "publish_trip_site: completed")).toBe(
      true,
    );
    const site = Schema.decodeUnknownSync(PublishedSite)(published.trips[0]?.published);

    expect(site.revision).toBe(2);
    expect(researchedNote.length).toBeGreaterThan(240);

    const update = Schema.decodeUnknownSync(Trip)(
      await rpc("SaveTrip", {
        ...trip,
        conversationId: firstConversation,
        title: "Lisbon revised again",
        notes: [...trip.notes, researchedNote],
        tripId: trip.id,
        expectedRevision: 2,
      }),
    );

    expect(update.revision).toBe(3);
    expect(update.published).toEqual(site);
    expect(update.notes).toContain(researchedNote);

    const saveAgain = {
      ...update,
      conversationId: firstConversation,
      title: "Survives a lost response",
      tripId: trip.id,
      expectedRevision: 3,
    };

    await arm("save:before");
    expect((await rpcExit("SaveTrip", saveAgain))._tag).toBe("Failure");
    expect((await snapshot()).trips[0]?.revision).toBe(3);
    await arm("save:before", "defect");
    expect((await rpcExit("SaveTrip", saveAgain))._tag).toBe("Failure");
    expect((await snapshot()).trips[0]?.revision).toBe(3);
    await arm("save:before", "interruption");
    expect((await rpcExit("SaveTrip", saveAgain))._tag).toBe("Failure");
    expect((await snapshot()).trips[0]?.revision).toBe(3);
    await arm("save:after");
    expect((await rpcExit("SaveTrip", saveAgain))._tag).toBe("Failure");
    expect((await snapshot()).trips[0]?.revision).toBe(4);
    // A lost response cannot overwrite or duplicate the committed revision on retry.
    expect((await rpcExit("SaveTrip", saveAgain))._tag).toBe("Failure");
    await arm("publication:after");
    expect((await rpcExit("PublishTrip", { tripId: trip.id, expectedRevision: 4 }))._tag).toBe(
      "Failure",
    );

    const republished = Schema.decodeUnknownSync(PublishedSite)(
      await rpc("PublishTrip", { tripId: trip.id, expectedRevision: 4 }),
    );

    expect(republished.revision).toBe(4);
    const firstBeforeNew = await snapshot();
    const emptyConversation = await snapshot("/api/rpc", secondConversation);

    expect(firstBeforeNew.trips.find((item) => item.id === trip.id)?.notes).toContain(
      researchedNote,
    );
    expect(emptyConversation.conversationId).toBe(secondConversation);
    expect(emptyConversation.messages.map(({ text }) => text)).toEqual([
      "Where do you want to go?",
    ]);
    expect(emptyConversation.activity).toEqual([]);
    expect(emptyConversation.pending).toBe(0);
    expect(emptyConversation.trips).toEqual(firstBeforeNew.trips);

    await rpc("SendMessage", {
      conversationId: secondConversation,
      message: "Let's go to Kyoto",
      selectedTripId: null,
      // Idempotency belongs to each framework Thread, not the owner Object.
      requestId: "create-lisbon",
    });

    const second = await until(
      (state) => state.pending === 0 && state.trips.length === 2,
      secondConversation,
    );

    const kyoto = second.trips.find((item) => item.conversationId === secondConversation);

    expect(kyoto).toMatchObject({
      destination: "Kyoto",
      title: "A few days in Kyoto",
      revision: 1,
    });
    expect(kyoto?.summary).toContain("Kyoto");
    expect(kyoto?.notes).toContain("Earlier user messages in this conversation: 0");
    expect(second.messages.filter(({ role }) => role === "user").map(({ text }) => text)).toEqual([
      "Let's go to Kyoto",
    ]);
    expect(second.activity.some((item) => item.text === "save_trip: completed")).toBe(true);

    expect(
      (
        await rpcExit("SendMessage", {
          conversationId: secondConversation,
          message: "Change Lisbon from Kyoto's conversation",
          selectedTripId: trip.id,
          requestId: "wrong-conversation-message",
        })
      )._tag,
    ).toBe("Failure");
    expect(
      (
        await rpcExit("SaveTrip", {
          ...saveAgain,
          conversationId: secondConversation,
          expectedRevision: 4,
          title: "Wrong conversation overwrite",
        })
      )._tag,
    ).toBe("Failure");
    const firstAfterNew = await snapshot();

    expect(firstAfterNew.messages).toEqual(firstBeforeNew.messages);
    expect(firstAfterNew.activity).toEqual(firstBeforeNew.activity);
    expect(firstAfterNew.trips.find((item) => item.id === trip.id)?.revision).toBe(4);
    expect((await snapshot("/api/rpc", secondConversation)).messages).toEqual(second.messages);

    const guestBefore = await snapshot("/api/rpc", firstConversation, guestEmail);

    expect(guestBefore.trips).toEqual([]);
    expect(guestBefore.conversations).toEqual([]);
    expect(guestBefore.conversationId).toBe(firstConversation);
    expect(guestBefore.messages.map(({ text }) => text)).toEqual(["Where do you want to go?"]);
    expect(guestBefore.activity).toEqual([]);
    await rpc(
      "SendMessage",
      {
        conversationId: firstConversation,
        message: "Let's go to Copenhagen",
        selectedTripId: null,
        requestId: "create-lisbon",
      },
      "/api/rpc",
      guestEmail,
    );

    const guestSaved = await until(
      (state) => state.pending === 0 && state.trips.length === 1,
      firstConversation,
      guestEmail,
    );

    expect(guestSaved.conversationId).toBe(firstConversation);
    expect(guestSaved.conversations).toEqual([
      { conversationId: firstConversation, title: "Let's go to Copenhagen" },
    ]);
    expect(guestSaved.trips[0]).toMatchObject({
      destination: "Copenhagen",
      conversationId: firstConversation,
    });
    expect(guestSaved.trips[0]?.id).not.toBe(trip.id);
    expect(guestSaved.trips[0]?.notes).toContain("Earlier user messages in this conversation: 0");
    expect(
      guestSaved.messages.filter(({ role }) => role === "user").map(({ text }) => text),
    ).toEqual(["Let's go to Copenhagen"]);
    for (const [tag, payload] of [
      [
        "SendMessage",
        {
          conversationId: firstConversation,
          message: "Read and change Daniel's trip",
          selectedTripId: trip.id,
          requestId: "guest-cross-owner",
        },
      ],
      [
        "SaveTrip",
        {
          ...saveAgain,
          conversationId: firstConversation,
          expectedRevision: 4,
          title: "Guest overwrite",
        },
      ],
      ["PublishTrip", { tripId: trip.id, expectedRevision: 4 }],
    ] as const) {
      expect((await rpcExit(tag, payload, "/api/rpc", guestEmail))._tag).toBe("Failure");
    }
    expect(await snapshot("/api/rpc", firstConversation, guestEmail)).toEqual(guestSaved);
    expect(await snapshot()).toEqual(firstAfterNew);

    await runtime.dispose();
    runtime = makeRuntime();
    const restored = await snapshot();

    expect(await snapshot("/api/rpc", firstConversation, guestEmail)).toEqual(guestSaved);
    expect(restored.trips.find((item) => item.id === trip.id)?.revision).toBe(4);
    expect(restored.trips.find((item) => item.id === trip.id)?.published).toEqual(republished);
    expect(restored.trips.find((item) => item.id === trip.id)?.notes).toContain(researchedNote);
    expect(restored.messages).toEqual(published.messages);
    const secondRestored = await snapshot("/api/rpc", secondConversation);

    expect(secondRestored.messages).toEqual(second.messages);
    expect(secondRestored.activity).toEqual(second.activity);
    expect(secondRestored.trips).toEqual(restored.trips);
    expect(secondRestored.conversationId).toBe(secondConversation);

    const unauthorizedControl = await runtime.dispatchFetch("http://planner/__test/progress", {
      method: "POST",
    });

    await unauthorizedControl.arrayBuffer();
    expect(unauthorizedControl.status).toBe(401);

    // The real attempt writer and DO read are observed through the authenticated streaming RPC.
    const streamingConversation = "streaming-conversation";
    const streams: Array<ReadableStreamDefaultReader<Uint8Array>> = [];

    const progressPacket = Schema.Struct({
      _tag: Schema.Literal("Chunk"),
      values: Schema.Array(PlannerProgress),
    });

    const openProgress = async (email = adminEmail) => {
      const response = await runtime.dispatchFetch("http://planner/api/progress", {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "x-test-email": email,
          "content-type": "application/ndjson",
        },
        body: `${JSON.stringify({ _tag: "Request", id: "live", tag: "WatchProgress", payload: { conversationId: streamingConversation }, headers: [] })}\n`,
      });

      expect(response.status).toBe(200);
      expect(response.headers.get("cache-control")).toBe("no-store");
      if (response.body === null) throw new Error("No streaming body");
      const reader = response.body.getReader();

      streams.push(reader);
      let buffer = "";
      const decoder = new TextDecoder();
      const queued: PlannerProgress[] = [];

      return {
        cancel: () => reader.cancel(),
        next: async (): Promise<PlannerProgress> => {
          while (queued.length === 0) {
            const chunk = await reader.read();

            if (chunk.done) throw new Error("Progress closed before its expected frame");
            buffer += decoder.decode(chunk.value, { stream: true });
            let newline = buffer.indexOf("\n");

            while (newline >= 0) {
              const line = buffer.slice(0, newline);

              buffer = buffer.slice(newline + 1);
              const packet = Schema.decodeUnknownSync(Schema.fromJsonString(progressPacket))(line);

              queued.push(...packet.values);
              newline = buffer.indexOf("\n");
            }
          }
          const frame = queued.shift();

          if (frame === undefined) throw new Error("No progress frame");

          return frame;
        },
      };
    };

    const control = async (advance = false) => {
      const response = await runtime.dispatchFetch("http://planner/__test/progress", {
        method: advance ? "POST" : "GET",
        headers: { authorization: `Bearer ${token}` },
      });

      return Schema.decodeUnknownSync(
        Schema.Struct({
          settings: Schema.NullOr(PlannerSettings),
          completed: Schema.Natural,
          finalized: Schema.Natural,
        }),
      )(await response.json());
    };

    const admittedSettings = { model: "gpt-6-astra", reasoningEffort: "high", fast: true } as const;

    try {
      await rpc("SendMessage", {
        settings: admittedSettings,
        conversationId: streamingConversation,
        message: "streaming fixture",
        selectedTripId: null,
        requestId: "streaming-run",
      });
      const stream = await openProgress();
      let partial = await stream.next();

      while (partial.tools[0]?.state !== "running") partial = await stream.next();
      expect(partial.text).toBe("A quiet ");
      expect(partial.tools[0]).toMatchObject({ label: "Reading travel details", state: "running" });
      expect(await control()).toEqual({ settings: admittedSettings, completed: 0, finalized: 0 });
      const inFlight = await snapshot("/api/rpc", streamingConversation);

      expect(inFlight.pendingSubmissionIds).toContain(partial.submissionId);
      expect(
        inFlight.messages.some(({ role, id }) => role === "assistant" && id !== "welcome"),
      ).toBe(false);
      const guestStream = await openProgress(guestEmail);

      expect(await guestStream.next()).toMatchObject({
        submissionId: null,
        attemptId: null,
        text: "",
        tools: [],
      });
      await guestStream.cancel();
      await stream.cancel();
      expect(await control()).toEqual({ settings: admittedSettings, completed: 0, finalized: 0 });
      const reconnected = await openProgress();

      expect(await reconnected.next()).toEqual(partial);
      await control(true);
      let finishedTool = await reconnected.next();

      while (finishedTool.text !== "A quiet escape." || finishedTool.tools[0]?.state !== "complete")
        finishedTool = await reconnected.next();

      expect(finishedTool.text).toBe("A quiet escape.");
      expect(finishedTool.text).not.toContain("A quiet A quiet");
      expect(finishedTool.tools[0]?.state).toBe("complete");
      expect(finishedTool.revision).toBeGreaterThan(partial.revision);
      expect(finishedTool.attemptId).toBe(partial.attemptId);
      expect(await control()).toEqual({ settings: admittedSettings, completed: 0, finalized: 0 });
      await reconnected.cancel();
      await control(true);
      const settled = await until((state) => state.pending === 0, streamingConversation);

      expect(
        settled.messages
          .filter(({ role, id }) => role === "assistant" && id !== "welcome")
          .map(({ text }) => text),
      ).toEqual(["A quiet escape."]);
      expect(settled.pendingSubmissionIds).toEqual([]);
      expect(await control()).toEqual({ settings: admittedSettings, completed: 1, finalized: 1 });
      expect(JSON.stringify([partial, finishedTool, settled])).not.toContain(
        "PRIVATE_REASONING_SENTINEL",
      );
    } finally {
      for (const stream of streams) await stream.cancel();
    }

    const cardsConversation = "cards-conversation";

    await rpc("SendMessage", {
      conversationId: cardsConversation,
      message: "travel cards fixture",
      selectedTripId: null,
      requestId: "show-cards",
    });

    const withCards = await until(
      (state) => state.messages.some((message) => message.content !== undefined),
      cardsConversation,
    );

    const cardMessage = withCards.messages.find((message) => message.content !== undefined);

    expect(withCards.pending).toBe(1);
    expect(cardMessage).toMatchObject({
      role: "assistant",
      text: fixtureTravelContent.title,
      content: fixtureTravelContent,
    });
    expect(withCards.activity.some((item) => item.text === "show_travel_options: completed")).toBe(
      true,
    );
    expect(
      withCards.messages.some(
        (message) => message.text === "These sourced options are ready to compare.",
      ),
    ).toBe(false);
    expect(
      (await snapshot("/api/rpc", cardsConversation, guestEmail)).messages.some(
        (message) => message.content !== undefined,
      ),
    ).toBe(false);
    await control(true);
    const cardsSettled = await until((state) => state.pending === 0, cardsConversation);

    expect(cardsSettled.messages.filter((message) => message.content !== undefined)).toEqual([
      cardMessage,
    ]);
    expect(cardsSettled.messages.at(-1)?.text).toBe("These sourced options are ready to compare.");

    await rpc("SendMessage", {
      conversationId: "delivered-cards",
      message: "complete travel cards fixture",
      selectedTripId: null,
      requestId: "deliver-cards",
    });

    const delivered = await until(
      (state) => state.pending === 0 && state.messages.length > 1,
      "delivered-cards",
    );

    expect(delivered.messages.filter((message) => message.content !== undefined)).toHaveLength(1);
    expect(delivered.messages.find((message) => message.content !== undefined)?.content).toEqual(
      fixtureTravelContent,
    );
    expect(delivered.messages.at(-1)?.text).toBe(
      "Compare these stays. Exact availability is still unverified.",
    );
    await rpc("SendMessage", {
      conversationId: "invalid-plain-reply",
      message: "plain shortlist fixture",
      selectedTripId: null,
      requestId: "invalid-plain-reply",
    });

    const invalidReply = await until(
      (state) => state.pending === 0 && state.activity.some((item) => item.kind === "failure"),
      "invalid-plain-reply",
    );

    expect(
      invalidReply.messages.some(
        (message) => message.text === "Plain shortlist must not complete this run.",
      ),
    ).toBe(false);

    // Navigation must survive model failure or a run that never calls save_trip.
    expect(invalidReply.trips.some((item) => item.conversationId === "invalid-plain-reply")).toBe(
      false,
    );
    expect(invalidReply.conversations).toContainEqual({
      conversationId: "invalid-plain-reply",
      title: "plain shortlist fixture",
    });
    expect(cardsSettled.conversations).toContainEqual({
      conversationId: cardsConversation,
      title: "travel cards fixture",
    });

    // Both sides of navigation persistence remain safe to retry, without admitting work twice.
    for (const mode of ["failure", "defect", "interruption"]) {
      for (const side of ["before", "after"]) {
        const conversationId = `navigation-${mode}-${side}`;

        const request = {
          conversationId,
          message: "A golf trip",
          selectedTripId: null,
          requestId: conversationId,
        };

        await arm(`conversation:${side}`, mode);
        expect((await rpcExit("SendMessage", request))._tag).toBe("Failure");
        const interrupted = await snapshot("/api/rpc", conversationId);

        expect(interrupted.messages.filter((item) => item.role === "user")).toEqual([]);
        expect(
          interrupted.conversations?.some((item) => item.conversationId === conversationId),
        ).toBe(side === "after");
        await rpc("SendMessage", request);

        const resumed = await until(
          (state) => state.pending === 0 && state.messages.length > 1,
          conversationId,
        );

        expect(
          resumed.conversations?.filter((item) => item.conversationId === conversationId),
        ).toEqual([{ conversationId, title: "A golf trip" }]);
        await rpc("SendMessage", request);
        const replayed = await snapshot("/api/rpc", conversationId);

        expect(replayed.messages.filter((item) => item.role === "user")).toHaveLength(1);
      }
    }
    const finalCatalogue = (await snapshot()).conversations;

    await runtime.dispose();
    runtime = makeRuntime();
    expect((await snapshot()).conversations).toEqual(finalCatalogue);
    expect((await snapshot("/api/rpc", "invalid-plain-reply")).messages).toEqual(
      invalidReply.messages,
    );
    expect((await snapshot("/api/rpc", firstConversation, guestEmail)).conversations).toEqual(
      guestSaved.conversations,
    );
    expect((await snapshot("/api/rpc", cardsConversation)).messages).toEqual(cardsSettled.messages);
    expect((await snapshot("/api/rpc", "delivered-cards")).messages).toEqual(delivered.messages);
    // Each response crosses native RPC ownership, is buffered, and is disposed by ingress.
    for (let poll = 0; poll < 6; poll++)
      expect(await snapshot(poll % 2 === 0 ? "/api/rpc/" : "/api/rpc")).toEqual({
        ...restored,
        conversations: finalCatalogue,
      });
  } finally {
    await runtime.dispose();
    await rm(directory, { recursive: true, force: true });
  }
}, 60_000);
