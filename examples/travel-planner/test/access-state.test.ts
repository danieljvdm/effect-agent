import { Schema } from "effect";
import { AsyncResult, AtomRegistry } from "effect/unstable/reactivity";
import { afterEach, expect, it, vi } from "vite-plus/test";

import { adminEmail, Email } from "../src/access-domain.ts";
import {
  manageMemberAtom,
  memberEmailAtom,
  membersAtom,
  refreshMembersAtom,
} from "../src/state.ts";

const Packet = Schema.Struct({
  id: Schema.Union([Schema.String, Schema.Number]),
  tag: Schema.Literals(["GetSession", "GetMembers", "InviteMember"]),
  payload: Schema.Unknown,
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

it("refreshes failed membership reads and uncertain invitations without repeating the write", async () => {
  vi.useFakeTimers();
  vi.stubGlobal("location", new URL("https://planner.test"));
  let readFails = true;
  const emails = [adminEmail];
  const requests: string[] = [];

  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(
      input instanceof Request ? input : new URL(String(input), "https://planner.test"),
      init,
    );

    const packet = Schema.decodeUnknownSync(Schema.fromJsonString(Packet))(
      (await request.text()).trim(),
    );

    requests.push(packet.tag);
    if (packet.tag === "InviteMember") {
      const invited = Schema.decodeUnknownSync(Schema.Struct({ email: Email }))(packet.payload);

      emails.push(invited.email);
    }

    const exit =
      packet.tag === "InviteMember" || (packet.tag === "GetMembers" && readFails)
        ? {
            _tag: "Failure",
            cause: [
              {
                _tag: "Fail",
                error: {
                  _tag: "AccessError",
                  code: "unavailable",
                  message: "Refresh before retrying.",
                },
              },
            ],
          }
        : {
            _tag: "Success",
            value:
              packet.tag === "GetSession"
                ? { email: adminEmail, isAdmin: true }
                : { emails, adminEmail },
          };

    return new Response(`${JSON.stringify({ _tag: "Exit", requestId: packet.id, exit })}\n`, {
      headers: { "content-type": "application/ndjson" },
    });
  });

  const registry = AtomRegistry.make();
  const unmounts = [registry.mount(membersAtom), registry.mount(manageMemberAtom)];

  try {
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isFailure(registry.get(membersAtom))).toBe(true);
    readFails = false;
    registry.set(refreshMembersAtom, undefined);
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.getOrElse(registry.get(membersAtom), () => null)?.emails).toEqual([
      adminEmail,
    ]);

    registry.set(memberEmailAtom, "friend@example.com");
    registry.set(manageMemberAtom, { action: "invite" });
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isFailure(registry.get(manageMemberAtom))).toBe(true);
    registry.set(refreshMembersAtom, undefined);
    await vi.advanceTimersByTimeAsync(1);
    expect(AsyncResult.isInitial(registry.get(manageMemberAtom))).toBe(true);
    expect(AsyncResult.getOrElse(registry.get(membersAtom), () => null)?.emails).toEqual([
      adminEmail,
      "friend@example.com",
    ]);
    expect(requests.filter((tag) => tag === "InviteMember")).toHaveLength(1);
    expect(requests.filter((tag) => tag === "GetMembers")).toHaveLength(3);
  } finally {
    for (const unmount of unmounts) unmount();
    registry.dispose();
  }
});
