import { Duration, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  NetworkDisabled,
  SandboxError,
  SandboxEvent,
  SandboxExitError,
  SandboxImplementation,
  SandboxRequest,
} from "../src/index.ts";

describe("Sandbox schemas", () => {
  it("round-trips a complete request without embedding raw secret values", () => {
    const request = SandboxRequest.make({
      runtime: { kind: "container", identity: "sha256:runtime" },
      command: "echo",
      args: ["hello"],
      cwd: "/workspace",
      environment: { allow: ["LANG"] },
      mounts: [{ source: "/workspace", target: "/workspace", access: "read-only" }],
      network: NetworkDisabled.make({}),
      limits: {
        cpuCores: 1,
        memoryBytes: 64 * 1024 * 1024,
        maxOutputBytes: 1_024,
        maxWallTime: Duration.seconds(5),
      },
      secretHandles: [{ id: "maps-api", purpose: "geocoding" }],
      artifactRules: [{ path: "/workspace/report.json", maxBytes: 1_024 }],
    });

    expect(Schema.decodeSync(SandboxRequest)(Schema.encodeSync(SandboxRequest)(request))).toEqual(
      request,
    );
  });

  it("keeps all emitted events and expected failures schema-decodable", () => {
    const implementation = SandboxImplementation.make({
      isolation: "unisolated",
      identity: "local-process",
    });
    const event = {
      _tag: "SandboxOutput",
      eventVersion: 1,
      implementation,
      stream: "stderr",
      text: "warning",
      bytes: 7,
    } as const;
    const failure = SandboxExitError.make({
      implementation,
      exitCode: 2,
      message: "process exited with code 2",
    });

    expect(Schema.decodeSync(SandboxEvent)(event)).toEqual(event);
    expect(Schema.decodeSync(SandboxError)(Schema.encodeSync(SandboxError)(failure))).toEqual(
      failure,
    );
  });

  it("rejects oversized untrusted request collections at the Schema boundary", () => {
    const oversizedArgs = Array.from({ length: 257 }, () => "argument");
    expect(() =>
      Schema.decodeSync(SandboxRequest)({
        runtime: { kind: "unisolated-process", identity: "node" },
        command: "node",
        args: oversizedArgs,
        cwd: "/workspace",
        environment: { allow: [] },
        mounts: [],
        network: NetworkDisabled.make({}),
        limits: {
          maxOutputBytes: 1_024,
          maxWallTime: Duration.seconds(5),
        },
        secretHandles: [],
        artifactRules: [],
      }),
    ).toThrow();
  });
});
