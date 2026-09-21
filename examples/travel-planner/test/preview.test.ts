import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { expect, it } from "vite-plus/test";

import { localPreview } from "../preview/runtime.ts";

it("boots an isolated authenticated preview without cloud configuration and keeps private routes protected", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "preview-build-" });
      const server = `${directory}/server`;
      const client = `${directory}/client`;

      yield* fs.makeDirectory(server);
      yield* fs.makeDirectory(client);
      yield* fs.writeFileString(`${client}/fixture.js`, "/* preview asset */");

      const bundle = yield* Effect.promise(() =>
        build({
          entryPoints: [`${import.meta.dirname}/../src/worker.ts`],
          bundle: true,
          write: false,
          format: "esm",
          target: "es2022",
          platform: "browser",
          conditions: ["workerd", "worker", "browser"],
          external: ["cloudflare:*", "node:*"],
          alias: {
            "@tanstack/react-start/server-entry": `${import.meta.dirname}/fixtures/start.ts`,
          },
          banner: {
            js: 'import { createRequire } from "node:module"; const require = createRequire("/fixture.mjs");',
          },
          logLevel: "silent",
        }),
      );

      yield* fs.writeFileString(`${server}/worker.js`, bundle.outputFiles[0]!.text);
      const { preview, origin } = yield* localPreview(0, { server, client });

      yield* Effect.promise(async () => {
        const session = await preview.dispatchFetch(`${origin}/auth/getSession`);

        expect(session.status).toBe(200);
        expect(await session.json()).toEqual({ _tag: "Success", value: null });
        expect(session.headers.get("cache-control")).toBe("no-store");
        for (const cookie of ["", "__Host-elsewhere-auth-session=forged"]) {
          const denied = await preview.dispatchFetch(`${origin}/api/rpc`, { headers: { cookie } });

          expect(denied.status).toBe(401);
          await denied.arrayBuffer();
        }
        const home = await preview.dispatchFetch(`${origin}/`, { redirect: "manual" });

        expect(home.status).toBe(303);
        expect(home.headers.get("location")).toBe("/login");
        const login = await preview.dispatchFetch(`${origin}/login`);

        expect(login.status).toBe(200);
        await login.arrayBuffer();

        const response = await preview.dispatchFetch(`${origin}/auth/beginEmailRegistration`, {
          method: "POST",
          headers: { origin, "x-effect-auth-csrf": "1", "content-type": "application/json" },
          body: JSON.stringify({ payload: { flowId: "preview-registration" } }),
        });

        expect(response.status).toBe(200);
        Schema.decodeUnknownSync(Schema.Struct({ _tag: Schema.Literal("Success") }))(
          await response.json(),
        );
        expect(response.headers.getSetCookie().some((cookie) => /Secure/i.test(cookie))).toBe(true);

        const crossOrigin = await preview.dispatchFetch(`${origin}/auth/beginEmailRegistration`, {
          method: "POST",
          headers: {
            origin: "https://attacker.test",
            "x-effect-auth-csrf": "1",
            "content-type": "application/json",
          },
          body: JSON.stringify({ payload: { flowId: "cross-origin" } }),
        });

        expect(crossOrigin.status).toBe(403);
        await crossOrigin.arrayBuffer();
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
}, 30_000);
