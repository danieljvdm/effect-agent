import { Context, DateTime, Effect, Layer, Schema } from "effect";
import git, { type HttpClient } from "isomorphic-git";
import { memfs } from "memfs";

import {
  PlannerError,
  Revision,
  Trip,
  TripId,
  TripSiteStore,
  type PublishedSite,
} from "./domain.ts";
import { renderTripSite } from "./trip-site.ts";

const MAX_FILE_BYTES = 512 * 1024;
const MAX_TRANSFER_BYTES = 4 * 1024 * 1024;
const encoder = new TextEncoder();

const failed = () =>
  new PlannerError({
    code: "publication",
    message: "Trip publication storage is unavailable or contains invalid data.",
  });

const conflict = () =>
  new PlannerError({
    code: "conflict",
    message: "This published revision already contains different trip details.",
  });

const hasCode = (code: string) => Schema.is(Schema.Struct({ code: Schema.Literal(code) }));

const operation = <A>(name: string, run: () => Promise<A>) =>
  Effect.tryPromise({
    try: run,
    catch: (error) => ({
      reason:
        error instanceof git.Errors.HttpError
          ? `HTTP ${error.data.statusCode}`
          : error instanceof TypeError
            ? `TypeError ${
                error.stack
                  ?.match(/worker\.js:\d+:\d+/g)
                  ?.slice(0, 3)
                  .join(", ") ?? ""
              }`
            : error instanceof ReferenceError
              ? "ReferenceError"
              : "Git or binding error",
    }),
  }).pipe(
    Effect.catch((diagnostic) =>
      Effect.logWarning("Trip publication operation failed").pipe(
        Effect.annotateLogs({ operation: name, ...diagnostic }),
        Effect.andThen(Effect.fail(failed())),
      ),
    ),
  );

const identity = Schema.Struct({ tripId: TripId, revision: Revision });
const tripJson = Schema.fromJsonString(Schema.Struct({ version: Schema.Literal(1), trip: Trip }));

const encodeTrip = (trip: Trip) =>
  Schema.encodeSync(tripJson)({ version: 1, trip: { ...trip, published: null } });

/** Deterministic crash injection around the adapter's durable mutations. */
export const ArtifactsFailpoint = Context.Reference<{
  readonly hit: (
    point: "repo:before" | "repo:after" | "push:before" | "push:after",
  ) => Effect.Effect<void, PlannerError>;
}>("travel-planner/ArtifactsFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

// Git's HTTP port owns cancellation and byte limits; upstream's web client does
// not expose an AbortSignal. Redirects cannot forward repository credentials.
const transport = (signal: AbortSignal): HttpClient => ({
  request: async ({ url, method = "GET", headers, body }) => {
    const chunks: Uint8Array[] = [];
    let size = 0;

    if (body)
      for await (const chunk of body) {
        signal.throwIfAborted();
        size += chunk.byteLength;
        if (size > MAX_TRANSFER_BYTES) throw failed();
        chunks.push(chunk);
      }
    const bytes = new Uint8Array(size);
    let offset = 0;

    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    // workerd supports manual/follow, but rejects redirect: "error" before I/O.
    const request = { headers, signal, redirect: "manual" as const };

    const response = await (body
      ? fetch(url, { ...request, method: "POST", body: bytes })
      : fetch(url, { ...request, method }));

    if (response.status >= 300 && response.status < 400) {
      await response.body?.cancel();
      throw failed();
    }

    const reader = response.body?.getReader();
    const result: Uint8Array[] = [];

    size = 0;
    try {
      if (reader)
        while (true) {
          const next = await reader.read();

          if (next.done) break;
          size += next.value.byteLength;
          if (size > MAX_TRANSFER_BYTES) throw failed();
          result.push(next.value);
        }
    } finally {
      await reader?.cancel();
    }

    return {
      url: response.url,
      method,
      headers: Object.fromEntries(response.headers),
      statusCode: response.status,
      statusMessage: response.statusText,
      body: (async function* () {
        yield* result;
      })(),
    };
  },
});

const workspace = Effect.acquireRelease(
  Effect.sync(() => ({ ...memfs(), controller: new AbortController() })),
  ({ vol, controller }) =>
    Effect.sync(() => {
      controller.abort();
      vol.reset();
    }),
);

const releaseRepo = (repo: ArtifactsRepo | null) =>
  Effect.sync(() => {
    if (repo !== null && Symbol.dispose in repo) {
      const dispose = repo[Symbol.dispose];

      if (typeof dispose === "function") dispose.call(repo);
    }
  });

/**
 * One immutable, parentless branch per revision: a depth-one clone can always
 * recover any publication. Push never forces an existing ref; retries compare
 * schema-normalized trip content before accepting an existing commit. A failed
 * or interrupted push can have committed remotely, and is reconciled on retry.
 */
export const artifactsLayer = (
  artifacts: Artifacts,
  remoteBase: string,
): Layer.Layer<TripSiteStore> => {
  const getRepo = (tripId: string) =>
    Effect.acquireRelease(
      operation("get repository", async () => {
        try {
          return await artifacts.get(`trip-${tripId}`);
        } catch (error) {
          if (hasCode("NOT_FOUND")(error)) return null;
          throw error;
        }
      }),
      releaseRepo,
    );

  const ensureRepo = Effect.fn("Artifacts.ensureRepo")(function* (tripId: string) {
    const failpoint = yield* ArtifactsFailpoint;

    yield* failpoint.hit("repo:before");
    yield* operation("create repository", async () => {
      try {
        await artifacts.create(`trip-${tripId}`, { setDefaultBranch: "main" });
      } catch (error) {
        if (!hasCode("ALREADY_EXISTS")(error)) throw error;
      }
    });
    yield* failpoint.hit("repo:after");

    return yield* Effect.acquireRelease(
      operation("get created repository", () => artifacts.get(`trip-${tripId}`)),
      releaseRepo,
    );
  });

  const read = Effect.fn("Artifacts.read")(function* (
    repo: ArtifactsRepo,
    tripId: string,
    revision: number,
  ) {
    const { fs, controller } = yield* workspace;
    const token = yield* operation("create read token", () => repo.createToken("read", 60));
    const http = transport(controller.signal);

    const auth = {
      http,
      // Repo handles expose methods, not metadata. Alchemy supplies the account
      // and namespace URL using Artifacts' documented Git remote format.
      url: `${remoteBase}/trip-${tripId}.git`,
      headers: { Authorization: `Bearer ${token.plaintext}` },
    };

    const branch = `revision-${revision}`;

    const refs = yield* operation("list revision refs", () =>
      // Discovery mutates its headers to request protocol v2. Clone uses v1.
      git.listServerRefs({
        ...auth,
        headers: { ...auth.headers },
        prefix: `refs/heads/${branch}`,
      }),
    );

    const ref = refs.find((entry) => entry.ref === `refs/heads/${branch}`);

    if (!ref) return null;
    yield* operation("clone revision", () =>
      git.clone({
        ...auth,
        fs,
        dir: "/repo",
        ref: branch,
        depth: 1,
        singleBranch: true,
        noTags: true,
        noCheckout: true,
      }),
    );

    const commitId = yield* operation("resolve revision", () =>
      git.resolveRef({ fs, dir: "/repo", ref: "HEAD" }),
    );

    if (commitId !== ref.oid) return yield* conflict();

    const { blob } = yield* operation("read trip snapshot", () =>
      git.readBlob({ fs, dir: "/repo", oid: commitId, filepath: "trip.json" }),
    );

    if (blob.byteLength > MAX_FILE_BYTES) return yield* failed();

    const json = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(blob),
      catch: failed,
    });

    const { trip } = yield* Schema.decodeEffect(tripJson)(json).pipe(Effect.mapError(failed));

    if (trip.id !== tripId || trip.revision !== revision) return yield* failed();

    const { commit } = yield* operation("read revision commit", () =>
      git.readCommit({ fs, dir: "/repo", oid: commitId }),
    );

    const publishedAt = yield* Effect.try({
      try: () => DateTime.formatIso(DateTime.makeUnsafe(commit.committer.timestamp * 1000)),
      catch: failed,
    });

    const site: PublishedSite = {
      tripId,
      revision,
      commitId,
      publishedAt,
      path: `/trips/${tripId}/${revision}`,
    };

    const { blob: htmlBytes } = yield* operation("read trip page", () =>
      git.readBlob({ fs, dir: "/repo", oid: commitId, filepath: "index.html" }),
    );

    if (htmlBytes.byteLength > MAX_FILE_BYTES) return yield* failed();

    const html = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(htmlBytes),
      catch: failed,
    });

    return { trip, html, site };
  });

  const publish = Effect.fn("Artifacts.publish")(
    function* ({ trip: input }: { readonly trip: Trip }) {
      const trip = yield* Schema.decodeEffect(Trip)(input).pipe(Effect.mapError(failed));
      const json = encodeTrip(trip);
      const html = renderTripSite(trip);

      if (
        encoder.encode(json).byteLength > MAX_FILE_BYTES ||
        encoder.encode(html).byteLength > MAX_FILE_BYTES
      )
        return yield* failed();
      const repo = (yield* getRepo(trip.id)) ?? (yield* ensureRepo(trip.id));
      const previous = yield* read(repo, trip.id, trip.revision);

      if (previous) {
        if (encodeTrip(previous.trip) !== json) return yield* conflict();

        return previous.site;
      }
      const { fs, controller } = yield* workspace;
      const token = yield* operation("create write token", () => repo.createToken("write", 60));
      const dir = "/repo";
      const branch = `revision-${trip.revision}`;

      yield* operation("initialize revision", () => git.init({ fs, dir, defaultBranch: branch }));
      yield* operation("write trip files", async () => {
        await fs.promises.writeFile(`${dir}/trip.json`, json);
        await fs.promises.writeFile(`${dir}/index.html`, html);
        await git.add({ fs, dir, filepath: ["trip.json", "index.html"] });
      });
      const now = yield* DateTime.now;
      const timestamp = Math.floor(DateTime.toEpochMillis(now) / 1000);

      yield* operation("commit revision", () =>
        git.commit({
          fs,
          dir,
          message: `Publish trip revision ${trip.revision}`,
          author: {
            name: "Travel planner",
            email: "travel-planner@example.invalid",
            timestamp,
            timezoneOffset: 0,
          },
        }),
      );
      const failpoint = yield* ArtifactsFailpoint;

      yield* failpoint.hit("push:before");

      const pushed = yield* operation("push revision", () =>
        git.push({
          fs,
          dir,
          ref: branch,
          remoteRef: branch,
          force: false,
          url: `${remoteBase}/trip-${trip.id}.git`,
          http: transport(controller.signal),
          headers: { Authorization: `Bearer ${token.plaintext}` },
        }),
      ).pipe(Effect.result);

      if (pushed._tag === "Success") yield* failpoint.hit("push:after");
      // Re-read even after success: verify the exact remote commit and reconcile
      // races or a response lost after receive-pack committed the branch.
      const stored = yield* read(repo, trip.id, trip.revision);

      if (!stored) return yield* pushed._tag === "Failure" ? pushed.failure : failed();
      if (encodeTrip(stored.trip) !== json) return yield* conflict();

      return stored.site;
    },
    Effect.scoped,
    Effect.timeoutOrElse({ duration: "45 seconds", orElse: () => Effect.fail(failed()) }),
  );

  const load = Effect.fn("Artifacts.load")(
    function* (input: { readonly tripId: string; readonly revision: number }) {
      const { tripId, revision } = yield* Schema.decodeEffect(identity)(input).pipe(
        Effect.mapError(failed),
      );

      const repo = yield* getRepo(tripId);

      if (!repo) return null;

      const stored = yield* read(repo, tripId, revision);

      return stored === null ? null : { trip: stored.trip, html: stored.html };
    },
    Effect.scoped,
    Effect.timeoutOrElse({ duration: "45 seconds", orElse: () => Effect.fail(failed()) }),
  );

  return Layer.succeed(TripSiteStore, { publish, load });
};
