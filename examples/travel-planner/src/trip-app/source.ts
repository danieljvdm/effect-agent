import { Clock, Context, Effect, Layer, Schema } from "effect";
import git, { type HttpClient, type TreeEntry } from "isomorphic-git";
import { memfs } from "memfs";

import { AppCommit, AppFile, PlannerError } from "../domain.ts";
import { TripFailpoint } from "../server/trips.ts";

export class AppSourceStore extends Context.Service<
  AppSourceStore,
  {
    readonly fork: (input: {
      readonly repoName: string;
      readonly files: ReadonlyArray<AppFile>;
    }) => Effect.Effect<{ readonly commitId: string }, PlannerError>;
    readonly read: (input: {
      readonly repoName: string;
      readonly commitId: string;
    }) => Effect.Effect<ReadonlyArray<AppFile>, PlannerError>;
    readonly commit: (input: {
      readonly repoName: string;
      readonly parentCommit: string;
      readonly files: ReadonlyArray<AppFile>;
      readonly message: string;
    }) => Effect.Effect<{ readonly commitId: string }, PlannerError>;
  }
>()("travel-planner/trip-app/AppSourceStore") {}

const TEMPLATE = "trip-app-template-v1";
const MAX_SOURCE = 2 * 1024 * 1024;
const MAX_TRANSFER = 8 * 1024 * 1024;
const encoder = new TextEncoder();
const RepoName = Schema.String.check(Schema.isPattern(/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,239}$/));

const SafePath = AppFile.fields.path.check(
  Schema.makeFilter((path) =>
    path
      .split("/")
      .every(
        (part) => part !== "" && ![".git", "node_modules", "dist"].includes(part.toLowerCase()),
      ),
  ),
);

const Files = Schema.Array(
  Schema.Struct({ path: SafePath, content: AppFile.fields.content }),
).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(100),
  Schema.makeFilter((files) => {
    let bytes = 0;
    const paths = new Set<string>();

    for (const file of files) {
      const size = encoder.encode(file.content).byteLength;

      if (size > 128 * 1024 || paths.has(file.path)) return false;
      paths.add(file.path);
      bytes += size;
      if (bytes > MAX_SOURCE) return false;
    }

    return files.every(({ path }) =>
      path
        .split("/")
        .slice(0, -1)
        .every((_, index, parts) => !paths.has(parts.slice(0, index + 1).join("/"))),
    );
  }),
);

const failed = () =>
  new PlannerError({
    code: "storage",
    message: "Trip app source storage is unavailable or contains invalid data.",
  });

const invalid = () =>
  new PlannerError({
    code: "invalid",
    message: "Invalid trip app source or source limits exceeded.",
  });

const conflict = () =>
  new PlannerError({
    code: "conflict",
    message: "Trip app source changed. Read the current version before retrying.",
  });

const hasCode = (code: string) => Schema.is(Schema.Struct({ code: Schema.Literal(code) }));

const operation = <A>(run: (signal: AbortSignal) => Promise<A>) =>
  Effect.tryPromise({ try: run, catch: failed });

const normalize = Effect.fn("AppSource.normalize")(function* (files: ReadonlyArray<AppFile>) {
  return (yield* Schema.decodeEffect(Files)(files).pipe(Effect.mapError(invalid))).toSorted(
    (a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0),
  );
});

const sameFiles = (a: ReadonlyArray<AppFile>, b: ReadonlyArray<AppFile>) =>
  JSON.stringify(a) === JSON.stringify(b);

// A transport belongs to one operation. Count aggregate request + response bytes;
// redirects never receive repository credentials and cancellation closes body readers.
const transport = (signal: AbortSignal): HttpClient => {
  let remaining = MAX_TRANSFER;

  const count = (size: number) => {
    remaining -= size;
    if (remaining < 0) throw failed();
  };

  return {
    request: async ({ url, method = "GET", headers, body }) => {
      const chunks: Uint8Array[] = [];
      let length = 0;

      if (body)
        for await (const chunk of body) {
          signal.throwIfAborted();
          count(chunk.byteLength);
          length += chunk.byteLength;
          chunks.push(chunk);
        }
      const bytes = new Uint8Array(length);
      let offset = 0;

      for (const chunk of chunks) {
        bytes.set(chunk, offset);
        offset += chunk.byteLength;
      }

      const response = await fetch(url, {
        method,
        headers,
        signal,
        redirect: "manual",
        ...(body ? { body: bytes } : {}),
      });

      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel();
        throw failed();
      }
      const reader = response.body?.getReader();
      const result: Uint8Array[] = [];

      try {
        if (reader)
          while (true) {
            const next = await reader.read();

            if (next.done) break;
            count(next.value.byteLength);
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
  };
};

const workspace = Effect.acquireRelease(
  Effect.sync(() => {
    const controller = new AbortController();

    return { ...memfs(), controller, http: transport(controller.signal) };
  }),
  ({ vol, controller }) =>
    Effect.sync(() => {
      controller.abort();
      vol.reset();
    }),
);

const disposeRepo = (repo: ArtifactsRepo | null) => {
  if (repo !== null && Symbol.dispose in repo) {
    const dispose = repo[Symbol.dispose];

    if (typeof dispose === "function") dispose.call(repo);
  }
};

const bounded = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(
    Effect.scoped,
    Effect.timeoutOrElse({ duration: "45 seconds", orElse: () => Effect.fail(failed()) }),
  );

/** Fixed native template forks, parentful main commits, no forced ref updates.
 * An uncertain push is reconciled against the remote tree and immediate parent.
 * All Git filesystem state, credentials, and repository handles belong to the operation scope.
 */
export const appSourceLayer = (
  artifacts: Artifacts,
  remoteBase: string,
): Layer.Layer<AppSourceStore> => {
  const base = Schema.String.check(
    Schema.makeFilter((value) => {
      try {
        const url = new URL(value);

        return (
          url.protocol === "https:" && !url.username && !url.password && !url.search && !url.hash
        );
      } catch {
        return false;
      }
    }),
  );

  const getRepo = (name: string) =>
    Effect.acquireRelease(
      operation(async (signal) => {
        let repo: ArtifactsRepo;

        try {
          repo = await artifacts.get(name);
        } catch (error) {
          if (hasCode("NOT_FOUND")(error)) return null;
          throw error;
        }
        if (signal.aborted) {
          disposeRepo(repo);
          signal.throwIfAborted();
        }

        return repo;
      }),
      (repo) => Effect.sync(() => disposeRepo(repo)),
      { interruptible: true },
    );

  const discardCreationToken = (repoName: string, token: string) =>
    Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const repo = yield* getRepo(repoName);

        if (repo !== null) yield* operation(() => repo.revokeToken(token));
      }).pipe(
        Effect.scoped,
        Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.void }),
        Effect.catch(() => Effect.void),
      ),
    );

  const session = Effect.fn("AppSource.session")(function* (
    repo: ArtifactsRepo,
    repoName: string,
    scope: "read" | "write",
  ) {
    const remote = yield* Schema.decodeEffect(base)(remoteBase).pipe(Effect.mapError(invalid));

    const work = yield* workspace;
    const token = yield* operation(() => repo.createToken(scope, 60));

    yield* Effect.addFinalizer(() =>
      operation(() => repo.revokeToken(token.id)).pipe(
        Effect.timeoutOrElse({ duration: "1 second", orElse: () => Effect.void }),
        Effect.catch(() => Effect.void),
        Effect.asVoid,
      ),
    );

    return {
      ...work,
      dir: "/repo",
      url: `${remote.replace(/\/$/, "")}/${repoName}.git`,
      headers: { Authorization: `Bearer ${token.plaintext}` },
    };
  });

  type Session = Effect.Success<ReturnType<typeof session>>;

  const main = (s: Session) =>
    operation(async () => {
      const refs = await git.listServerRefs({
        http: s.http,
        url: s.url,
        headers: { ...s.headers },
        prefix: "refs/heads/main",
      });

      return refs.find((entry) => entry.ref === "refs/heads/main")?.oid ?? null;
    });

  const fetchCommit = (s: Session, oid: string) =>
    operation(async () => {
      await git.init({ fs: s.fs, dir: s.dir, defaultBranch: "main" });
      await git.addRemote({ fs: s.fs, dir: s.dir, remote: "origin", url: s.url, force: true });
      await git.fetch({
        fs: s.fs,
        dir: s.dir,
        http: s.http,
        url: s.url,
        headers: { ...s.headers },
        ref: oid,
        remoteRef: oid,
        depth: 1,
        singleBranch: true,
        tags: false,
      });
    });

  const treeFiles = (s: Session, oid: string) =>
    operation(async () => {
      const files: AppFile[] = [];
      let bytes = 0;
      let entries = 0;

      const walk = async (tree: string, prefix: string): Promise<void> => {
        const result = await git.readTree({ fs: s.fs, dir: s.dir, oid: tree });

        for (const entry of result.tree) {
          // Every admitted file has at most 240 path characters. Bound even a
          // hostile tree full of empty directories without excluding valid paths.
          if (++entries > 100 * 240) throw failed();
          const path = prefix + entry.path;

          if (!Schema.is(SafePath)(path)) throw failed();
          if (entry.type === "tree") await walk(entry.oid, `${path}/`);
          else {
            if (
              entry.type !== "blob" ||
              !["100644", "100755"].includes(entry.mode) ||
              files.length >= 100
            )
              throw failed();
            const { blob } = await git.readBlob({ fs: s.fs, dir: s.dir, oid: entry.oid });

            bytes += blob.byteLength;
            if (blob.byteLength > 128 * 1024 || bytes > MAX_SOURCE) throw failed();
            files.push({ path, content: new TextDecoder("utf-8", { fatal: true }).decode(blob) });
          }
        }
      };

      await walk(oid, "");

      return files;
    }).pipe(Effect.flatMap(normalize), Effect.mapError(failed));

  const loadMain = Effect.fn("AppSource.loadMain")(function* (s: Session) {
    const commitId = yield* main(s);

    if (commitId === null) return null;
    yield* fetchCommit(s, commitId);
    const files = yield* treeFiles(s, commitId);

    const { commit } = yield* operation(() =>
      git.readCommit({ fs: s.fs, dir: s.dir, oid: commitId }),
    );

    return { commitId, files, parent: commit.parent };
  });

  const write = Effect.fn("AppSource.write")(function* (
    s: Session,
    files: ReadonlyArray<AppFile>,
    parent: string | null,
    message: string,
  ) {
    const timestamp = parent === null ? 0 : Math.floor((yield* Clock.currentTimeMillis) / 1000);

    return yield* operation(async () => {
      await git.init({ fs: s.fs, dir: s.dir, defaultBranch: "main" });

      const writeTree = async (prefix: string): Promise<string> => {
        const tree: TreeEntry[] = [];
        const directories = new Set<string>();

        for (const file of files) {
          if (!file.path.startsWith(prefix)) continue;
          const name = file.path.slice(prefix.length);
          const slash = name.indexOf("/");

          if (slash >= 0) directories.add(name.slice(0, slash));
          else
            tree.push({
              mode: "100644",
              path: name,
              type: "blob",
              oid: await git.writeBlob({
                fs: s.fs,
                dir: s.dir,
                blob: encoder.encode(file.content),
              }),
            });
        }
        for (const path of directories)
          tree.push({
            mode: "040000",
            path,
            type: "tree",
            oid: await writeTree(`${prefix}${path}/`),
          });

        return git.writeTree({ fs: s.fs, dir: s.dir, tree });
      };

      const tree = await writeTree("");

      return git.commit({
        fs: s.fs,
        dir: s.dir,
        tree,
        parent: parent === null ? [] : [parent],
        message,
        author: {
          name: "Travel planner",
          email: "travel-planner@example.invalid",
          timestamp,
          timezoneOffset: 0,
        },
      });
    });
  });

  const push = Effect.fn("AppSource.push")(function* (s: Session) {
    const failpoint = yield* TripFailpoint;

    yield* failpoint.hit("app-source:push:before");
    yield* operation(async () => {
      const result = await git.push({
        fs: s.fs,
        dir: s.dir,
        http: s.http,
        url: s.url,
        headers: { ...s.headers },
        ref: "main",
        remoteRef: "main",
        force: false,
      });

      if (!result.ok) throw failed();
    });
    yield* failpoint.hit("app-source:push:after");
  });

  const fork = Effect.fn("AppSource.fork")(function* (input: {
    readonly repoName: string;
    readonly files: ReadonlyArray<AppFile>;
  }) {
    const repoName = yield* Schema.decodeEffect(RepoName)(input.repoName).pipe(
      Effect.mapError(invalid),
    );

    if (repoName === TEMPLATE) return yield* invalid();
    const files = yield* normalize(input.files);

    yield* Schema.decodeEffect(base)(remoteBase).pipe(Effect.mapError(invalid));
    const failpoint = yield* TripFailpoint;
    let template = yield* getRepo(TEMPLATE);

    if (template === null) {
      yield* failpoint.hit("app-source:repo:before");

      const created = yield* operation(async () => {
        try {
          return await artifacts.create(TEMPLATE, { setDefaultBranch: "main" });
        } catch (error) {
          if (!hasCode("ALREADY_EXISTS")(error)) throw error;

          return null;
        }
      });

      if (created !== null) yield* discardCreationToken(TEMPLATE, created.token);
      yield* failpoint.hit("app-source:repo:after");
      template = yield* getRepo(TEMPLATE);
    }
    if (template === null) return yield* failed();
    const templateSession = yield* session(template, TEMPLATE, "write");
    let seed = yield* loadMain(templateSession);

    if (seed === null) {
      yield* write(templateSession, files, null, TEMPLATE);
      const pushed = yield* push(templateSession).pipe(Effect.result);

      seed = yield* loadMain(templateSession);
      if (seed === null) return yield* pushed._tag === "Failure" ? pushed.failure : failed();
    }
    if (seed.parent.length !== 0 || !sameFiles(seed.files, files)) return yield* conflict();
    let target = yield* getRepo(repoName);

    if (target === null) {
      yield* failpoint.hit("app-source:fork:before");

      const forked = yield* operation(async () => {
        try {
          return await template.fork(repoName, { defaultBranchOnly: true });
        } catch (error) {
          if (!hasCode("ALREADY_EXISTS")(error)) throw error;

          return null;
        }
      }).pipe(Effect.result);

      if (forked._tag === "Success") {
        if (forked.success !== null) yield* discardCreationToken(repoName, forked.success.token);
        yield* failpoint.hit("app-source:fork:after");
      }
      target = yield* getRepo(repoName);
      if (target === null) return yield* forked._tag === "Failure" ? forked.failure : failed();
    }
    const stored = yield* loadMain(yield* session(target, repoName, "read"));

    if (stored === null) return yield* failed();
    if (stored.commitId !== seed.commitId || !sameFiles(stored.files, files))
      return yield* conflict();

    return { commitId: stored.commitId };
  }, bounded);

  const read = Effect.fn("AppSource.read")(function* (input: {
    readonly repoName: string;
    readonly commitId: string;
  }) {
    const repoName = yield* Schema.decodeEffect(RepoName)(input.repoName).pipe(
      Effect.mapError(invalid),
    );

    const commitId = yield* Schema.decodeEffect(AppCommit)(input.commitId).pipe(
      Effect.mapError(invalid),
    );

    const repo = yield* getRepo(repoName);

    if (repo === null)
      return yield* new PlannerError({ code: "not-found", message: "Trip app source not found." });
    const s = yield* session(repo, repoName, "read");

    yield* fetchCommit(s, commitId);

    return yield* treeFiles(s, commitId);
  }, bounded);

  const commit = Effect.fn("AppSource.commit")(function* (input: {
    readonly repoName: string;
    readonly parentCommit: string;
    readonly files: ReadonlyArray<AppFile>;
    readonly message: string;
  }) {
    const repoName = yield* Schema.decodeEffect(RepoName)(input.repoName).pipe(
      Effect.mapError(invalid),
    );

    if (repoName === TEMPLATE) return yield* invalid();

    const parent = yield* Schema.decodeEffect(AppCommit)(input.parentCommit).pipe(
      Effect.mapError(invalid),
    );

    const message = yield* Schema.decodeEffect(
      Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(240)),
    )(input.message).pipe(Effect.mapError(invalid));

    const files = yield* normalize(input.files);
    const repo = yield* getRepo(repoName);

    if (repo === null)
      return yield* new PlannerError({ code: "not-found", message: "Trip app source not found." });
    const s = yield* session(repo, repoName, "write");
    const previous = yield* loadMain(s);

    if (previous === null) return yield* conflict();
    if (previous.commitId !== parent) {
      if (
        previous.parent.length === 1 &&
        previous.parent[0] === parent &&
        sameFiles(previous.files, files)
      )
        return { commitId: previous.commitId };

      return yield* conflict();
    }
    if (sameFiles(previous.files, files)) return { commitId: parent };
    yield* write(s, files, parent, message);
    const pushed = yield* push(s).pipe(Effect.result);
    const stored = yield* loadMain(s);

    if (stored === null) return yield* pushed._tag === "Failure" ? pushed.failure : failed();
    if (pushed._tag === "Failure" && stored.commitId === parent) return yield* pushed.failure;
    if (
      stored.parent.length !== 1 ||
      stored.parent[0] !== parent ||
      !sameFiles(stored.files, files)
    )
      return yield* conflict();

    return { commitId: stored.commitId };
  }, bounded);

  return Layer.succeed(AppSourceStore, { fork, read, commit });
};
