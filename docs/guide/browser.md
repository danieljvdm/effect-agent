---
title: Browser tools
description: Capture rendered pages, crawl Markdown, and run bounded interactive browser passes.
---

# Browser tools

Give an agent rendered page text, extract records, collect a site's Markdown, or let an operator
watch an interactive browser pass. Your application supplies Cloudflare bindings or credentials,
authorizes actions, and chooses the data its Tools return.

Start with a stateless capture for one page. Choose a crawl only when the task needs a bounded set
of same-host pages. Use an interactive pass only when navigation or page actions are essential.

| Need                                                                  | Choose                                | Where it runs                     | What the application provides                                      |
| --------------------------------------------------------------------- | ------------------------------------- | --------------------------------- | ------------------------------------------------------------------ |
| Render one URL as Markdown, scrape selectors, or take a PNG           | **Quick Actions**                     | A Cloudflare Worker               | A Browser Run binding                                              |
| Render Markdown, links, selector groups, or structured data from Node | **REST capture**                      | Any host with Effect `HttpClient` | Cloudflare account ID and API token                                |
| Crawl a site into bounded rendered Markdown records                   | **REST crawl**                        | Any host with Effect `HttpClient` | Account ID, API token, and a Scope                                 |
| Navigate, read, click, fill, scroll, or capture one active page       | **Interactive Browser**               | A Cloudflare Worker               | Browser binding, lifecycle token, and Puppeteer                    |
| Let an operator inspect or take over an active pass                   | **Interactive Browser host controls** | A trusted Cloudflare Worker host  | A Browser Run API token, kept private                              |
| Keep one page through approval, credentials, and human takeover       | **Browser Sessions**                  | A trusted Cloudflare Worker host  | Durable owner, current authority, browser binding, lifecycle token |

Browser output is untrusted input. Validate model-selected URLs against your host policy. Resolve
vault credentials in the host; keep provider handles, Live View URLs, and handoff identities out of
model Tools and agent journals.

In your application, install the browser adapters:

```sh
bun add @effect-agent/platform-cloudflare@beta
```

Requires `effect@^4.0.0-rc.116`. For the examples below, also install `effect-agent@beta`.
Keep framework packages at the [same release](./getting-started#installation-and-compatibility).
The REST examples need no Puppeteer dependency.

## Choose an adapter

### Quick Actions in a Worker

Quick Actions are best for a single render operation: Markdown, selector scrape, screenshot, and
the other Browser Run one-shot actions. A Worker binding authenticates the request without putting
an API token in the Worker. Configure the binding and use a compatibility date of `2026-03-24` or
newer. For local `wrangler dev`, Browser Run Quick Actions need remote mode.

```jsonc
{
  "compatibility_date": "2026-03-24",
  "browser": {
    "binding": "BROWSER",
    "remote": true,
  },
}
```

The `remote` setting is for local development. Deployments use the binding normally. Cloudflare
documents the binding, compatibility date, and remote-mode requirement in its
[Quick Actions guide](https://developers.cloudflare.com/browser-run/quick-actions/).

For a WebCapture Tool, use `CloudflareBrowser.layer(ReadPage, { browser: env.BROWSER })` as shown
below. For direct port access, provide
`BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })` to the adapter. Use
`browserQuickActionCaptureLayer` for `PageCapture` and
`browserQuickActionScreenshotLayer` for `PageScreenshot`. The capture adapter supports rendered
Markdown, links, selector scrape, and structured extraction. Structured extraction may invoke
Workers AI: authorize that separately and account for its provider cost before using it.

Quick Actions have no local implementation. Surface rate or quota failures and keep calls bounded.

### REST capture and crawl

The REST adapters run in Node or a Worker and need an account ID, a redacted API token with
**Browser Rendering - Edit** permission, and `FetchHttpClient.layer`. They are useful when the
browser work belongs in a Node service, job, or test harness rather than inside a Worker binding.

`browserRestCaptureLayer` implements `PageCapture`. It can capture rendered Markdown, links,
selector scrape, and extraction requests. `browserRestCrawlLayer` implements
`PageCrawl`: it starts the provider job, polls bounded pages, and cancels a known-running job when
the consuming Scope exits. The REST crawl adapter deliberately exposes only a credential-free HTTPS
starting URL and returns Markdown records from that start host.

Cloudflare's [Markdown endpoint](https://developers.cloudflare.com/browser-run/quick-actions/markdown-endpoint/)
accepts either a URL or HTML. `PageCaptureRequest` likewise accepts a `PageUrlTarget` or
`PageHtmlTarget`; authorize a URL target in your host before requesting it. Cloudflare's
[`/crawl` documentation](https://developers.cloudflare.com/browser-run/quick-actions/crawl-endpoint/)
explains how declared purposes interact with a target site's Content Signals policy. The framework
requires an explicit `purposes` array. Declare `ai-input` when feeding crawled content to a model;
use `search` when building a search index.

### Interactive Browser

An interactive pass owns one browser, context, and page for one Scope. It is for workflows that
need to inspect an active page, follow a known flow, or perform host-approved UI actions. It is not
a general browsing session and cannot become an agent Tool.

Install `@effect-agent/platform-cloudflare@beta`, `effect@^4.0.0-rc.116`, and `effect-cf@^0.44.1`.
The adapter includes its Puppeteer client. Then provide
`CloudflareInteractiveBrowser.layer({ browser: env.BROWSER, accountId, apiToken })` with
`FetchHttpClient.layer` for browser actions. `CloudflareInteractiveBrowser.hostLayer` opts into
trusted host controls for Live View and handoff. Both variants assemble the browser binding and
confirmed-session cleanup; the API token must be redacted. The lower-level binding, lifecycle,
and adapter Layers remain available for custom composition.

The policy is immutable when the pass opens:

- `ExactHosts` permits only a fixed set of HTTPS host authorities for page requests. It is a URL
  allowlist, not a public-network boundary.
- `PublicWeb` requires the adapter to enforce public-address containment at connection time. An
  adapter that cannot enforce it fails before opening a browser. Cloudflare rejects this policy
  with `InteractiveBrowserUnsupportedError` before acquisition.
- `Unrestricted` explicitly opts out of host and private-network containment while retaining the
  action, elapsed-time, and result-byte limits.

Choose `ExactHosts` for a known site. Let a trusted host, never model output, choose
`Unrestricted`. One policy also fixes maximum actions, elapsed time, and bytes returned by each
operation.

## Capture one rendered page from Node

This complete composition captures rendered Markdown through the Node-safe REST adapter. The
application owns the Cloudflare credentials and provides the `HttpClient`; the result stays in the
typed Effect channel.

```ts twoslash
import { browserRestCaptureLayer } from "@effect-agent/platform-cloudflare/browser-rest-capture";
import {
  CapturePageMarkdown,
  PageCapture,
  PageCaptureLimits,
  PageCaptureRequest,
  PageUrlTarget,
} from "effect-agent/page-capture";
import { Config, Effect } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const captureExample = Effect.gen(function* () {
  const accountId = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
  const apiToken = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");
  return yield* Effect.gen(function* () {
    const capture = yield* PageCapture;
    return yield* capture.capture(
      PageCaptureRequest.make({
        target: PageUrlTarget.make({ url: "https://example.com/" }),
        action: CapturePageMarkdown.make({}),
        engine: "kitesurf",
        limits: PageCaptureLimits.make({ maxOutputBytes: 16 * 1_024 }),
      }),
    );
  }).pipe(Effect.provide(browserRestCaptureLayer({ accountId, apiToken })));
}).pipe(Effect.provide(FetchHttpClient.layer));
```

`PageCaptureRequest` fixes the URL, operation, browser engine, and output limit before the request
starts. It can also carry a fixed resource policy, navigation options, and viewport. Capture
results have a discriminated output type; inspect it before using Markdown, links, scrape groups,
or structured data.

## Give an agent a capture Tool

Use `WebCapture` from `effect-agent` to wrap capture in a native Effect AI Tool. Fix the allowed
hosts, actions, and output size in the definition. In a Worker, the Cloudflare package assembles
the capture adapter, binding, and handlers in one Layer:

```ts twoslash
import { WebCapture } from "effect-agent";
import {
  CloudflareBrowser,
  type CloudflareBrowserOptions,
} from "@effect-agent/platform-cloudflare/cloudflare-browser";
import { Toolkit } from "effect/unstable/ai";

declare const env: { BROWSER: CloudflareBrowserOptions["browser"] };

const ReadPage = WebCapture.make("read_page", {
  description: "Read example.com as rendered Markdown.",
  urls: ["example.com"],
  actions: ["markdown"],
  maxResponseBytes: 16 * 1024,
});

export const BrowserTools = Toolkit.make(ReadPage.tool);
export const ReadPageLive = CloudflareBrowser.layer(ReadPage, {
  browser: env.BROWSER,
});
```

Use `BrowserTools` as the agent's toolkit and provide `ReadPageLive` when running it.
`CloudflareBrowser.layer` also accepts
`WebCapture.makeScrape` and `WebCapture.makeExtract` definitions. Extraction requires an explicit
`workersAi` option with an `authorizeAndAccount` Effect, using the same policy as
`BrowserQuickActionWorkersAi.layer`. Without it, extraction fails before making a browser request.
The constructor supplies only `PageCapture`; any schema decoding services remain required.
It preserves the definition's host policy, output bounds, typed failures, and response cleanup.

For REST capture, use the Node-safe REST subpath and supply an HTTP client:

```ts twoslash
import { WebCapture } from "effect-agent";
import {
  CloudflareBrowserRest,
  type CloudflareBrowserRestOptions,
} from "@effect-agent/platform-cloudflare/browser-rest-capture";
import { Layer } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

const readPage = WebCapture.make("read_page", {
  description: "Read example.com as rendered Markdown.",
  urls: ["example.com"],
  actions: ["markdown"],
  maxResponseBytes: 16 * 1024,
});

export const BrowserTools = Toolkit.make(readPage.tool);
export const browserToolsLive = (credentials: CloudflareBrowserRestOptions) =>
  CloudflareBrowserRest.layer(readPage, credentials).pipe(Layer.provide(FetchHttpClient.layer));
```

Use `BrowserTools` as the agent's toolkit and provide `browserToolsLive(credentials)` when running
it. Use `WebCapture.makeScrape` for grouped selector results or `WebCapture.makeExtract` for
Schema-validated extraction. Extraction also needs the adapter's explicit Workers AI authorization
and accounting policy. Capture Tools have uncertain external outcomes because page rendering can
execute JavaScript. Code Mode can expose them through its authorized Tool allowlist; their resource
policies still apply.

`CloudflareBrowserRest.layer` accepts the same optional `workersAi` policy as the Worker
constructor. It preserves schema decoding requirements and leaves `HttpClient` injectable.
For a custom capture adapter, provide its Layer directly to `readPage.handlers`.

## Capture and crawl {#capture-and-crawl}

### Crawl bounded same-host Markdown

`PageCrawl.crawl` returns a Stream. Consume it within `Effect.scoped` so interrupting the enclosing work
cancels the provider job when the adapter has a job identity to clean up.

```ts twoslash
import { browserRestCrawlLayer } from "@effect-agent/platform-cloudflare/browser-rest-crawl";
import { PageCrawl, PageCrawlLimits, PageCrawlRequest } from "effect-agent/page-crawl";
import { Config, Effect, Layer, Stream } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const BrowserCrawlLive = Layer.unwrap(
  Effect.gen(function* () {
    const accountId = yield* Config.NonEmptyString("CLOUDFLARE_ACCOUNT_ID");
    const apiToken = yield* Config.Redacted("CLOUDFLARE_API_TOKEN");
    return browserRestCrawlLayer({ accountId, apiToken });
  }),
).pipe(Layer.provide(FetchHttpClient.layer));

const crawlDocumentation = Effect.gen(function* () {
  const crawl = yield* PageCrawl;

  return yield* crawl
    .crawl(
      PageCrawlRequest.make({
        startUrl: "https://example.com/docs/",
        purposes: ["search"],
        limits: PageCrawlLimits.make({
          maxPages: 10,
          maxDepth: 2,
          maxPageBytes: 64 * 1_024,
          maxTotalBytes: 512 * 1_024,
          deadlineMillis: 120_000,
        }),
      }),
    )
    .pipe(Stream.runCollect);
}).pipe(Effect.scoped, Effect.provide(BrowserCrawlLive));
```

The Layer loads the real account ID and redacted token once from application configuration. The
operation keeps the crawl and its cleanup in one Scope.

Each record includes a URL, provider status, optional bounded Markdown, and optional origin
metadata. A non-completed status may have no Markdown. Treat a rate limit, protocol failure,
caller limit, or provider terminal status as a typed crawl failure. Do not turn it into an empty
successful crawl.

The framework caps requests at 100 pages, depth 10, 8 MiB per page, 64 MiB total, and a 10-minute
deadline. Keep limits lower for an agent request and declare the narrowest
`purposes` array. The provider's crawl job identity and pagination are private to the adapter.

## Capture a PNG

`PageScreenshot` is the stateless counterpart to an interactive screenshot. It returns exactly one
bounded `image/png` byte array, which the caller owns. Use the Quick Action screenshot layer in a
Worker; the REST capture adapter implements `PageCapture`, not `PageScreenshot`. Set the full-page
choice and byte limit in `PageScreenshotRequest`; do not persist image bytes in framework
thread records by default.

For a single known URL, use a stateless screenshot instead of opening an interactive session.
Choose an interactive screenshot only when it must reflect the page after navigation, filling,
clicking, or scrolling in that same pass.

## Interact with a browser {#interact-with-a-browser}

Open the browser inside `Effect.scoped`, then use the handle only inside that Scope. The handle
supports navigation, text reads, fill, click, screenshot, scroll, and early explicit close. Click
and fill require exactly one matching element. Action failures are typed; malformed selectors and
an undispatched provider action can be identified without treating them as a successful no-op.

```ts twoslash
// @types: @cloudflare/workers-types
import { CloudflareInteractiveBrowser } from "@effect-agent/platform-cloudflare/interactive-browser";
import {
  BrowserNavigateRequest,
  BrowserReadTextRequest,
  InteractiveBrowser,
  InteractiveBrowserPolicy,
} from "effect-agent/interactive-browser";
import { Effect, Layer, Redacted } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { WorkerEnvironment } from "effect-cf";

// In an application, Wrangler generates these binding types.
declare global {
  namespace Cloudflare {
    interface Env {
      readonly BROWSER: BrowserRun;
      readonly CLOUDFLARE_ACCOUNT_ID: string;
      readonly BROWSER_RENDERING_API_TOKEN: string;
    }
  }
}

const InteractiveLive = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    return CloudflareInteractiveBrowser.layer({
      browser: env.BROWSER,
      accountId: env.CLOUDFLARE_ACCOUNT_ID,
      apiToken: Redacted.make(env.BROWSER_RENDERING_API_TOKEN),
    }).pipe(Layer.provide(FetchHttpClient.layer));
  }),
);

export const readExampleDomain = Effect.gen(function* () {
  const browser = yield* InteractiveBrowser;
  const handle = yield* browser.open(
    InteractiveBrowserPolicy.make({
      network: { _tag: "ExactHosts", allowedHosts: ["example.com"] },
      maxActions: 3,
      maxElapsedMillis: 30_000,
      maxReturnedBytes: 16 * 1_024,
    }),
  );
  yield* handle.navigate(BrowserNavigateRequest.make({ url: "https://example.com/" }));
  return yield* handle.readText(BrowserReadTextRequest.make({}));
}).pipe(Effect.scoped);

export const program = readExampleDomain.pipe(Effect.provide(InteractiveLive));
```

`readExampleDomain` requires only `InteractiveBrowser`. `InteractiveLive` yields `WorkerEnvironment`
to construct the Cloudflare adapter, so the composed `program` retains `WorkerEnvironment` in `R`.
Run it inside an `effect-cf` Worker, which supplies that service. Tests can provide a different
`InteractiveBrowser` Layer to the same operation.

The adapter installs `BrowserRunSessionLifecycle` even for ordinary actions because every session
needs exact-session cleanup. The browser closes on Scope exit even after an interruption. Running
`handle.close` ends the pass early and invalidates that handle.

## Host Live View and handoff

`BrowserRunInteractiveHost` extends the regular pass with a short-lived redacted Live View URL,
handoff start, handoff state, and host-controlled close. Keep these operations in trusted Worker
code. Your application can expose these controls through an authenticated operator UI. Never
expose them to the model or store them in canonical threads.

The host layer requires `BrowserRunSessionLifecycle.layer({ accountId, apiToken })` and
`FetchHttpClient.layer` in addition to the browser binding. The lifecycle token permits exact-session
cleanup for every interactive pass; browser actions themselves use the Worker binding. Give a Live
View a short expiry and a handoff a finite timeout. Your application owns authentication, operator
authorization, and what happens after a handoff.

A live handle remains ephemeral. A trusted host can persist `session.checkpoint`, then call
`session.detach` to retain the provider page when its Scope closes. With exclusive ownership,
`host.resume(checkpoint, { pendingInput })` attaches only the recorded session, context, and page;
it preserves the original deadline and action budget, never creates a replacement page, and never
replays navigation or input. Store this private checkpoint separately from model-visible records.

Write a durable input receipt before dispatch. Include any unfinished receipt in `pendingInput`,
even when it was written after the checkpoint. Reconnection cannot prove old input stopped:
restart-unknown input permits reads and screenshots but blocks mutations, Live View, and handoff.
`session.drainInput` can clear a local running-input fence after the SDK settles; it cannot clear
restart or transport uncertainty. Human abandonment of a receipt does not stop SDK input. Only
confirmed exact-session closure ends an unprovable input fence.

Failures carry content-free execution evidence. `dispatch: "completed"` means SDK input completed
before observation failed; it does not prove website acceptance. Recoverable failures leave reads
usable. Keep durable unknown receipts independent of handle health and never replay uncertain input.
`BrowserRunPageObservation` decodes the existing JSON text observation. Its document and node IDs
can be passed as `expectedTarget` to click or fill; a replacement node is refused before dispatch.
Include its control `state` snapshot to also refuse changed checked, disabled, input-type, label, or
validity state on the same node. An optional `scopeSelector` must still resolve to one root
containing that node. Guarded click and fill validate and dispatch on the node in one page task;
guarded click uses native DOM click semantics rather than pointer coordinates.
Human handoff receipts remain host-owned; `getHandoffState` queries the reattached provider page.

## Keep a browser across attempts {#browser-sessions}

`BrowserSessions` keeps one native Cloudflare page under host ownership while scoped attachments
come and go. The same page can survive an approval wait, a correction, or human takeover.

```text
Host owner ── retains ──> Cloudflare browser
Attempt ── authorized attachment ──> same browser
Human   ── authorized Live View  ──> same browser
```

Import `BrowserSessions` from `@effect-agent/platform-cloudflare/browser-session`. Provide
`BrowserSessions.layer({ browser: env.BROWSER, accountId, apiToken })` and `FetchHttpClient.layer`.
The binding runs native browser commands; the private API token permits exact-session cleanup.

```ts twoslash
import {
  BrowserSessions,
  type BrowserSessionReference,
} from "@effect-agent/platform-cloudflare/browser-session";
import { Effect } from "effect";

declare const retain: (reference: BrowserSessionReference) => Effect.Effect<void>;
declare const authorize: Effect.Effect<void>;

const startBrowser = Effect.gen(function* () {
  const sessions = yield* BrowserSessions;
  const reference = yield* sessions.create({ maxElapsedMillis: 3_600_000 }, retain);

  return yield* Effect.gen(function* () {
    const session = yield* sessions.attach(reference);
    return yield* session.run(authorize, async (page) => {
      await page.goto("https://example.com/");
      return await page.title();
    });
  }).pipe(Effect.scoped);
});
```

`retain` commits the private reference to the application's existing durable owner. Creation
attempts exact-session cleanup if that commit fails. Keep references outside Tool results and agent journals.
The reference identifies the exact provider session, context, and page, with a fixed expiry.
Attachment never creates a replacement page.

The owner retains the reference and remains responsible for cleanup after an attachment's Scope
exits. Attachments disconnect locally; they do not transfer ownership or close the remote browser.
At task completion, cancellation, or expiry, the owner calls `sessions.close(reference.sessionId)`
and reconciles unconfirmed cleanup. An Attempt may supply current authority and borrow an
attachment through `attemptLayer`; its end does not require a browser checkpoint or handoff.

Every `session.run(authorize, action)` checks the supplied Effect before invoking native Puppeteer
code. Recheck the current controller and grants there. The application owns network restrictions,
bounded Tool results, and durable receipts for external actions. Native callbacks are trusted host
code: await every SDK operation and never accept model-provided JavaScript. A settled native SDK
rejection leaves the session available for inspection, with uncertain dispatch evidence. Inspect
and reconcile the page before deciding what to do next; never automatically replay that operation.
Unfinished commands interrupted by timeout or cancellation, uncertain credential writes, and
uncertain handoffs fence and terminate the session. Confirmed cleanup does not undo website effects.

For spectators, use `session.getReadOnlyLiveView(authorize, { mode: "tab", expiresInMs: 60_000 })`.
It uses the REST credential to mint a connection that blocks input, navigation, and JavaScript,
and fails unless Cloudflare confirms the read-only guardrail for the retained target. UI input
suppression does not secure an interactive URL. Expiry limits when a connection can start;
established connections last until the browser closes. Read-only viewers can still see page data.

For human control, fence agent dispatch in the owner, then use the attachment's `handoff`,
`getLiveView`, and `getHandoffState` methods with current operator authorization. Keep Live View
URLs private to the authorized recipient. Before returning to agent control, verify the recorded
handoff completed and inspect the current page under the new controller's authority.

Cloudflare may expire an idle session before the application's deadline. The owner's existing
alarm can call `sessions.keepAlive(reference.sessionId)` while the session remains authorized;
this neither extends the reference's expiry nor restores an expired browser. See
[session options](../reference/packages#browser-session-options) for bounds and defaults.

### Fill login or card credentials

Use `session.fillCredential(request)` on that same page. Import its schemas and
`BrowserCredentialAccess` from `@effect-agent/platform-cloudflare/browser-credentials`.
Each call requires current invocation authority: the host authorizes the actual top-page,
frame, and form-recipient origins and resolves redacted credential material from its vault.
Bind the invocation's caller and credential identifier to one vault item; repeated authorization
checks consult that item's current grants.

A `FillCredentialRequest` contains an opaque credential identifier, `kind`, an optional iframe
selector path, and explicit `{ selector, role }` fields. All selected fields must belong to one
native form. Use separate calls for separate forms or processor frames. The helper fills supported
native controls; it does not infer fields or submit the form. Authorize filling itself because
the site's input/change handlers may send data immediately. Submission remains an ordinary,
separately authorized browser action.

Credential material stays out of fill arguments, results, logs, and traces. The browser is allowed
to display it: subsequent native observations, screenshots, and page content follow the host's
ordinary disclosure policy. There is no protected observation mode or promise to scrub page echoes.

`CredentialFillResult.filled` counts acknowledged assignments. It proves neither authentication
nor payment acceptance. Inspect the site's result separately. An error's dispatch, filled count,
and cleanup retain partial-write and termination evidence; an uncertain fill must not be retried
automatically. Confirmed cleanup does not undo website effects.

The [runnable Worker proof](https://github.com/danieljvdm/effect-agent/blob/main/tooling/browser-run-worker-proof/src/credentials.ts)
uses dummy credentials, continues with ordinary observations, and reattaches the same session.

### Replace the removed Protected Browser API

The `/protected-browser` APIs and transfer checkpoints have been removed. Move browser ownership
to the host and use `/browser-session` plus `/browser-credentials`. Existing protected checkpoints
are not new session references: close or reconcile their provider sessions through the owning
application, then create a fresh session. Preserve existing operation and cleanup evidence.

## Limits, cleanup, and network boundaries

Browser APIs use finite requests and typed expected failures:

- `PageCapture` fixes one output-byte limit. Navigation, rate, protocol, unsupported-operation, and
  output-limit failures remain typed.
- `PageCrawl` fixes the start host, purposes, page/depth/byte/deadline limits, and cancellation
  lifecycle. Its stream ends only after the provider reports a terminal result or a typed failure.
- `PageScreenshot` accepts only PNG and enforces a caller-selected byte limit.
- An interactive policy fixes network mode, at most 1,000 actions, a caller-selected positive safe
  integer elapsed allowance in milliseconds, and at most 8 MiB from one result. Handles expire at
  policy limits or explicit close.

Quick Action failures retain bounded response text and Browser Run API status, selected request
identifiers, and body truncation metadata in their host-only `cause`. That status describes the
Browser Run API response, not necessarily the destination page. Applications can explicitly
redact and retain these causes for operator diagnostics; they are not automatically exposed to
models or logged.

Recognized Browser Run navigation timeouts report the provider's elapsed limit in the public
`PageCaptureNavigationError` message. An API HTTP 422 is not the destination's status and does
not establish that the destination blocked the request. Unknown provider text stays private.

For JavaScript-rendered pages, choose a content-specific `waitForSelector` with a finite timeout
alongside the navigation timeout. `domcontentloaded` alone can capture a navigation shell, and
a heading alone may precede the content being researched. Inspect the returned evidence before
treating the pass as useful; missing amenities are not evidence of their absence. See Cloudflare's
[Markdown endpoint](https://developers.cloudflare.com/browser-run/quick-actions/markdown-endpoint/)
and [independent timeout controls](https://developers.cloudflare.com/browser-run/reference/timeouts/).

Quick Action response readers are canceled and unlocked on local interruption, including an
outer Effect timeout. The native `quickAction()` binding exposes neither an abort signal nor a
session handle: interrupting an unresolved RPC stops local waiting but does not confirm remote
browser termination. Provider navigation/readiness limits remain important. The adapter does not
retry that RPC. Tests with a scripted binding establish local waiting and reader cleanup only;
hosted provider lifecycle behavior requires separate live evidence.

These caps do not authorize the destination, protect every network path, or make provider actions
replay-safe. Keep an application allowlist for stateless capture; choose the interactive network
policy that matches the actual isolation guarantee; and treat all rendered data as untrusted.

`isBrowserRunUndispatchedActionError` identifies selector failures before dispatch. Callers can
correct those selectors. Other action failures invalidate the handle; never retry a mutation
whose outcome is unknown. Interruption cannot reliably cancel an action already sent to Puppeteer.

`readText().text` contains JSON with page text, selector counts, and at most 64 controls.
Control diagnostics omit field values and HTML. Results, including PNG screenshots, obey the
pass byte limit. Logs omit URLs, selectors, labels, field values, credentials, and provider errors.

`selectFile(BrowserSelectFileRequest.make({ selector, target: "input", fileName, mediaType, bytes }))`
selects up to 8 MiB of host-owned bytes without a browser filesystem path. Use `target: "chooser"`
for a button that creates or opens a file input. The result confirms selection only; inspect the
website's receipt separately to establish upload or submission. Change handlers may send bytes
immediately, so authorize the destination before selection and never replay an unknown outcome.

Set the initial `viewport` on `BrowserRunInteractiveBinding.layer` or use the host session's
`resizeViewport`. Width and height accept integers in `1..2048`; `deviceScaleFactor` accepts
`1..2`, defaults to `1`, and must satisfy `max(width, height) * deviceScaleFactor <= 2048`.
Mobile, touch, and orientation options are unsupported. Resizing consumes no agent action but
remains subject to the pass deadline and lock. Authorize viewport changes in your host.

For `BrowserRunInteractiveHost`, call `host.acquire(policy)`, persist the returned private `sessionId`,
then run `acquisition.connect`. The acquisition owns the browser in its original Scope even
if connection or page setup fails; connection is attempted at most once. `host.open(policy)`
combines these steps for callers that do not need a persistence boundary. Acquisition failures
without an identity remain indeterminate unless the provider conclusively refused allocation.

Install an Effect `ErrorReporter` in the invocation runtime to capture recovered browser
and cleanup failures. Adapters report only source-authored stages, failure categories, and HTTP
statuses, including work that settles after interruption. Public errors retain dispatch and cleanup
evidence without provider text or session capabilities. In-process public projections carry
`ErrorReporter.ignore`; custom recovery/reporting hooks must honor it to avoid duplicate captures.
Do not serialize that marker as a cross-process diagnostic receipt.

Session closure waits up to ten seconds to confirm whole-browser termination or exact-session
absence. A pending close or transport/authentication failure is not proof of cleanup.
`BrowserRunCleanupError` reports a sanitized reason. Correct authorization or configuration
failures before retrying. The [interactive browser API comments](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/src/interactive-browser.ts)
describe action timing and lifecycle details.

## Hosted browser and checkout proof

The repository includes an [opt-in temporary deployment proof](https://github.com/danieljvdm/effect-agent/tree/main/tooling/browser-run-worker-proof).
It exercises the hosted Browser Run binding with Markdown capture, selector scrape, PNG screenshot,
an interactive pass, credentials and uploads. A real buyer then discovers a multi-step store,
uses cross-origin card fields or an accelerated wallet, pauses for approval, and resumes the same
browser through a consumer-owned Durable Object. The server checks the exact purchase and every
payment attempt, including declines and uncertain confirmations. An operator profile adds human
takeover and return. Alchemy owns deployment and teardown; reports retain unsuccessful attempts.
It needs Cloudflare and model credentials. Its README documents the command, recovery, deliberate
CI policy, and the distinction between simulated checkout behavior and actual provider compatibility.

## Next steps

- [Tools & layers](./tools) explains how browser services become bounded Effect AI Tools.
- [Cloudflare](../platforms/cloudflare) covers Durable Object agent hosts.
- [Operations](./operations#authorization-and-isolation) covers host authorization and isolation.
