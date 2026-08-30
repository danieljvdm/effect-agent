# Browser Run deployed Worker proof

This example declares `@cloudflare/puppeteer` directly because it imports
`@effect-agent/platform-cloudflare/interactive-browser`. Durable-host and REST-only consumers
do not need that optional peer dependency.

This private class E example proves the shipped Cloudflare Browser Run binding path against one
temporary deployed Worker. The Worker resolves `env.BROWSER` through
`BrowserQuickActionBrowserBinding.layer`, provides `browserQuickActionCaptureLayer`, and invokes a
`WebCapture.make` handler directly. It captures `https://example.com/` as bounded Markdown and
keeps the stable `Example Domain` fact. It then invokes `WebCapture.makeScrape` with the `h1` and
`a` selectors, validates the grouped heading result, and discards the rendered records. During the
same Worker invocation, it also provides
`browserQuickActionScreenshotLayer`, captures a bounded PNG through `PageScreenshot`, validates its
media type and exact eight-byte PNG signature, and discards the bytes. The response contains only
the Markdown fact and Schema-defined validation metadata. The Worker leaves 11 seconds between
each of the three Quick Actions so the proof respects the Free plan request interval. Finally,
`BrowserRunInteractiveHost` launches a scoped Chromium pass through the same real `BROWSER`
binding. It navigates to the fixed `example.com` host, validates the `Example Domain` fact within
4 KiB of text, scrolls the viewport, and captures a PNG from that same page. The immutable policy
permits one page, seven operations, 256 KiB per result, and 90 seconds of elapsed time.

The host session also creates a one-minute tab Live View, starts a five-second handoff, and checks
that the provider reports the same active handoff identity. The proof does not open the Live View
or wait for human input. It explicitly closes the session and verifies that the old handle rejects
further actions. Scope still owns cleanup on every exit. The Worker discards the text, image bytes,
session identity, handoff identity, Live View URL, and handles before returning bounded validation
metadata. It uses no language model or Workers AI extraction. The scroll check proves the command
was accepted on the current page; the short Example Domain page may not have vertical overflow.

The live workflow generates a fresh Worker name, rejects an existing name, deploys through the
example's direct Wrangler dependency, provisions its narrow Browser Rendering token, waits 15 seconds for route propagation, invokes the Worker
once, and deletes the deployment when its Scope closes. The propagation wait and invocation share
a 150-second timeout. It never retries an unresolved invocation. The deletion
finalizer is registered only after Wrangler confirms deployment, and a deletion failure fails the
task instead of being logged and ignored.
The deployment credential stays in the local workflow. Only the account ID and the narrow browser
token enter the temporary Worker; deleting the Worker also removes that secret. Remote browser
cleanup must confirm exact-session termination before the proof succeeds.

Set these values before opting in:

- `CLOUDFLARE_ACCOUNT_ID`: the 32-character account ID;
- `CLOUDFLARE_API_TOKEN`: a token that can read and edit Workers Scripts, held as an Effect
  `Redacted` value;
- `BROWSER_RENDERING_API_TOKEN`: an account-scoped Browser Rendering Write token, uploaded only
  to the temporary Worker after its deletion finalizer is registered;
- `CLOUDFLARE_WORKERS_SUBDOMAIN`: the account's workers.dev subdomain without `.workers.dev`.

Run the proof from the repository root:

```sh
vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

Ordinary checks and the scripted finalizer test need no Cloudflare credentials and never deploy a
Worker.
