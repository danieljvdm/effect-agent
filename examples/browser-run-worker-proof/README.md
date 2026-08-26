# Browser Run deployed Worker proof

This private class E example proves the shipped Cloudflare Browser Run binding path against one
temporary deployed Worker. The Worker resolves `env.BROWSER` through
`BrowserQuickActionBrowserBinding.layer`, provides `browserQuickActionCaptureLayer`, and invokes a
`WebCapture.make` handler directly. It captures `https://example.com/` as bounded Markdown and
returns only the stable `Example Domain` fact. It uses no language model or Workers AI extraction.

The live workflow generates a fresh Worker name, rejects an existing name, deploys through the
example's direct Wrangler dependency, invokes the Worker once with a 60-second timeout, and deletes
the deployment when its Scope closes. It never retries an unresolved invocation. The deletion
finalizer is registered only after Wrangler confirms deployment, and a deletion failure fails the
task instead of being logged and ignored.

Set these values before opting in:

- `CLOUDFLARE_ACCOUNT_ID`: the 32-character account ID;
- `CLOUDFLARE_API_TOKEN`: a token that can read and edit Workers Scripts, held as an Effect
  `Redacted` value;
- `CLOUDFLARE_WORKERS_SUBDOMAIN`: the account's workers.dev subdomain without `.workers.dev`.

Run the proof from the repository root:

```sh
vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

Ordinary checks and the scripted finalizer test need no Cloudflare credentials and never deploy a
Worker.
