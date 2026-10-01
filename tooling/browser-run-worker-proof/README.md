# Hosted checkout proof

One ephemeral Worker serves a controlled store and a real model-driven buyer. Its
`CheckoutRun` Durable Object owns the one-shot dispatch fence, private browser session,
payment-attempt count, and receipt. The host uses BrowserUse over
CloudflareInteractiveBrowser; the browser may reach only the fixture origin.

```text
Alchemy Test → Worker + CheckoutRun → Cloudflare Browser Run
  /run once → /shop/login → /shop/product → /shop/review
             → POST /shop/pay (commits, returns 503) → /shop/orders
  independent GET /evidence → POST /close → GET /evidence → destroy
```

The designated buyer is `buyer@example.test`. This store never contacts a payment
processor or charges money. Model and Cloudflare usage still incur provider costs.
Passing this proof establishes the controlled checkout behavior; it does not establish
Stripe, Shop Pay, real payment, or human-takeover compatibility.

## What it checks

The buyer chooses one blue, medium Everyday Shirt and standard shipping. The runner
independently checks every receipt field, including the designated buyer, delivery to
123 Test Street, San Francisco, CA 94107, US, quantity one, subtotal 3400 cents,
shipping 500 cents, tax 312 cents, total 4212 cents, USD, and paid status.

The payment endpoint deliberately returns an ambiguous 503 after committing the receipt.
The terminal submission tool submits once and reads browser order history; ordinary
browser actions cannot submit payment. The runner requires exactly one payment attempt
and an independently fetched matching receipt. A model completion message or an HTTP
success alone cannot pass the gate.

Before dispatch, the runner waits for authenticated evidence from a pristine
Durable Object, retrying fresh-route 404 and transient 5xx/transport failures every
two seconds for up to one minute.
Authentication failures, invalid evidence, and nonpristine owners fail immediately.

The browser then reloads the initial login GET until it observes the email, password,
and Sign in controls, every two seconds for up to one minute. Model execution and
credential input begin only after that check. The fixture deliberately serves a 503
on its first login GET; independent evidence must show a subsequent login request.
This covers startup error pages that browser navigation accepts as completed loads.

A read-only scrape of inline fixture HTML runs before browser creation, avoiding
propagation of a new public route to the Quick Actions browser. Transient 5xx/reset
failures of that preflight may retry, at most twice. Read-only browser observations
also retry an attached-session read failure twice, using public execution evidence.
The runner never retries `POST /run`
or a purchase. Lost responses remain unresolved, and the Durable Object's persisted
started fence refuses a replacement run. Closure persists a stop fence before
acknowledgement so a suspended preflight cannot allocate a browser afterward.

The host injects `CHECKOUT_PASSWORD` through guarded credential input. It is a
redacted Worker binding, not model input. This is host-owned credential injection,
not coverage of the BrowserCredentialAccess helper. The separate control token
authorizes `/run`, `/evidence`, and `/close`; fixture navigation never receives it.
Worker observability is disabled. Reports contain no transcripts, raw exceptions,
passwords, control tokens, session IDs, or private URLs.

## Run

Use a clean committed checkout. Required environment:

| Variable                       | Purpose                                                       |
| ------------------------------ | ------------------------------------------------------------- |
| `CHECKOUT_EXPECTED_SHA`        | Exact 40-character commit that must equal HEAD                |
| `CHECKOUT_RUN_ID`              | Fresh lowercase letters/digits/hyphens, at most 24 characters |
| `CHECKOUT_MODEL`               | Explicit model ID; CI uses gpt-6-luna                         |
| `CHECKOUT_TOKEN`               | Fresh random control API bearer token                         |
| `CHECKOUT_PASSWORD`            | Fresh random password for the designated fixture buyer        |
| `OPENAI_API_KEY`               | Model provider credential                                     |
| `CLOUDFLARE_ACCOUNT_ID`        | Intended account, checked against Alchemy's account           |
| `CLOUDFLARE_API_TOKEN`         | Deployment credential; stays on the runner                    |
| `BROWSER_RENDERING_API_TOKEN`  | Narrow account-scoped Browser Run Write token                 |
| `CLOUDFLARE_WORKERS_SUBDOMAIN` | Workers subdomain without .workers.dev                        |

After supplying provider credentials through the environment or the
[documented secret manager](../../docs/TOOLCHAIN.md#live-credentials):

```sh
export CHECKOUT_EXPECTED_SHA="$(git rev-parse HEAD)"
export CHECKOUT_RUN_ID="checkout-$(openssl rand -hex 6)"
export CHECKOUT_TOKEN="$(openssl rand -hex 32)"
export CHECKOUT_PASSWORD="$(openssl rand -hex 32)"
export CHECKOUT_MODEL=gpt-6-luna
vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

Missing configuration, a dirty checkout, a mismatched SHA, an existing report, or an
occupied Worker name fails before deployment. Local Alchemy emulation is refused.
The runner records its identity and pending report before provisioning the stage.

The ignored `.checkout-proof/<run>/report.json` in this workspace retains the commit,
Worker name, dispatch status, sanitized independent evidence, allowlisted Worker failure codes,
the last checkout operation, login request count, failure stage/status,
cleanup result, and total, checkout, and cleanup milliseconds. Total time includes
deployment and retirement. Checkout time measures the single run request; cleanup time
measures closure, its independent confirmation, and Worker retirement. Evidence is
written atomically. A mismatched receipt is omitted rather than publishing unexpected
identity or payload values.

Failure codes distinguish invalid helper calls from browser failures and retain known
error tags even when a failure is carried as an Effect defect. The operation identifies
whether it occurred during scraping, browser setup, model execution, input, or closure;
unknown errors still use `worker-failure` without publishing their messages.

## Recovery

The runner closes the exact private browser session, independently reads its closed
state, persists closure evidence, then destroys the Worker. Unconfirmed closure fails
the gate and preserves the Worker and its Durable Object alarm for recovery. Destruction
must also be confirmed by the Worker management API. A lost browser-acquisition reply
without a session ID keeps cleanup unconfirmed and requires provider reconciliation;
the owner is retained rather than claiming that no browser was created.

Keep the same clean revision, environment, report, and private `.alchemy` directory.
Recovery performs closure and retirement only:

```sh
CHECKOUT_CLEANUP=true vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

Recovery never deploys or calls `/run`. An absent owner is acceptable after recorded
closure, or when no run was dispatched. Otherwise it fails clearly. Successful cleanup
does not turn a failed checkout into passing evidence. Use a fresh run ID for a new
attempt; existing evidence cannot be overwritten.

## CI policy

Ordinary PR CI runs the local workerd receiver proof without deployment or credentials.
The Changesets version PR runs one hosted checkout on its exact head through
`release-gates.yml`, required by `ready`. Publication reuses that proof only under
the repository's existing release revision policy. The manual hosted checkout workflow
also runs one attempt at the selected dispatch commit.

Both workflows generate and mask the control token and buyer password, retry recorded
cleanup after failure or cancellation, and retain only `report.json` for 30 days.
Missing evidence, failed checkout, or unconfirmed cleanup fails the job. Never upload
`.alchemy`, credentials, or private session capabilities. Hard runner termination
can prevent finalizers and lose local Alchemy state; the retained Worker alarm remains
the fallback for closing its browser. Preserve private deployment state securely when
manual retirement is needed.
