# Hosted browser and checkout proof

A real `effect-agent` buyer receives a purchase request and discovers a controlled store through
ordinary browser observations. The store and payment pages run on separate HTTPS Workers;
Cloudflare Browser Run supplies Chromium. A consumer-owned Durable Object retains the browser
reference, authentication, cart, approval and payment-attempt ledger across requests.

```text
Alchemy test lifecycle
  ├─ existing binding proof → Quick Actions, credentials, upload, scoped cleanup
  ├─ shop Worker → CheckoutRun (SQLite) → retained Browser Run session
  └─ payment Worker ← browser iframe / wallet redirect

buyer → discover → cart → sign in → address / wallet verification → shipping → payment
      → request approval → separate user request → submit once → inspect order history
```

This extends the existing hosted proof and replaces its Wrangler deployment script with Alchemy's
test lifecycle. See the [browser guide](../../docs/guide/browser.md) for the production adapters.
The example depends directly on `@cloudflare/puppeteer` for the binding proof and uses the public
native `BrowserSessions` and `BrowserCredentialAccess` APIs for the buyer.

## What it checks

Each isolated run starts with four units of inventory and a dummy account. The buyer must find one
blue medium Everyday Shirt, choose standard shipping, and obtain approval for $42.12 USD. The
runner validates the full server-recorded product, variant, quantity, address, shipping, tax,
currency and payment result. It requires one order and exactly the expected payment attempts.
An accidental second submission fails the proof even though the receiver refuses a second order.

| Scenario           | Embedded card                               | Accelerated wallet                                        | Expected attempts                      |
| ------------------ | ------------------------------------------- | --------------------------------------------------------- | -------------------------------------- |
| Success            | Dynamically mounted cross-origin card frame | Email, verification, saved address/card                   | paid                                   |
| Correction         | Invalid saved ZIP plus primary-card decline | Saved-card decline                                        | declined, paid with backup             |
| Ambiguous response | Payment commits; confirmation responds 503  | Same                                                      | paid; inspect history, no resubmission |
| Human takeover     | —                                           | Operator enters verification and returns the same browser | paid                                   |

The generic browser tools expose observations, navigation, clicks, text entry, selections and
credential filling. The buyer receives no selectors, click sequence or purchase API. Its owner
allows only the two fixture origins and supplies dummy credentials through the native fill helper.
Observations include visible frames and current control values, so collapsed payment sections must
be opened before their fields become observable. Each frame includes its ordered CSS iframe path;
copy that path into browser and credential tools, with field selectors relative to the selected
frame. A frame URL is not a selector. Dummy field values can be visible; this is not a
credential-secrecy proof.

Approval belongs to the owner. The agent can request a pause but cannot grant approval. The runner
approves the independently checked quote through a separate authenticated request. Changing cart,
address, shipping or payment invalidates that approval. Each continuation starts a bounded agent
Run and reattaches the same browser; agent conversation history is not persisted. This demonstrates
consumer-owned browser continuity, not framework durable-thread replay.

The owner fences a running request before dispatch. A lost request is unresolved and cannot start
again automatically. A native attachment is scoped to one request; releasing it disconnects without
closing the retained browser. An alarm enforces the session's twenty-minute lifetime. The runner
closes the exact session before destroying its owner. Closure or destruction failure fails the gate
and retains the Alchemy stage when browser recovery is still needed.

## Run

Required environment:

| Variable                       | Purpose                                                                 |
| ------------------------------ | ----------------------------------------------------------------------- |
| `CLOUDFLARE_ACCOUNT_ID`        | Intended account; checked against Alchemy's resolved account            |
| `CLOUDFLARE_API_TOKEN`         | Local deployment credential with Workers and Durable Object permissions |
| `BROWSER_RENDERING_API_TOKEN`  | Narrow account-scoped Browser Run Write token                           |
| `CLOUDFLARE_WORKERS_SUBDOMAIN` | Workers subdomain, without `.workers.dev`; also used for recovery       |
| `OPENAI_API_KEY`               | Real model credential, injected by the operator's secret manager        |
| `CHECKOUT_MODEL`               | Explicit OpenAI model ID                                                |
| `CHECKOUT_TOKEN`               | Fresh random bearer token for the fixture control API                   |
| `CHECKOUT_RUN_ID`              | Fresh lowercase letters/digits/hyphens, at most 24 characters           |
| `CHECKOUT_REPETITIONS`         | 1–5 repetitions of every automated scenario; default 1                  |
| `CHECKOUT_CONCURRENCY`         | 1–12 simultaneous checkout cases; default 4                             |
| `CHECKOUT_START_INTERVAL_MS`   | Minimum interval between case admissions, 1000–60000 ms; default 1000   |
| `CHECKOUT_HUMAN`               | `true` runs one operator takeover before the matrix; default `false`    |

The browser and OpenAI credentials enter only temporary Workers as secret bindings. The deployment
credential stays local. Keep the bearer token, `.alchemy` state and temporary Live View file private.

From the repository root, after injecting credentials:

```sh
export CHECKOUT_RUN_ID="checkout-$(openssl rand -hex 6)"
export CHECKOUT_TOKEN="$(openssl rand -hex 32)"
export CHECKOUT_MODEL=gpt-5.6-luna
export CHECKOUT_REPETITIONS=2
export CHECKOUT_CONCURRENCY=4
export CHECKOUT_HUMAN=false
vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

The automated matrix runs four isolated checkouts at a time, admitting at most one new case per
second. Actions and approval continuations within each checkout remain sequential. Deployment and
the binding proof finish before the matrix starts; stage retirement waits for all case fibers,
including interrupted children. Report updates are serialized and published atomically. Adjust
concurrency and admission spacing to the account's browser and model limits; failed requests are
not retried automatically.

Human takeover is a separate, optional operator check. Set `CHECKOUT_HUMAN=true` explicitly to run
it before the automated matrix. The runner prints the path to a temporary `live-view.txt` file. Open its private URL,
enter the fixture code `246810`, and select **Verify and use saved details** within five minutes.
Select **Done** if Live View offers it. The owner resumes after verification and an inactive provider
handoff; no further browser action is needed. The URL is removed afterward and is never put in the
report or model history. Automated runs never wait for an operator and do not establish human takeover.

The ignored `tooling/browser-run-worker-proof/.checkout-proof/<run>/report.json` records model,
source commit/dirty state, policy, selected profile, completion rate, failures, observations, tool
outcomes, model usage and finish reasons, server orders, attempt ledger and cleanup result.
Failed credential calls also retain the requested alias, frame path and field selectors/roles,
without resolved credential material.

Instrumented reports retain request-local monotonic spans for model calls, browser dispatch,
observations, waits, attachment, approval/resume and exact closure. Each browser span identifies
the model turn that requested it; model spans retain requested/resolved model IDs, tool names
and reported tokens. An observation-only turn can therefore be counted independently from an
observation tool call. Observation sizes are UTF-8 bytes. Missing usage and browser protocol
counts remain unavailable; provider costs are unpriced, not zero.

Case elapsed time starts before seeding and ends after exact browser closure, including approval
requests and cleanup. Admission queueing is excluded from cases and included in matrix time.
Deployment, readiness, the binding proof and retirement have separate totals. Spans may nest or
overlap: do not add their durations to estimate wall time. Timing adds two synchronous SQLite
writes per span and native response metadata collection; hosted comparisons must use the same
instrumentation. Worker clocks may coarsen synchronous work; zero-duration spans do not imply
zero cost. Running spans survive process loss; interruption retains finalizer outcomes.

Each request has a five-minute duration, 60-turn, 120-tool-call and 500,000-token budget; each
scenario permits at most six continuations.
Provider/model work costs money. Repetitions use fresh stores and browsers; failed attempts are
retained without automatic model or purchase retries. Deterministic fixtures do not make model
actions deterministic.

If interrupted, retain the same configuration and `.alchemy` directory, then run only recovery:

```sh
CHECKOUT_CLEANUP=true vp run --no-cache -F @effect-agent/example-browser-run-worker-proof prove:live
```

Recovery reads the existing report, closes its recorded sessions, destroys the same Alchemy stage,
and confirms all three Worker scripts are absent. Browser retirement is recorded atomically before
Worker teardown, so recovery also handles an already-absent owner. It never deploys or reruns purchases. A fresh
attempt needs a fresh run ID; existing evidence cannot be overwritten. Hard termination can prevent
local finalizers, so the owner alarm and explicit recovery remain necessary.

## Provider evidence and limits

The fixture reproduces interaction patterns, not provider branding or payment processing.

| Provider | Simulated here                                                                                                                     | Separate verification and blockers                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| -------- | ---------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Stripe   | Mounted card fields in an independent-origin frame, validation, card replacement, final review, decline and uncertain confirmation | Public [checkout demo](https://checkout.stripe.dev/) inspected without payment submission. No actual payment compatibility established. Stripe explicitly [does not support automated UI tests](https://docs.stripe.com/automated-testing) of Checkout/Payment Element; its [sandbox API tests](https://docs.stripe.com/testing) are a separate integration boundary. No Stripe sandbox credential is configured for this proof.                                                                                                                                                                         |
| Shop Pay | Email-first redirect, six-digit verification, saved address/card, shipping and final review                                        | Pattern follows the [documented buyer checkout](https://help.shop.app/en/shop/shop-pay/check-out). No provider checkout was completed: no Shopify development store or test Shop account is available. Shopify's [Shop Pay testing setup](https://shopify.dev/docs/apps/build/checkout/test-checkout-ui-extensions) requires a development store, Shopify Payments test mode, a Shop account with a vaulted test card, and phone verification. [Test-mode guidance](https://help.shopify.com/en/manual/payments/shopify-payments/testing-shopify-payments) covers test cards, not Shop Pay Installments. |

Passing this controlled receiver never establishes Stripe or Shop Pay compatibility. Genuine
provider checks require the supported sandbox setup and their own receipts; do not substitute a
clone result. Real funds, fulfillment, live accounts, fraud systems, 3DS and installment eligibility
are outside this fixture.

## CI policy

Ordinary PR CI runs deterministic state tests, lifecycle failure/interruption tests, and the local
workerd receiver checks without credentials or deployment. After a Changesets version PR merges
and its exact revision passes CI, `release:checked-publish` runs the hosted matrix before publishing.
It skips paid work when every public version is already on npm. Adding a changeset or opening a PR
does not trigger a hosted run. Checkout or cleanup failure blocks publication.

For an on-demand run, select **Manual hosted checkout** in GitHub Actions and choose a trusted
branch or tag. It checks out that dispatch's exact commit. Both workflows use `gpt-5.6-luna`, two
repetitions (12 cases), concurrency four, one-second admission spacing, and `CHECKOUT_HUMAN=false`.
Only an explicit local operator run establishes human takeover coverage.

Configure the `OPENAI_API_KEY`, `CLOUDFLARE_ACCOUNT_ID`, `CLOUDFLARE_API_TOKEN`, and narrow
`BROWSER_RENDERING_API_TOKEN` repository secrets, plus the `CLOUDFLARE_WORKERS_SUBDOMAIN` variable.
Each attempt generates a fresh run ID and masked control token. Both workflows retry recorded
cleanup after failure or cancellation and retain `report.json`, including failed results, for 30 days.
Never upload `.alchemy` or `live-view.txt` as CI artifacts. Hard runner termination can still prevent
cleanup; use the recorded run ID to inspect remaining Workers and their browser-owner alarms.
