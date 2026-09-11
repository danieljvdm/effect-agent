# Travel planner

The canonical runnable Effect Agent example: a small travel chat, saved trips, and
standalone trip websites on Cloudflare.

The app consumes published Effect Agent packages, pinned to exact npm versions in
`package.json`. `vp install` uses the repository's `bunfig.toml` to keep those dependencies
and their transitive imports on registry packages. Framework workspaces use explicit
`workspace:` dependencies for local development. Library fixes belong in separate PRs
with regression tests and changesets; upgrade this app after those fixes are released.

Start a conversation, describe a destination, and the agent saves a draft through
`save_trip`. Select a trip in the sidebar to revise it. Ask for a website or use **Create trip
website** to fork a public full-stack app. The request itself authorizes creation; there is
no special publication phrase. Its card shows build progress, **Open trip app**, and code
versions. Ask to add a journey map or customize the source; restore an earlier version from
the card. Saved trip data stays current when code changes or is restored. App creation and
code changes run in a separate durable editor. You can keep chatting while it works, including
sending a new message while the planner's previous turn is pending. Accepted messages join
at a safe runtime boundary; accepting a message does not mean its work has finished.
When the planner is idle, messages appear immediately in the conversation and stay there
as delivery is confirmed. Messages sent during an active turn appear above the input with
**Sending** or **Queued** status, then move into the conversation when recorded. The pending
list scrolls independently so the input remains reachable. Failed acknowledgements offer
**Retry** where the message appeared, preserving the original request
ID and model settings without clearing a new draft. Accepted queued messages survive reload;
messages that have not reached the server remain local to the current tab.

**Plan a new trip** starts a separate conversation with fresh model history. Returning
to a trip restores that conversation's messages. Each conversation has a private
`/conversations/<conversationId>` URL, including conversations without a saved itinerary.
Opening `/` or **Plan a new trip** redirects to a fresh conversation URL before the first
message; it does not create a stored conversation until you send one. Refresh, bookmarks,
and browser Back/Forward restore the conversation selected by that URL. Sidebar links can
also open in another tab. Links still require sign-in to the owning account; they do not
publish conversation history. Malformed conversation IDs show the not-found page.
The first message reserves a private
sidebar entry before agent admission, independently of whether the model saves trip details.
Unfinished and failed conversations remain reachable after reload or restart. Entries initially
use the first message as their title; saved trip details replace that label without a duplicate.
A failed admission can leave an empty, retryable conversation; retries retain its original entry.
Existing saved trips remain discoverable without rewriting their conversation associations.
The composer stays at the bottom of the
viewport while the conversation scrolls. Search results and saved source URLs are clickable.
Replies arrive as live text. The expandable activity chip keeps a stable step count;
current work and public progress appear in its details and the response.

The conversational planner delegates public-source research to up to six focused scouts;
it has no web search or page-reading tools. It dispatches useful independent tasks before
optional draft saves, then replies after acceptance while asking about your preferences.
Scouts do not require a saved trip; the app editor does, so a requested site may need a save first.
Later details steer those same durable workers. Completed findings return
to the planner automatically; you do not need to ask it to check again. A compact research
dock above the composer opens live progress and recorded activity in a dialog. Scouts can
research public sources but cannot book, change trip apps, or launch more agents. Internal
reports do not appear as user messages or authorize another research pass on their own.
The planner's trip lookup returns only the current conversation's saved trip, including
when a scout report has no selected trip ID. A rejected trip lookup or save is returned to
the model so it can refresh the ID/revision and correct the request without ending the turn.
Cross-conversation writes remain forbidden. A storage failure may follow a committed write;
the planner must read the saved state before retrying and stop saving if it cannot verify it.
The host allows seven active workers across research and app editing, leaving room for six
scouts and an editor, and retains at most 100 workers per conversation. Each worker accepts
up to 256 total inputs and 16 pending inputs; its reference lasts seven days. Existing
conversations and app editors continue to work.

New research scouts validate completion drafts inside the `finish_research` handler. A rejected
draft returns corrective feedback to the same running scout, preserving its research context.
Accepted findings still require a summary of at most 4,000 characters, at most six sources, valid
public HTTPS links, and a complete encoded result of at most 8 KiB. Nothing is silently truncated.
Corrections use the existing turn, tool, and duration budgets; structural provider errors and
exhausted budgets can still fail a run. Previously accepted scouts retain their original contracts.

For a request with several parts, the coordinator must complete or dispatch every part
before its final reply. A request to build a site and find golf courses and surf breaks
starts the editor and both research tasks; saving a draft or starting only the editor
does not fulfill it. Optional questions do not block independent work. The coordinator
has a cumulative 8,000,000-token run allowance, separate from its 128,000-token context
target. The context target triggers compaction of the model prompt, not deletion of saved
conversation records, and is an application setting rather than a model context-window limit.
Repeated input tokens count toward the cumulative allowance, which leaves room for the
longer run even with a large conversation. These are ceilings, not work quotas: finish once
the requested work is complete or accepted by the appropriate workers.

| Agent          | Model steps | Tool calls | Duration   | Concurrent tools |
| -------------- | ----------: | ---------: | ---------- | ---------------: |
| Planner        |          48 |         96 | 15 minutes |                1 |
| Research scout |          32 |         64 | 10 minutes |                4 |
| App editor     |          48 |         96 | 15 minutes |                1 |

All three receive visible remaining run limits and a 128,000-token context target. Scouts
and editors have independently funded run budgets without a cumulative token cap. The host
selects the expanded policy from the new worker's canonical v11 owner submission, then
persists that policy in its immutable origin. Previously accepted runs and existing workers
retain their original allowances and bindings, including after restart or follow-up. New
messages on existing trips use v11; new workers receive the expanded allowance. Run-policy updates do not reset history. The authentication cutover described below is a deliberate clean start.

Researched options appear as native stay, flight, restaurant/activity, and itinerary cards.
Stay cards include source photos, a keyboard-accessible gallery, amenities, and listing links.
Flight cards summarize the route; itinerary cards let you browse days and their activities.
Unknown prices remain unset, and source links let you check the details. Missing or blocked
photos have a fallback; the planner never substitutes stock images. Older replies render
Markdown headings, lists, tables, and sourced images too. **Itinerary** in the top bar opens
the saved trip details without crowding the conversation by default. On phones, the navigation menu contains trips, new-trip, itinerary, activity, and account actions. The compact toolbar keeps model controls accessible. The composer uses 16px text on mobile and tracks the visual viewport to stay above the keyboard, while preserving pinch zoom; the menu traps focus and restores it to its trigger when dismissed.

Opening or closing the keyboard keeps the latest reply anchored when you're at the bottom.
If you've scrolled up, resizing preserves your reading position instead.

Returning to a recently opened trip shows its cached conversation immediately. An uncached
trip shows a loading state, and the sidebar retains its latest catalogue. Only an actually
empty conversation shows the welcome prompt. History is cached in this tab for 30 minutes,
partitioned by local Auth subject ID; only the selected conversation is polled, with the next refresh
scheduled after the current request finishes. Background refreshes,
client reconnects, and temporary session-check failures preserve the last loaded view for
that account and conversation. An explicit access rejection clears it; another account or
an uncached conversation never inherits those messages. Conversation atoms use a single
structural account/conversation key so garbage collection cannot discard a nested cache
factory while its queries are mounted.

The top-right settings button selects **GPT-5.6 Luna** or **GPT-6 Astra** and a reasoning
level from Low through Maximum. Luna also supports None. The lightning button switches
between Standard and Fast processing; Fast uses higher token rates. Preferences are saved to
the signed-in account and restored on other browsers and devices. Settings are captured when
a message is admitted. Changing a setting cannot alter
work already running. Accounts without saved preferences default to Astra, Low reasoning,
Fast processing. Existing saved preferences remain unchanged.

## Voice conversation

Connect an OpenAI key with access to `gpt-live-1`, then select **Start voice** and allow the
microphone. GPT-Live handles the spoken conversation and delegates travel work to the existing
planner. Voice and typed messages use the selected trip conversation, tools, account credentials,
and durable admission. The voice model is fixed; the planner uses the model settings captured
when the call starts. This uses [GPT-Live client delegation](https://developers.openai.com/api/docs/guides/live-delegation).

**Stop playback** mutes audio locally. **End voice** closes the call; accepted planning and app
work continues. Spoken lines appear under the same You and Elsewhere speakers in the main
conversation. Display grouping follows each speaker’s transcript timestamps independently:
brief overlapping acknowledgments do not split a continuing user sentence, while distinct
assistant updates get separate messages. Questions and substantive replies separate quick
user answers. Grouping uses revisable timing heuristics, retains original caption fragments,
and never submits or cancels work. A typed-input boundary or replacement call starts fresh
groups; already saved transcripts are not retrospectively rewritten. Full planner answers
remain available under **Trip details** while voice carries
the exchange; cards stay visible. You can type during a call or return to text afterward. Typed
follow-ups include recent spoken context, and voice follows their results without resubmitting
them. Only a delegation event admits spoken work. Its user request and attributed conversation
context are separate fields; a transcript never becomes a synthetic user message or title. The delegation policy explicitly includes answers to preference questions and corrections to ongoing work (such as confirming 50 km running ability). The planner sends accepted corrections to the relevant existing workers before claiming they were applied.

Reconnect creates a replacement voice session with recent saved conversation history. This tab
retains up to sixteen frozen request envelopes in session storage, partitioned by local Auth subject ID
and conversation. Reconnect looks up every prepared or uncertain admission by its original request
ID before retrying any missing admission with its exact envelope; accepted work is only observed.
Newer spoken or typed input changes which result voice follows without abandoning older uncertain
requests. During a call, those requests remain observed; another attempt requires reconnect.
Only known admissions can be evicted from the retry cache. If all sixteen entries are unresolved,
voice disconnects before admitting another request so reconnect can reconcile them.
Closing the tab loses this local retry cache,
while already accepted work and its canonical conversation remain saved. Recent undelegated
speech stays in this tab after ending a call, and accompanies the next typed or spoken request.
It is not promised to survive reload before that submission. Changing conversations or
accounts closes the media session. A full page reload can require reopening the original trip
before reconnecting.

Only native response text, designated `deliver_response.message` previews,
and schema-decoded settled answers reach the voice model. Previews are explicitly
provisional and sent as context; final answers use canonical settlement. Reasoning, credentials,
raw tool output and diagnostics are excluded. A short summary of the saved trip and visible
option names keeps references such as “the second one” grounded in the screen. Later canonical
research answers return to the same voice exchange. Successfully settled research summaries
can reach voice before the planner finishes synthesizing the full answer. The complete public
summary arrives as bounded quiet context chunks, including its caveats, before one spoken
finding update. Each result is sent
once; corrections discard pending notes. Activity labels and timers do not trigger waiting
announcements. A planner reply does not consume unrelated pending scout findings. Raw tool data, private reasoning, and diagnostics are excluded. New scouts can deliberately report a sourced milestone with `report_research_progress` while they continue; Effect Agent durable messaging delivers it to the original conversation. The host derives the destination and account from canonical worker lineage, never model-selected routing. These milestones preserve uncertainty and cannot authorize new research or app edits. Earlier accepted workers retain their original executable definitions and completion reporting. A brief utterance
delays an outgoing result without discarding it; actual delegated corrections and typed requests
replace the work followed by voice.
Typing redirects the current explanation without leaving audio muted. Provider acknowledgment,
playback and durable settlement remain separate internal states; no event proves speech was heard. Website editing and deployment remain separate: editor completion reports return to the planner, while current build phases update voice context directly. A newly ready or failed website gets a spoken update without another user request; an earlier ready version is not announced as the requested edit while its editor is active.

The browser uses WebRTC media and a bounded event queue (128 events, 32 KiB per event). It waits
for `session.started`, keeps at most 128 caption fragments, coalesces pending delegation metadata,
and limits each call to fifteen minutes and 64 delegation IDs. Public context appends are capped
at 420 UTF-8 bytes, with at most one outstanding acknowledgment and progress updates no more
often than every three seconds. Startup, acknowledgment, admission, and close waits are bounded.
Failed connections require explicit reconnect. Closing audio never calls planner cancellation.

Session creation runs behind Yielded Auth session authorization, account binding, origin checks, and request-size
limit, using only the verified account's encrypted key. It requests `gpt-live-1` with client
delegation and returns only the session ID and SDP answer. No shared key or credential reaches
the browser. Voice sessions incur provider duration charges separately from planner inference.
Removing a saved key prevents new sessions and new planner model requests; an already established
voice session must be ended separately.

For the live acceptance check, start a new conversation and speak a request that requires saving
or researching a trip. Confirm tool activity, a saved planner answer, and an audible answer.
Interrupt playback while work is pending, end and reconnect the call, and verify that the same
request/receipt finishes without repeating the accepted work. Then type a correction and confirm
it updates the same conversation. Deterministic transport tests cover these boundaries but do
not substitute for checking real microphone input, provider delegation, and audible playback.

## Run

From the repository root:

```sh
vp install
cp examples/travel-planner/.env.example examples/travel-planner/.env
```

Fill the ignored `.env` using `.env.example`. The app pins `@yielded/auth@0.1.0-beta.2`
and the existing Effect `4.0.0-rc.112` catalog. It uses one `Auth.make` service for
email codes and GitHub, with the published SQLite Durable Object adapters. Auth owns
proofs, request binding, credential/session authority and OAuth exchanges; application
mappings own durable local subjects and claims. No in-memory production stores are used.

On `/login`, choose **Create account** for a new email account. Verify the registration
code, then enter the fresh sign-in code sent automatically. This second proof is required
by the published registration/session contract; account creation alone does not sign you in.
Returning accounts use **Sign in**. Codes expire after five minutes; five failed guesses
exhaust a proof. Resend cooldown is 30 seconds, with per-address and global issue/attempt
budgets. Delivery acceptance does not promise inbox delivery; ambiguous sends are not retried.
GitHub automatically provisions a new local account when registration is required and
starts a fresh authorization to establish its session. Denied or failed exchanges offer
an explicit new attempt. Email and GitHub are separate credentials/accounts even if their
profile emails match. There is no automatic linking, admin bootstrap or invitation gate.

```sh
vp run -F @effect-agent/example-travel-planner dev
```

Use a trusted local HTTPS origin matching `AUTH_ORIGIN`, with a separate GitHub OAuth App
whose callback exactly matches that origin plus `/auth/github/callback`. The secure
`__Host-` cookies require HTTPS. Alchemy's Email binding simulator writes local messages to
`.alchemy/local/email`; leave it local to avoid sending real mail. Test-only Workerd fixtures
supply deterministic codes and provider responses without production credentials. Browser
fixtures exercise the real UI with intercepted Auth/RPC responses. There is no production
auth bypass. Cloudflare credentials must target the intended account; Artifacts and Browser
Run require access to those products. Every account supplies its own OpenAI key in Settings.

Model selection and the provider's native web-search tool stay at the host boundary.
The agent accepts a research toolkit and a provider-independent model Layer.
The selectable models support native web search. `OPENAI_MODEL` is retained for the previous
registration so already accepted work can finish with its original model after deployment.
Page inspection accepts newly discovered HTTPS websites without a travel-site allowlist,
including tourism boards, campsites, and independent hotels. URL checks reject IP literals,
local hostnames, embedded credentials, and custom ports, and also apply to browser redirects
and subresources. These are URL checks, not a DNS-resolution firewall. Page-size limits,
timeouts, and access-challenge handling still apply.

Airbnb listing URLs on `airbnb.com` and `www.airbnb.com` use a 10-second
`domcontentloaded` navigation limit followed by a 10-second wait for
`[data-section-id="AMENITIES_DEFAULT"]`. This waits for listing details without depending
on network quiet. Other sites retain their existing navigation policy. A navigation shell,
a title/gallery alone, or a heading without a text section is a typed `WebCapturePageUnready`
failure. A successful result still contains selected source excerpts, not a complete amenity
inventory: missing amenities remain unverified, and titles or photos alone do not prove them.

Each inspection makes one capture request with a 25-second outer timeout, a 512 KiB capture
limit, and a 12 KiB result limit. There is no automatic retry or fallback capture. Selector
changes, incomplete content, access challenges, and provider timeouts can still fail. Browser
Run API HTTP 422 is not a destination status or proof that Airbnb blocked the request.
The provider's detailed cause stays in private diagnostics; the published beta.78 adapter
reports only the generic API status for these navigation timeouts to the model.

Local interruption stops waiting and finalizes an acquired response reader; it does not
confirm remote browser termination because the native Quick Action binding provides no
abort signal or session handle. REST captures informed the readiness policy. Local tests
exercise the published adapter with scripted binding responses, not the hosted provider's
lifecycle.

## Deploy with Alchemy

The app pins published `0.1.0-beta.78` packages, including JSON persistence,
existing-conversation worker upgrades, and browser failure diagnostics. Library fixes
are released separately before the demo adopts them; local library patches are not bundled.

For an existing deployment, approve and complete the maintenance prerequisites in
[Clean-start cutover and reset](#clean-start-cutover-and-reset) before deploying.

```sh
vp run ready
vp run -F @effect-agent/example-travel-planner deploy --dry-run
vp run -F @effect-agent/example-travel-planner deploy --yes
```

`alchemy.run.ts` owns the separate `effect-agent-travel-planner` stack: a TanStack Start
SSR Worker, owner/conversation SQLite Durable Objects, Browser Run, an Artifacts namespace,
an R2 build bucket, a Workflow, a Sandbox container, and a Dynamic Worker loader. Worker
and Workflow entrypoints, Sandbox processes, and R2 use `effect-cf`. Docker is required for
the Alchemy container build; its image and SDK use the same pinned version.
It uses Cloudflare's remote Alchemy state store. `ALCHEMY_LOCAL_STATE=true` selects local
state for isolated experiments; do not switch state stores for an existing deployment.
The root docs stack is independent. Deployment credentials are never bound into the Worker.
The `Deploy travel planner` GitHub Actions workflow deploys changes on `main` to this
production stack and also supports manual dispatch from `main`. It reuses the docs workflow's
`CLOUDFLARE_API_TOKEN` and `CLOUDFLARE_ACCOUNT_ID`, including the account-wide Alchemy state
store; it does not need a second Alchemy key. Application checks and tests run before deployment.
Set repository Actions secrets `TRAVEL_PLANNER_GITHUB_CLIENT_ID`,
`TRAVEL_PLANNER_GITHUB_CLIENT_SECRET`, `TRAVEL_PLANNER_AUTH_BINDING_KEY`,
`TRAVEL_PLANNER_AUTH_PROOF_KEY`, `TRAVEL_PLANNER_AUTH_TRANSACTION_KEY` and
`TRAVEL_PLANNER_BYOK_ENCRYPTION_KEY`. The workflow fixes the origin to
`https://travel.effect-agent.com` and the sender to `auth@effect-agent.com`.
Use manual dispatch for the initial cutover only after the maintenance and reset prerequisites
below are complete. After the cutover and live login checks, set repository variable
`TRAVEL_PLANNER_DEPLOY_ENABLED=true` to enable automatic deployments. Until then push-triggered
jobs are skipped. Normal deployments never run the one-time cleanup.
Alchemy owns the custom domain; workers.dev and preview URLs are disabled. Generated app
hosts remain public. The planner uses a single canonical origin and 30-day Auth sessions.
The obsolete Access application, policy, membership APIs and demo-funding controls are removed.

### Provider setup for release

- Enable [Cloudflare Email Sending](https://developers.cloudflare.com/email-service/get-started/send-emails/)
  on the intended account (currently requires Workers Paid). Onboard the sender's domain
  to Email Service, finish its `cf-bounce` MX/SPF, DKIM and DMARC checks, then set
  `AUTH_EMAIL_FROM` to the verified sender. `AUTH_EMAIL` is a structured Workers send binding
  restricted to that sender, with unrestricted recipients for public signup. It needs no
  email API key. The template and bounded acceptance adapter are in `src/auth/email-delivery.ts`.
  Disable the sending domain's activity-log message previews so login codes do not appear there.
- Create a GitHub **OAuth App**, with homepage `https://travel.effect-agent.com` and exact
  callback `https://travel.effect-agent.com/auth/github/callback`. Supply
  `AUTH_GITHUB_CLIENT_ID` and secret `AUTH_GITHUB_CLIENT_SECRET`. This requests identity
  only: no repository API access, Gmail integration, Google login or provider token custody.
- Set `AUTH_ORIGIN=https://travel.effect-agent.com`. Redirects are server-configured; the
  return allowlist contains only `/`. Generate three independent, cryptographically random
  32-byte **base64url** values for `AUTH_BINDING_KEY`, `AUTH_PROOF_KEY` and
  `AUTH_TRANSACTION_KEY`, and a separate 32-byte **base64** `BYOK_ENCRYPTION_KEY`.
  Supply secrets through the deployment environment, never source control or logs. Keep
  them stable after launch; arbitrary replacement invalidates flows or encrypted keys.
- Verify mail delivery and GitHub authorization with explicitly authorized test accounts
  before public release. Deterministic tests and local browser fixtures do not establish
  live sender readiness, inbox delivery or OAuth App configuration.
- Automatic invocation URL logs, request traces and Worker Logpush are disabled to keep
  callback codes/state out of telemetry. Keep that setting; review any independently managed
  edge Logpush/analytics pipeline to exclude callback query strings before enabling it.
  Auth diagnostics contain only safe stage names, never request bodies or provider errors.

### Clean-start cutover and reset

This release deliberately discards the old app accounts and their data. Everyone registers
again and re-enters their OpenAI key. There is no identity migration or recovery path.
The following is a release procedure for the owner to approve and execute; PR preparation
must not deploy, change provider consoles/secrets or delete live data.

| State                       | Exact reset scope                                                                                                                                                                                                                                                                                                                                                                                                                     |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Old account/agent data      | This stack's `PlannerThread` SQLite Durable Object namespace, including original `travel-planner-owner-v1`, `member-*` owners, their conversation Objects and child workers. This removes trips/revisions, conversation history, admitted work, journals/checkpoints, diagnostics, model settings, encrypted BYOK rows, app/editor records and the old `travel_demo_access` funding row. No other Worker/stack namespace is included. |
| New auth and planner state  | New logical binding `AuthThreadsV1` / class `AuthPlannerThread`, and `AuthV1` / class `PlannerAuth` (Object name `auth-v1`). The latter begins empty: local subjects, identifiers/provider bindings, credentials, proof/abuse/continuation records, OAuth flow/registration records and sessions. Existing CF Access auth/proofs are external; no shared IdP or Cloudflare account-wide user store is deleted.                        |
| Source and snapshots        | Old Artifacts namespace exactly `effect-agent-travel-planner`, including trip repositories, published snapshot branches, generated-app forks and old template. New writes use `effect-agent-travel-planner-auth-v1`. No other Artifacts namespace is included.                                                                                                                                                                        |
| Builds and public addresses | In this stack's `TripAppBuilds` R2 bucket only, old object prefixes `apps/` and `app-addresses/v1/`. New writes use `auth-apps/v1/` and `app-addresses/auth-v1/`. Old generated URLs return not found; there is no legacy admin lookup or automatic directory repair.                                                                                                                                                                 |
| Browser state               | Close planner tabs; clear site data only for the planner origin, including old Access/Auth cookies, local/session storage, HTTP cache and retained voice admission IDs. New account runtimes discard drafts, history, settings, key metadata, progress and media on sign-out/account change.                                                                                                                                          |

1. Inventory the actual deployed Worker, Durable Object namespace ID, `TripAppBuilds`
   bucket name, Artifacts namespace and `SiteBuild` Workflow from **this stack's existing
   Alchemy state**. Do not guess provider IDs or run a repository-wide storage reset.
   Export the old address directory’s hostname inventory for the scoped cache purge below.
   Confirm no other application shares these resources. Resolve any discrepancy before release.
2. Keep existing Access protection while preparing provider configuration and the release
   plan. Immediately before cutover, install a temporary edge maintenance block scoped to
   `travel.effect-agent.com`, allowing only release testers. Alchemy may remove `TravelAccess`
   and `TravelInvitedUsers` during reconciliation; their removal must not expose the old Worker.
   The temporary block is separate from those managed resources and remains until step 6.
   Revoke sessions for the old `TravelAccess` application only; retain shared IdPs and groups.
   Do not block unrelated apps or wildcard hosts.
3. Stop admission to the old app. Terminate this app's active `SiteBuild` Workflow instances
   and its active build Sandboxes; stop old agent work before reset. This prevents old builds
   from republishing obsolete addresses during cutover. These are app resources, not other
   applications' Workflow or container instances.
4. Review the Alchemy dry-run against the existing state. Require deletion of only the old
   `PlannerThread` class and creation of SQLite classes `AuthPlannerThread` and `PlannerAuth`;
   the new logical ID must not be interpreted as a class rename/transfer. This is an
   irreversible application namespace deletion on deployment. Require removal of only
   `TravelAccess` and `TravelInvitedUsers`, the new Email binding/auth configuration, fresh
   Artifacts namespace and disabled workers.dev/previews. Preserve the bucket and unrelated
   stack resources. Only then execute the deployment commands above as the release decision.
5. With maintenance still active, enumerate and delete repositories **only** in the old
   Artifacts namespace. Enumerate R2 keys using each exact old prefix, review that inventory,
   then delete those keys and verify both listings are empty. Do not empty the whole bucket
   or touch the new prefixes. Purge CDN cache entries only for the retired generated hostnames
   from step 1; old open tabs/browser caches can retain already public assets until cleared
   or expired. Verify the old DO namespace is gone. Remove obsolete app-only
   `ACCESS_*`, `OPENAI_API_KEY` and `DEMO_OPENAI_API_KEY` Worker bindings if still present:
   Alchemy has previously left a removed secret bound after deployment. Never delete shared
   IdPs, Access groups, other apps' secrets or provider credentials as part of this reset.
6. In fresh browser profiles, test new and returning email/GitHub accounts, separate-account
   data, sign-out, BYOK, voice session authorization, private RPC/progress denial, and a newly
   generated public site. Check old generated URLs return not found and private APIs return
   401 without Auth cookies. Confirm the deployed binding inventory matches the reviewed plan.
   Remove the temporary maintenance block only after these checks pass.

A failed check leaves maintenance protection in place. Redeploying the old code cannot
restore deleted accounts or conversations and must not reopen an unprotected old Worker.
Do not reset the new Auth database after users begin registering. Future schema upgrades
must preserve supported state; unknown auth formats fail without mutation.

## Bring your own OpenAI key

Settings lets each account connect, replace, or remove its key. Connection checks use OpenAI's
model-list endpoint, with a 15-second timeout and redirects disabled; they do not run inference.
The check verifies authentication, not credit balance or permission to use every selectable model.
The selected model and processing tier must be supported by the supplied OpenAI account.
Cloudflare hosting, browsing, storage, and build containers remain billed to the platform owner.

Keys are AES-256-GCM encrypted with a fresh nonce and account-bound associated data, in a
separate versioned SQLite row in the account's owner Object. HTTP returns only connection status,
last four characters, and update time. Keys never enter trip data, canonical submissions, prompts,
traces, generated apps, or browser persistence. The password draft is cleared when submitted and
when its form closes. Validation and storage failures return bounded messages without provider
bodies or credential values. Before/after-write failpoints cover save and removal; after a lost
reply, refresh the connection status before retrying. Unsupported rows fail without replacement.

Every model HTTP request resolves the current key from the verified account. Planner attempts use
the host-owned conversation namespace; scouts and editors use their validated canonical source
lineage, never a model-supplied billing account or the child thread's name. Legacy registrations
also require the owner's key. There is no host-funded fallback. Rotation affects the next
model request; key removal prevents new requests, including background work. An already dispatched
provider request may finish. Failed work is not automatically replayed when a key is reconnected.
Saved trips, messages, and published apps remain readable without a key, and existing accounts
need a personal key to make model requests. Removing a key deletes the current credential row;
provider-side revocation is needed to invalidate copies in historical database backups.

## Boundaries and behavior

- TanStack Start renders the shell on the server. Authenticated data loads after
  hydration through an Effect Atom RPC client. There are no TanStack server functions.
- UI primitives follow shadcn's Base UI variant with Tailwind and the existing visual theme.
  Standalone Streamdown handles streaming Markdown; Effect Atom continues to own client
  workflows. Model HTML and executable URLs are rejected. Images load directly from source
  URLs without a referrer; no image proxy or model-supplied server fetch is introduced.
- `deliver_response` is the required read-only completion tool. Its `PlannerResponse` Schema
  carries conversational text and nullable, bounded `TravelContent`; recommendations use cards
  and ordinary greetings or clarifications can omit them. `show_travel_options` can display
  an early batch while research continues. Successful, validated results project into the
  selected conversation as cards. The canonical tool-result log preserves them across reconnects
  and restarts; no separate card database or booking mutation exists. Failed or malformed
  results never become cards. New work uses v6; v5, v4, v3 and v2 registrations remain available for
  already admitted work with their original tool schemas and output formats.
- Auth HTTP middleware supplies request context; `auth.requireSession()` explicitly authorizes
  private planner pages, published snapshots, RPC, voice and progress. `/login`, callback GET
  and required static assets are public. Callback GET only renders: an inline script removes
  secret query values before assets/hydration, then typed Auth actions submit the callback
  through a same-origin CSRF-protected POST. Codes/state/tokens are never logged or persisted
  in browser storage. Only the public GitHub flow ID survives the round trip in session storage.
- `/auth/*` uses shared AuthContract actions, typed `Client.make` and `AuthAtom.make`. Named
  auth mutations survive their own session transition; custom workflows render from
  `auth.session` and may retire at completion. One shared Atom runtime owns the app clients.
  Each Auth account lifetime owns a separate registry; retiring it cancels requests, progress
  and voice resources and discards private caches. Focus/visibility and periodic session checks
  observe cookie changes in other tabs. Requests include the registry's captured subject ID;
  a mismatch with the server session returns 409 before reading/writing private data.
- `/api/access` is removed. Provider profiles and emails do not select a local owner. Verified
  Auth subjects map to server-owned `account-<UUID>` namespaces; conversation addresses cannot
  contain a caller-supplied owner prefix. Same-origin checks, POST-only private APIs and the
  32 KiB streaming body cap remain in place. Generated app hosts are intentionally public.
- Each account stores one versioned model-preference row. Only explicit changes save it;
  loading defaults does not write. Saves are serialized within a tab; across devices the
  last completed save wins. Failed saves remain visible and can be retried; malformed or
  unsupported stored data fails without being overwritten. Accepted messages retain their
  original settings, including when a save happens during a run.
- `SendMessage` admits work to the framework's durable thread runtime and returns
  acceptance. Alarm execution survives a disconnected browser. A stable request ID
  deduplicates retries with the original admitted input, including model settings and any
  publication revision. Reusing the ID with changed input fails. Snapshot messages carry
  their request IDs so optimistic entries reconcile without matching message text; queued
  entries come from nonterminal submissions until their input enters the canonical log.
  The UI reconciles saved snapshots
  every two seconds. The current attempt reads its settings from its admitted submission,
  never from model-generated text or a later message joining the run.
- `/api/progress` streams schema-defined replacement frames for the selected conversation
  while work is pending. A bounded, temporary Object projection receives public text deltas
  (including the incrementally decoded `deliver_response.message`),
  native search events, and tool-handler start/end signals. Reasoning and raw tool contents
  are excluded. Observation reads that projection every 200 ms, deduplicates unchanged frames,
  and stops on disconnect. The client reconnects after transport loss without replaying text
  deltas or restarting work. Eviction may discard provisional text; saved records remain
  authoritative. Old JSON answers and new plain-text answers both retain their history.
- Each conversation uses its own Thread Object. A private, schema-encoded namespace RPC
  keeps the trip catalogue in the account's owner Object. Creating a trip records its
  conversation association atomically with the first revision. Cross-conversation edits
  are refused. This clean start has no shared-log seeding or legacy owner compatibility.
- Trips are schema-encoded, versioned, append-only SQLite rows. Updates require the
  current revision. The RPC and native agent tools call the same repository and
  publication service. Corrupt or unsupported stored values fail without resetting data.
  Notes allow up to 4,000 characters each so researched options retain their source links.
- Each published revision gets its own immutable Git branch in the trip's Artifacts
  repository, containing `trip.json` and escaped, script-free `index.html`. The public
  `/trips/:id/:revision` page reads that branch; `/trip.json` exposes the same snapshot.
  Signed-in members with the link can read all fields, including notes. Drafts remain private.
- Legacy shared snapshot publication requires a grant bound to the selected trip's admitted revision,
  from a complete command such as “Publish this trip” or “Please share my trip website.”
  Browser content and model prose cannot grant publication. The explicit UI action
  authorizes only the revision it displays. This example never books or buys travel.
- The host's native web search finds real travel and lodging links. Cloudflare Browser
  Run inspects public HTTPS listing and destination hosts, including Airbnb, Vrbo, Booking.com,
  Wikivoyage, and direct property managers. There is no site allowlist. It captures at most 512 KiB and returns
  at most 12 KiB of focused excerpts and photo references. Up to four image references
  are extracted from the already captured page Markdown, with a 4 KiB metadata cap;
  this adds no browser requests and excludes obvious navigation/profile imagery.
  Sites that omit photos from their page Markdown may still require opening the listing.
  Blocked pages and access challenges produce typed
  failures; the agent can use another source and distinguishes source evidence from
  verified availability. There are no logins, bookings, or access-control bypasses.
  Page content is untrusted. Each selectable-model response permits four native search calls;
  the legacy model registration retains its original one-call limit for admitted work. Runs
  allow twelve turns, eighteen tool calls, 64,000 tokens, and two minutes, with one tool
  at a time. Each page inspection has a 25-second timeout. Ordinary tools with uncertain outcomes are
  not automatically replayed after ownership loss.
- Native search status is retained in the trace. A provider search that ends without completing
  is shown as **Not completed**, separately from an explicit failure. This can happen when
  the provider reaches its per-response tool allowance; a tool-result event alone does not
  establish that the search succeeded. Canonical activity already persists in Durable Object
  storage, so debugging does not require a duplicate trace database.
- Activity expands each canonical model turn, tool call, result, request settings, `RunFailed` record,
  and settlement diagnostic, with run/turn/call IDs and token usage. Browser and model failures also
  record redacted original causes, stack traces, request URLs/options, available provider bodies,
  HTTP status, request identifiers, and timeout/limit information. Browser API HTTP status is labeled
  separately from destination-page evidence: challenge text alone does not establish an HTTP 403.
  Empty pages, not-found text, access challenges, timeouts, and URL-policy rejections are distinguished.
- Detailed failures are schema-versioned, append-only rows in each Thread Object's SQLite database;
  they survive reloads, eviction, and model-context compaction. They are operator diagnostics, never
  model prompt history or execution/recovery authority. Source-owned worker inspection guards scout
  and editor reads; the planner's session authorization still applies. No diagnostic data is published
  into generated trip sites. Canonical tool results remain unchanged. The latest 100 diagnostic rows
  are considered for each activity view; the view shows the latest 100 events (40 for workers).
  Older diagnostic rows remain stored. Individual failure details retain up to 65,536 characters,
  with bounded nesting/collections; ordinary details retain 16,384 characters. Limits are marked.
  Credentials, cookies, signed URL secrets, and private provider reasoning are removed before storage.
  Recording is best effort, capped at two seconds, and cannot change the original operation outcome;
  if storage fails, redacted diagnostic data falls back to Worker logs. A crash can precede recording;
  replacement attempts may record another observation. Already discarded historical detail cannot
  be recovered. UTC timestamps and T+ elapsed times come from the journal; recorded step intervals
  include orchestration and storage. Live response and tool timers reset after restart. Estimated cost is shown
  as unavailable until a pricing policy is configured; it is never presented as travel
  spend. Failed or stopped requests also appear in the conversation so a timeout never
  silently drops a reply. The app does not log raw credentials or private provider reasoning.

## Editable trip apps

The starter is a real monorepo: shared Effect Schemas and HttpApi, an `effect-cf` server,
and React/Effect Atom frontend. The server receives a fixed `TRIP_DATA` service binding.
Its API exposes the selected trip's title, summary, dates, travelers, days, stays, and ordered
map locations. Raw notes and conversation history are excluded. Map locations use saved
coordinates supplied by research or the user; itinerary lines do not claim driving directions.
The Leaflet/OpenStreetMap view preserves attribution and shows an empty state without locations.

The planner delegates app work through the native background-worker start/follow-up tools;
source-editing tools are available only to the registered editor. Later changes reuse its worker
when available. The editor captures the message's model settings and has a separate bounded
run budget (48 turns, 96 tools, fifteen minutes for newly created workers). Worker admission and canonical lineage establish
the source account; each tool is restricted to the admitted trip. Parent completion does not
cancel the editor. The Cloudflare host resumes durable work after eviction, while unresolved
ordinary tool outcomes retain the framework's protection against automatic replay. Existing
accepted planner versions remain registered so deployment does not discard pending work.

Source lives in a native Artifacts fork of `trip-app-template-v1`, with parentful Git history.
`read_trip_app_files` reads bounded files; `edit_trip_app` commits complete changed files
against an expected SHA. Conflicting edits fail without force-pushing. Source is limited to
100 files, 128 KiB per file, and 2 MiB total. Paths exclude traversal and reserved build directories.

Each committed edit starts a deterministic Workflow. The host transfers source into a fresh
scoped Sandbox, runs `vp install --ignore-scripts`, `vp check --no-fmt`, and `vp run build`,
then destroys it. The container has no account, model, Auth, or Git credentials. Each builder
uses explicit resources matching `standard-3` (2 vCPUs, 8 GiB RAM, 16 GB disk), with capacity
capped at four containers. Deployment verification checks the returned resource values;
the named instance tier alone did not change the allocation through the current provider.
If all are busy, startup waits with bounded backoff
and records that it is waiting for a builder. Only the initial directory creation is retried;
source writes and build commands are not replayed on ambiguous Sandbox errors. Cancellation
still releases the container. Startup failures identify the operation or capacity limit
without exposing the raw provider error or blaming the app's source.
Build output must include `dist/web/index.html` and `dist/server/index.js`, with at most 200 files,
4 MiB per file, and 24 MiB total. R2 stores immutable objects and commits the validated
manifest last. Failed builds retain the last working version and show bounded diagnostics.
Retry reuses a valid completed build; an obsolete Workflow cannot activate over newer edits.
Starting a build does not finish the user's other requests. The planner completes or
delegates the remaining research before replying. Scouts report findings automatically,
and the planner saves useful results while the build proceeds.

The compact app activity dock stays above the message input. Tap it for an animated dialog
with editor text, tool progress, expandable trace details, recorded build stages, timestamps,
errors, and version controls; on phones the dialog
uses nearly the full visible screen. Progress records actual starting, dependency installation,
code checks, compilation, upload, and completion boundaries. It does not estimate percentages
or expose private model reasoning. The latest 40 events for the requested build are saved
with the app's existing append-only revision checks, survive reload, and reject stale builds.
Editor progress comes from its own thread and does not keep the parent conversation busy.
The latest task and worker reference are projected from canonical source records. A temporarily
unavailable observer leaves saved app state intact; failed editor runs are shown separately from
build failures. Editor history is bounded to 40 displayed events and excludes credentials and
private provider reasoning. There is no automatic completion message injected into the parent;
the dock shows the current editor/build state.
Older app records without progress remain readable. Retry and code edits begin a fresh timeline.

The public gateway resolves app ownership through a private R2 address directory and confirms
the app and trip against the owner repository before serving any file or API request. It never
accepts an owner ID from request headers, cookies, or query parameters. New links use a readable
trip-title slug and a stable collision suffix; the original hash hostname remains an alias.
Directory entries use conditional immutable writes and verify the result, so collisions fail
without changing ownership and interrupted registrations can be retried. They never become
public assets. Dynamic Workers receive only the fixed trip service, no owner namespace or host secrets,
and have outbound server networking disabled. Browser origins are separate per app and
from the planner. API requests strip cookies and Access assertions before dispatch. Current
app APIs are read-only; generated redirects, cookies, and caller-selected trip IDs cannot
expand the data binding. Anyone with the app URL can read its live trip projection; raw notes,
conversations, source editing, and account settings stay private.
The public app URL is distinct from the old authenticated `/trips` snapshot.

After cutover, new app IDs, repositories, builds and URLs persist normally within the new
account namespaces. Missing or malformed directory entries never trigger an owner search.
Generated app hosts have no login wall; private planner routes require Auth sessions.

App state is schema-versioned append-only owner SQL with revision checks. Source SHA,
active build SHA, and trip-data revision are separate. Restore commits the selected version's
source as a new revision and rebuilds it; subsequent edits start from that restored tree.
The current trip data is unchanged. The native card keeps the app discoverable without relying
on model Markdown. New snapshot links retain their authenticated access rules; pre-cutover snapshots are reset.

## Verification and scope

```sh
vp run -F @effect-agent/example-travel-planner check
vp run -F @effect-agent/example-travel-planner test
vp run -F @effect-agent/example-travel-planner build
```

Test-only scripted models exercise real RPC, Durable Object SQL, native tool calls,
revision conflicts, publication authorization, restart persistence, and crash seams.
Gated Workerd tests verify text before completion, tool start/end, observer cancellation,
reconnect, and tenant isolation. Selectable models request sequential function calls. The
runtime allows completion alongside terminal hosted-tool results, including OpenAI web search.
Completion must remain the sole application tool call. Mixed application batches are rejected
before their handlers start and can be corrected within the existing budgets while preserving
hosted results. Missing or mismatched hosted results still fail closed; rejected application
calls are never replayed as executed tools. Provider transport tests verify selected model parameters
and exclude reasoning from progress. The stream body owns and releases its observation
handler; closing it does not abort a durable request.
Native-card tests cover early display, isolation, and restart persistence. Navigation tests
cover immediate cached returns, delayed and failed loads, inactive polling, and identity
changes, including same-account session revalidation without clearing the loaded conversation. Authentication failures still clear data and changed accounts remain isolated. Auth tests use real Workerd SQL and HTTP cookies with deterministic email and GitHub transports, including first registration, returning signin, proof expiry/reuse/attempt limits, delivery ambiguity, callback rejection, CSRF/body limits, account isolation and schema fault seams. Cloudflare sender tests cover acceptance, rejection, timeout and interruption without real mail. Rendering tests cover legacy Markdown photos, escaped model content, and unsafe URLs.
They substitute the model and Git service, so live provider, browser, and Artifacts
verification still requires a deployed run. The application and Git adapters expose
failpoints before and after durable mutations for deterministic fault tests.

Public signup has no administrator role. The UI has a bounded transcript view and a simple
itinerary template. Progressive tool discovery, Code Mode comparisons, durable child
research agents, steering controls, approvals beyond publication, scheduled rechecks,
memory controls, priced usage, and workload performance reports remain follow-up work.
Existing release and performance harnesses live under [`tooling/`](../../tooling/),
separate from this sole runnable example.
