# Travel planner

The canonical runnable Effect Agent example: a small travel chat, saved trips, and
standalone trip websites on Cloudflare. This is KOM-173's first vertical slice.

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
to a trip restores that conversation's messages. The first message reserves a private
sidebar entry before agent admission, independently of whether the model saves trip details.
Unfinished and failed conversations remain reachable after reload or restart. Entries initially
use the first message as their title; saved trip details replace that label without a duplicate.
A failed admission can leave an empty, retryable conversation; retries retain its original entry.
Existing saved trips remain discoverable without rewriting their conversation associations.
The composer stays at the bottom of the
viewport while the conversation scrolls. Search results and saved source URLs are clickable.
Replies arrive as live text. The expandable activity chip keeps a stable step count;
current work and public progress appear in its details and the response.

The planner can send up to six focused research scouts into the background while asking about
your preferences. Later details steer those same durable workers. Completed findings return
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
messages on existing trips use v11; new workers receive the expanded allowance. No worker
or conversation history is reset by the upgrade.

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
partitioned by verified email; only the selected conversation is polled, with the next refresh
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
work already running. The default remains Luna, Low reasoning, Standard speed.

## Run

From the repository root:

```sh
vp install
cp examples/travel-planner/.env.example examples/travel-planner/.env
```

Set `OPENAI_API_KEY` in that ignored file and configure the Cloudflare Access values
shown in `.env.example`. The deployed app is at <https://travel.effect-agent.com>.
Cloudflare handles email-code sign-in; there is no application access key. Each email
has private trips and conversations. Daniel's existing catalogue and history keep
their original storage addresses.

```sh
vp run -F @effect-agent/example-travel-planner dev
```

The Alchemy CLI prints the local URL. Local requests also require a valid Access assertion;
deterministic tests substitute authentication only in their test Worker. There is no
production development bypass. Cloudflare credentials must target the account
that will host the application. Artifacts and Browser Run require account access to
those products. The planner uses your OpenAI API key for every conversation.

Model selection and the provider's native web-search tool stay at the host boundary.
The agent accepts a research toolkit and a provider-independent model Layer.
The selectable models support native web search. `OPENAI_MODEL` is retained for the previous
registration so already accepted work can finish with its original model after deployment.
Page inspection accepts newly discovered HTTPS websites without a travel-site allowlist,
including tourism boards, campsites, and independent hotels. URL checks reject IP literals,
local hostnames, embedded credentials, and custom ports, and also apply to browser redirects
and subresources. These are URL checks, not a DNS-resolution firewall. Page-size limits,
timeouts, and access-challenge handling still apply.

## Deploy with Alchemy

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
Alchemy owns the custom domain and Access application/policy covering both the custom
domain and original `workers.dev` hostname. Trip apps have separate origins at
`{trip-title}-{stable-suffix}-trip.effect-agent.com`. Generated sites are public and do not
use Cloudflare Access; the planner retains its invited-user, 30-day Access policy. Existing
`{appId}-trip.effect-agent.com` links remain valid.
A proxied wildcard DNS record supplies otherwise unmatched names; only the narrow
`*-trip.effect-agent.com/*` Worker route handles apps. Existing exact DNS records take
precedence. Version preview URLs are disabled.

Secrets are bound through Alchemy's redacted configuration. Deployment requires nonempty
`OPENAI_API_KEY` and `ACCESS_API_TOKEN`; missing or invalid Access configuration fails closed. The application
compatibility date matches the runtime shipped by the pinned Alchemy version.

## Invitations

`danieljmerwe@gmail.com` is the sole administrator and is always allowed by the Access
policy. **Manage access** adds and removes verified email addresses in the dedicated
`effect-agent-travel-planner-invited` Access group. Share the displayed sign-in link with
invitees; adding an address does not send an invitation email. Each invitee starts with
an empty private planner. Removing an address prevents its next sign-in; existing Access
sessions may remain valid for the configured 30 days. The administrator cannot be removed.

The group is bootstrapped once and its ID retained in `ACCESS_GROUP_ID`: the pinned
Alchemy Group resource replaces membership during reconciliation, so the app owns this
mutable list instead. Deployments reference it without resetting invitations. Runtime
management requires `ACCESS_API_TOKEN`, restricted to **Access: Groups Write** in this
Cloudflare account. Cloudflare cannot restrict that token to one group; the application
uses only the configured group ID and refuses unexpected group names or rule shapes.
Deployments require this credential so the admin panel cannot silently ship without working
invitations. An older deployment with missing configuration reports that no access was changed.

Group updates are serialized in the original owner Object. Cloudflare remains the source
of truth, and failed updates are not automatically retried: refresh before retrying because
a lost response may follow a committed update. **Refresh members** reloads the authoritative
list without repeating the invitation. Tests exercise both sides of this mutation and the
RPC-to-Object-to-HTTP path in workerd. Credentialed requests use manual redirects and reject
non-success statuses; the token is never forwarded to a redirect destination.

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
- The Worker validates the Access JWT signature, issuer, audience, expiry, and email
  before serving planner routes, including its assets and legacy published snapshots.
  Generated app hosts are public and resolve through a private, schema-validated address directory. It ignores unverified
  email headers and old bearer keys. `/api/rpc`, `/api/access`, and `/api/progress` check same-origin browser
  requests, method, and a 32 KiB body limit. Effect HTTP serves
  schema-defined RPCs: `GetPlanner`, `SendMessage`, `SaveTrip`, `PublishTrip`,
  `GetPlannerSettings`, `SavePlannerSettings`, `CreateTripApp`, `RetryTripAppBuild`, and `RestoreTripApp`.
- `/api/access` also serves the signed-in session and administrator-only membership RPCs.
  Identity is request-scoped. Member storage addresses derive from the verified email,
  never a client-supplied owner ID; a member cannot address another member's private trips.
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
  keeps the trip catalogue in the original owner Object. Creating a trip records its
  conversation association atomically with the first revision. Cross-conversation edits
  are refused. Existing trips retain their data and trip-specific messages from the old
  shared log; those messages seed the first isolated run without rewriting the original log.
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
  Run inspects approved listing and destination hosts, including Airbnb, Vrbo, Booking.com,
  Wikivoyage, and selected direct property managers. It captures at most 512 KiB and returns
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
- Activity expands each canonical model turn, tool call, result, request settings, and failure diagnostic, with run/turn/call IDs and token usage. The latest 100 events are shown; individual detail sections are capped at 16,384 characters and marked when truncated. Credentials, raw provider errors, and private provider reasoning are excluded. UTC timestamps and T+ elapsed times come from the journal; recorded step intervals include orchestration and storage, rather than claiming model-only or handler-only latency. Live response and tool timers measure the current attempt and reset after restart. Estimated cost is shown
  as unavailable until a pricing policy is configured; it is never presented as travel
  spend. Failed or stopped requests also appear in the conversation so a timeout never
  silently drops a reply. The app does not log private request bodies or provider error responses.

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
then destroys it. The container has no account, model, Access, or Git credentials. Each builder
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
conversations, source editing, account settings, and invitation administration stay private.
The public app URL is distinct from the old authenticated `/trips` snapshot.

Existing app IDs, repository names, build objects, and saved URLs are preserved. Original
administrator hash links can repair their directory entry from the fixed original owner on the
first public request. Other members' existing entries are repaired when their planner loads or
a build starts; load those planners before sharing their existing links after the first rollout.
Missing or malformed entries never trigger an owner search. The deployment removes only the
`TripAppsAccess` application; `TravelAccess`, invitations, and session duration remain unchanged.

App state is schema-versioned append-only owner SQL with revision checks. Source SHA,
active build SHA, and trip-data revision are separate. Restore commits the selected version's
source as a new revision and rebuilds it; subsequent edits start from that restored tree.
The current trip data is unchanged. The native card keeps the app discoverable without relying
on model Markdown. Legacy snapshot links remain clickable and retain their existing access rules.

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
changes, including same-account session revalidation without clearing the loaded conversation. Authentication failures still clear data and changed accounts remain isolated. Rendering tests cover legacy Markdown photos, escaped model content, and unsafe URLs.
They substitute the model and Git service, so live provider, browser, and Artifacts
verification still requires a deployed run. The application and Git adapters expose
failpoints before and after durable mutations for deterministic fault tests.

The initial UI deliberately has one administrator, a bounded transcript view, and a simple
itinerary template. Progressive tool discovery, Code Mode comparisons, durable child
research agents, steering controls, approvals beyond publication, scheduled rechecks,
memory controls, priced usage, and workload performance reports remain follow-up work.
Existing release and performance harnesses live under [`tooling/`](../../tooling/),
separate from this sole runnable example.
