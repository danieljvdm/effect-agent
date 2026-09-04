# Durable Objects

Identify the object's identity, coordination scope, durable state, and callers. Preserve existing class and storage migration history.

## State and concurrency

Choose an object boundary around the entities that need coordination. Separate
independent entities when scale requires it; a bounded singleton can still be an
intentional coordinator. Evaluate its traffic and responsibilities before
calling it a bottleneck.

Use durable storage for state that must survive eviction or failure. Memory may
cache durable values but cannot be the only record of acknowledged durable work.
Prefer SQLite for new objects when appropriate; retain existing migration history
and plan a storage migration explicitly for deployed classes.

Understand input and output gates for the storage APIs in use. Synchronous
SQLite operations without an intervening await can have different guarantees
from operations separated by asynchronous work. Use a transaction for the
required atomic unit. Avoid a blanket rule that every await breaks atomicity.

Keep `blockConcurrencyWhile` short and tied to initialization or a specific
invariant. Avoid external network work while holding it. Reconstruct necessary
state after eviction and make initialization safe to repeat.

Check schema changes against existing data, including partially completed
upgrades when relevant. Consult the [rules of Durable Objects](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
and the installed storage API before relying on concurrency guarantees.

## Calls and lifecycle

Use typed RPC for service calls where supported. Retain HTTP handling when the
transport requires it, such as an incoming WebSocket upgrade. Keep caller
authentication and authorization at the actual trust boundary.

Route repeat calls using the same intended object identity. Persist any mapping
needed to recover an opaque generated ID. Changing naming or namespaces can
route traffic to a different object with different storage.

An object has one alarm time; setting it replaces the previous alarm. If the
application owns multiple deadlines, keep those deadlines in durable state and
schedule the next due time. Make alarm work safe under retries and interrupted
execution. External side effects need their own deduplication strategy.

Use hibernating WebSockets when that fits the connection lifecycle. Reconstruct
connection metadata from durable state or supported attachments after waking.
Do not assume a constructor, in-memory map, or ordinary timer survives eviction.
Handle close and error paths without leaving stale application presence.

Inspect the relevant [alarm](https://developers.cloudflare.com/durable-objects/api/alarms/)
or [WebSocket](https://developers.cloudflare.com/durable-objects/best-practices/websockets/)
documentation when modifying those behaviors.

## Verify the boundary

Follow repository policy for test placement and whether a committed regression
is warranted. Reuse the existing Worker test environment and generated bindings.
Avoid creating a standalone test configuration beside an established one.

Choose verification based on the changed behavior: RPC response and failure,
durable data after a new instance, retry-safe alarm handling, or connection
state after hibernation. Include real migrations when schema behavior matters.
In-memory mocks alone cannot establish platform storage or lifecycle semantics.

Use [Cloudflare's testing guidance](https://developers.cloudflare.com/durable-objects/testing/)
for runtime-specific helpers, checked against installed versions. Exercise the
running application when tests are not warranted. State when eviction, retries,
or multi-request coordination remain unverified.
