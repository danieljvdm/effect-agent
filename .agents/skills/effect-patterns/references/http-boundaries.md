# Effect HTTP Boundaries

Use this when changing `HttpApi` contracts, handlers, DTOs, or route boundary
code.

## Contracts

Prefer one endpoint per file.

The endpoint file declares route params, query, payload, success, and error
schemas inline. Group and index files compose or re-export only.

Do not create endpoint DTO bundle files unless multiple endpoints share a real
transport type.

## Handlers

Handlers are thin request-boundary adapters.

A handler should decode transport inputs, call one service workflow, and map the
result to the response DTO.

Business orchestration, persistence, rollback, and cross-service coordination
belong in services, not endpoint files.

Do not provide app service layers inside endpoint files unless there is a
specific transport-only dependency.

## DTOs

Request DTOs describe the route contract explicitly.

Response DTOs describe the wire shape explicitly. If clients see `id`, the DTO
uses `id`; service models can keep `publicId`.

Use small route-layer mappers for service model to wire DTO conversion.

Prefer named nested DTOs when fields are reused or transport-sensitive.

## Errors

Expected HTTP failures belong in endpoint `error:` schemas.

Prefer tagged or schema errors with HTTP status metadata.

Success schemas describe success only. Do not encode error bodies in `success`.

## Transport APIs

Use `HttpServerResponse` for upstream response passthrough, redirects, cookies,
or non-default success status/body behavior.

Do not use `HttpServerResponse.jsonUnsafe` for ordinary typed 4xx errors.

Prefer Effect request and cookie APIs over manual header or cookie parsing.
