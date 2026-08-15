# Shared contracts

Use a runtime-neutral domain package as the protocol boundary shared by the
server and every typed client.

## Package shape

```text
packages/domain/
├── package.json
└── src/
    ├── models/
    │   ├── ids.ts
    │   ├── errors.ts
    │   └── views.ts
    └── http/
        ├── endpoints/
        │   ├── get-project.ts
        │   └── update-project.ts
        ├── authorization.ts
        ├── project-api.ts
        ├── application-api.ts
        └── index.ts
```

Export the root API, contract-visible middleware declarations, and any genuinely
reusable schemas. Keep middleware implementations, server handlers, React,
database clients, secrets, platform bindings, and runtime layers out of this
package. A workspace export map can expose `./http`, `./models`, and endpoint
subpaths when consumers need them.

## Model the wire

Use schemas for every external value. Prefer reusable named classes for DTOs,
branded scalar schemas for identifiers, and serializable tagged errors with
HTTP status metadata for expected failures.

```ts
import { Schema } from "effect";
import { HttpApiSchema } from "effect/unstable/httpapi";

export const ProjectId = Schema.String.pipe(Schema.brand("ProjectId"));
export type ProjectId = typeof ProjectId.Type;

export class ProjectDto extends Schema.Class<ProjectDto>("ProjectDto")({
  id: ProjectId,
  name: Schema.NonEmptyString,
}) {}

export class ProjectNotFoundError extends Schema.TaggedError<ProjectNotFoundError>()(
  "ProjectNotFoundError",
  { id: ProjectId },
) {}

export const ProjectNotFound = ProjectNotFoundError.pipe(HttpApiSchema.status("NotFound"));
```

Keep success schemas limited to successful values. Put every expected 4xx/5xx
failure in `error`; defects remain defects. Reuse a tuple of transport errors
only when the same set is genuinely standard across endpoints.

Make DTOs describe the wire exactly. If clients receive `id`, expose `id` even
when a persistence model uses `publicId`; map between them in a small boundary
function. Keep endpoint-specific request DTOs beside their endpoint and move a
DTO into shared models only when multiple contracts reuse the same concept.

## Define one endpoint per file

An endpoint declares the complete request and response contract inline.

```ts
import { HttpApiEndpoint } from "effect/unstable/httpapi";
import { ProjectDto, ProjectId, ProjectNotFound } from "../../models";
import { RequestHeaders } from "../headers";

export const GetProjectEndpoint = HttpApiEndpoint.get("getProject", "/projects/:projectId", {
  params: { projectId: ProjectId },
  headers: RequestHeaders,
  success: ProjectDto,
  error: ProjectNotFound,
});
```

Use the operation identifier as a stable protocol name: handlers and generated
clients address it directly. Represent optional query values with the installed
version's optional schema operator, such as `Schema.optionalKey`.

If headers use `Schema.Class`, generated client input may require a constructed
class instance rather than a structurally similar object. Encode a representative
instance in a test so this contract stays explicit.

## Compose groups and the root

```ts
import { HttpApi, HttpApiGroup } from "effect/unstable/httpapi";
import { GetProjectEndpoint, UpdateProjectEndpoint } from "./endpoints";

export const ProjectsApi = HttpApiGroup.make("projects")
  .add(GetProjectEndpoint, UpdateProjectEndpoint)
  .prefix("/projects");

export const ApplicationApi = HttpApi.make("application").add(ProjectsApi).prefix("/api");
```

Group and root files compose; endpoint files specify. Re-export through index
files so server and clients import the same `ApplicationApi` value. A contract
change is complete only after all affected handlers, callers, tests, and
OpenAPI output agree with it.
