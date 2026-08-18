import { ProposedWorkOrder, SettledWorkOrder } from "@effect-agent/example-pr-work-orders";
import { Config, Effect, FileSystem, Path, Schema } from "effect";

import {
  CheckedWorkOrder,
  WorkOrderActionFailure,
  WorkOrderAdmission,
  WorkOrderTerminal,
} from "./action-contracts.ts";

const phaseFiles = {
  admission: "admission.json",
  proposal: "proposal.json",
  settlement: "settlement.json",
  checked: "checked.json",
  implementationTerminal: "implementation-terminal.json",
  checksTerminal: "checks-terminal.json",
  publicationTerminal: "publication-terminal.json",
} as const;

type PhaseFile = keyof typeof phaseFiles;

const artifactDirectory = Config.nonEmptyString("EFFECT_AGENT_ARTIFACT_DIRECTORY").pipe(
  Config.withDefault(".effect-agent-work-order"),
);

const failure = (operation: string, cause: unknown) =>
  WorkOrderActionFailure.make({
    phase: "present",
    errorTag: "ArtifactFailure",
    detail: `${operation}: ${String(cause)}`.slice(0, 2_048),
  });

export const artifactPath = Effect.fn("artifactPath")(function* (name: PhaseFile) {
  const path = yield* Path.Path;
  return path.join(yield* artifactDirectory, phaseFiles[name]);
});

type ArtifactValue =
  | WorkOrderAdmission
  | ProposedWorkOrder
  | SettledWorkOrder
  | CheckedWorkOrder
  | WorkOrderTerminal;

const encodeArtifact = (name: PhaseFile, value: ArtifactValue) => {
  switch (name) {
    case "admission":
      return Schema.encodeUnknownEffect(Schema.fromJsonString(WorkOrderAdmission))(value);
    case "proposal":
      return Schema.encodeUnknownEffect(Schema.fromJsonString(ProposedWorkOrder))(value);
    case "settlement":
      return Schema.encodeUnknownEffect(Schema.fromJsonString(SettledWorkOrder))(value);
    case "checked":
      return Schema.encodeUnknownEffect(Schema.fromJsonString(CheckedWorkOrder))(value);
    case "implementationTerminal":
    case "checksTerminal":
    case "publicationTerminal":
      return Schema.encodeUnknownEffect(Schema.fromJsonString(WorkOrderTerminal))(value);
  }
};

export const writeArtifact = Effect.fn("writeArtifact")(function* (
  name: PhaseFile,
  value: ArtifactValue,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* artifactDirectory;
  yield* fs
    .makeDirectory(directory, { recursive: true })
    .pipe(Effect.mapError((cause) => failure("create artifact directory", cause)));
  const encoded = yield* encodeArtifact(name, value).pipe(
    Effect.mapError((cause) => failure(`encode ${name} artifact`, cause)),
  );
  const target = path.join(directory, phaseFiles[name]);
  const next = `${target}.next`;
  yield* fs.writeFileString(next, encoded).pipe(
    Effect.andThen(fs.rename(next, target)),
    Effect.mapError((cause) => failure(`write ${name} artifact`, cause)),
  );
});

const readWithSchema = Effect.fn("readArtifact")(function* <S extends Schema.Top>(
  name: PhaseFile,
  schema: S,
) {
  const fs = yield* FileSystem.FileSystem;
  const target = yield* artifactPath(name);
  const text = yield* fs
    .readFileString(target)
    .pipe(Effect.mapError((cause) => failure(`read ${name} artifact`, cause)));
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError((cause) => failure(`decode ${name} artifact`, cause)),
  );
});

export const readAdmissionArtifact = readWithSchema("admission", WorkOrderAdmission);
export const readProposalArtifact = readWithSchema("proposal", ProposedWorkOrder);
export const readSettlementArtifact = readWithSchema("settlement", SettledWorkOrder);
export const readCheckedArtifact = readWithSchema("checked", CheckedWorkOrder);
export const readTerminalArtifactOption = Effect.fn("readTerminalArtifactOption")(function* () {
  const fs = yield* FileSystem.FileSystem;
  for (const name of ["publicationTerminal", "checksTerminal", "implementationTerminal"] as const) {
    const target = yield* artifactPath(name);
    if (yield* fs.exists(target)) return yield* readWithSchema(name, WorkOrderTerminal);
  }
  return undefined;
});
