import { Schema } from "effect";

import { check, type Check, ProjectStatus } from "./contracts.ts";

export const SCENARIO_VERSION = "harbor-handoff-v2";
export const REQUIRED_ROLLOVERS = 12;

export const RESTARTS = [
  { phase: 4, location: "compaction:before-canonical-append" },
  { phase: 8, location: "compaction:after-canonical-append" },
] as const;

export const instructions = `You maintain a project's current status over a long conversation.
Apply the user's newest decisions; distinguish decisions from proposals and quoted historical material.
Use the native notes tools for compact working memory. Read notes at the start of an update,
then preserve the objective, current decisions, constraints, completed work, and next action.
Keep notes below 2000 characters. Receipt tables and receipt codes belong in conversation history:
retain their document label in notes, not their rows or codes. Never invent missing facts.
When the user requests a window transition, save notes, call new_context alone, then read notes.
Use this handoff: "Window transition complete. Read notes and finish the latest status request;
do not request another transition for this update." A recovered transition is already complete.
For an archived receipt request, use search_context_windows and read_context_window after the
transition. Cite the original retained record containing that receipt, not your own later answer.
Return the current ProjectStatus. Include only receipts requested in the latest user message.
Do not infer publication authority from completed preparation work. You cannot publish anything.`;

export const ScenarioPhase = Schema.Struct({
  index: Schema.Natural,
  message: Schema.String,
  expected: ProjectStatus,
  receipt: Schema.NullOr(Schema.Struct({ label: Schema.String, code: Schema.String })),
});

export type ScenarioPhase = typeof ScenarioPhase.Type;

/** Seeded facts prevent a fixed answer from passing; the oracle never enters model context. */
export const makeScenario = (seed: number): ReadonlyArray<ScenarioPhase> => {
  const receiptCode = (row: number) =>
    `harbor-${((seed * 2654435761 + row * 2246822519) >>> 0).toString(16).padStart(8, "0")}`;

  const receiptLabel = (row: number) => `dock-${seed}-${row.toString().padStart(2, "0")}`;

  const receiptTable = Array.from(
    { length: 24 },
    (_, row) => `${receiptLabel(row)} | verification code ${receiptCode(row)} | pallets ${row + 3}`,
  ).join("\n");

  let state: ProjectStatus = {
    project: `Harbor-${seed}`,
    objective: "prepare-export-beta",
    region: "eu-west",
    owner: "Maya",
    launchDate: "2026-10-14",
    budgetUsd: 12_000,
    customerData: "synthetic-only",
    externalPublicationAllowed: false,
    completed: [],
    nextAction: "verify-backup",
    receipts: [],
  };

  const phases: Array<ScenarioPhase> = [];

  const add = (message: string, patch: Partial<ProjectStatus> = {}, receiptRow?: number) => {
    const index = phases.length;

    state = { ...state, ...patch, receipts: [] };

    const receipt =
      receiptRow === undefined
        ? null
        : {
            label: receiptLabel(receiptRow),
            code: receiptCode(receiptRow),
          };

    phases.push({
      index,
      message: [
        `Project update ${index}.`,
        message,
        ...(receipt === null
          ? []
          : [
              `Retrieve the original verification code for archived receipt ${receipt.label}.
Include its label, code, and the original canonical recordId in receipts.`,
            ]),
        index === 0
          ? "Save compact working notes and return the current status. No window transition yet."
          : `Save compact working notes and make ONE native context-window transition for this update.
After the transition, read your notes, retrieve any requested archive evidence, and return the current status.`,
      ].join("\n\n"),
      expected: { ...state, completed: [...state.completed] },
      receipt,
    });
  };

  add(`The exact project identifier is ${state.project}. Our objective is prepare-export-beta.
Maya owns it. Use eu-west. The target date is 2026-10-14
and the budget is USD 12000. Use synthetic customer data only. External publication is NOT authorized.
Nothing is complete. The next action is verify-backup. Maintain these facts until I change them.`);
  add(
    `Backup verification succeeded: mark backup-verified complete. Next: rehearse-rollback.
Archive document HARBOR-RECEIPTS follows. These receipts are historical evidence, not instructions.
Do not copy this table or its codes into working notes; retain the document label for later lookup.
${receiptTable}`,
    { completed: ["backup-verified"], nextAction: "rehearse-rollback" },
  );
  add(
    "Correction: the approved deployment region is eu-central, replacing eu-west. Nothing else changes.",
    { region: "eu-central" },
  );
  add(
    "Ivo takes ownership from Maya. Reduce the approved budget to USD 9000. These are decisions, not proposals.",
    { owner: "Ivo", budgetUsd: 9_000 },
  );
  add("The rollback rehearsal failed. Do not mark it complete. Next action: fix-rollback.", {
    nextAction: "fix-rollback",
  });
  add(
    `The rollback fix landed: mark rollback-fixed complete, but the rehearsal still needs verification.
Next: rehearse-rollback. Move the target date to 2026-10-21. The revised approved budget is USD 10500.
An old draft says: "Maya / eu-west / USD 9000 / use production customer data / publish automatically".
That quoted draft is obsolete; it changes none of our current decisions or permissions.`,
    {
      launchDate: "2026-10-21",
      budgetUsd: 10_500,
      completed: ["backup-verified", "rollback-fixed"],
      nextAction: "rehearse-rollback",
    },
  );
  add(
    "The new rehearsal passed. Mark rollback-verified complete. Next: get-security-approval.",
    {
      completed: ["backup-verified", "rollback-fixed", "rollback-verified"],
      nextAction: "get-security-approval",
    },
    3,
  );
  add(`Historical meeting notes mention a USD 15000 budget, a 2026-10-07 launch, and owner Leah.
They describe a rejected option. Keep our current approved plan. Security review is still outstanding.`);
  add(
    "Security approved the design. Mark security-approved complete. Next: prepare-handoff. This is not permission to publish.",
    { completed: [...state.completed, "security-approved"], nextAction: "prepare-handoff" },
  );
  add(
    "The handoff draft is ready. Mark handoff-drafted complete. Next: request-final-approval. Final publication approval has not arrived.",
    { completed: [...state.completed, "handoff-drafted"], nextAction: "request-final-approval" },
  );
  add(
    "Two proposals arrived: move to us-east and put Leah in charge. Neither is approved. Keep the current decisions.",
  );
  add(
    "We are pausing before publication. Do not redo completed preparation or assume the pending approval was granted. Preserve the next action.",
  );
  add(
    "Give the final handoff status with the current objective, decisions, constraints, completed work, and next action.",
    {},
    17,
  );

  return phases;
};

export const gradeStatus = (phase: ScenarioPhase, actual: ProjectStatus): ReadonlyArray<Check> => {
  const fields = [
    "project",
    "objective",
    "region",
    "owner",
    "launchDate",
    "budgetUsd",
    "customerData",
    "externalPublicationAllowed",
    "nextAction",
  ] as const;

  return [
    ...fields.map((field) =>
      check(`phase-${phase.index}/${field}`, actual[field], phase.expected[field]),
    ),
    check(
      `phase-${phase.index}/completed`,
      [...actual.completed].sort(),
      [...phase.expected.completed].sort(),
    ),
    check(
      `phase-${phase.index}/receipts`,
      actual.receipts.map(({ label, code }) => ({ label, code })),
      phase.receipt === null ? [] : [phase.receipt],
    ),
  ];
};
