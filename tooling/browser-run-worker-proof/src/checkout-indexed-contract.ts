import { Schema } from "effect";

const Text = Schema.String.check(Schema.isMaxLength(2_048));

export const Operation = Schema.Literals([
  "CLICK",
  "TYPE",
  "SELECT",
  "LOGIN",
  "PRIMARY_CARD",
  "BACKUP_CARD",
  "WAIT",
  "APPROVAL",
  "HUMAN",
  "DONE",
  "BLOCKED",
]);

export type Operation = typeof Operation.Type;

/** The model sees indices and current state; selectors and live handles stay with the executor. */
export const IndexedControl = Schema.Struct({
  index: Schema.NonEmptyString,
  frame: Schema.Natural,
  document: Schema.NonEmptyString,
  node: Schema.NonEmptyString,
  kind: Text,
  label: Text,
  value: Text,
  checked: Schema.Boolean,
  disabled: Schema.Boolean,
  readOnly: Schema.Boolean,
  href: Text,
  form: Text,
  recipient: Text,
  autocomplete: Text,
  operations: Schema.Array(Operation),
  options: Schema.Array(
    Schema.Struct({ value: Text, label: Text, disabled: Schema.Boolean }),
  ).check(Schema.isMaxLength(64)),
});

export type IndexedControl = typeof IndexedControl.Type;

export const IndexedObservation = Schema.Struct({
  url: Text,
  frames: Schema.Array(
    Schema.Struct({
      index: Schema.Natural,
      url: Text,
      document: Schema.NonEmptyString,
      text: Schema.String.check(Schema.isMaxLength(24_000)),
    }),
  ).check(Schema.isMaxLength(8)),
  controls: Schema.Array(IndexedControl).check(Schema.isMaxLength(128)),
});

export type IndexedObservation = typeof IndexedObservation.Type;

export const ControllerInput = Schema.Struct({
  goal: Schema.String.check(Schema.isMaxLength(4_096)),
  approvalGranted: Schema.Boolean,
  observation: IndexedObservation,
  history: Schema.Array(Schema.Struct({ operation: Operation, target: Text, result: Text })).check(
    Schema.isMaxLength(10),
  ),
});

export type ControllerInput = typeof ControllerInput.Type;

export const SelectedAction = Schema.Struct({
  operation: Operation,
  target: Schema.optionalKey(Text),
});

export type SelectedAction = typeof SelectedAction.Type;
