import { Schema } from "effect";

export const objectName = "persisted-owner";
export const alarmId = "pending";
export const alarmTag = "restart";
export const Payload = Schema.Struct({ message: Schema.String });

export const Introspection = Schema.Struct({
  alarmDeliveries: Schema.Natural,
  completedAlarmDeliveries: Schema.Natural,
  handledEvents: Schema.Natural,
  objectRequests: Schema.Natural,
});

export const Status = Schema.Struct({
  objectName: Schema.String,
  deliveries: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      tag: Schema.String,
      message: Schema.String,
      scheduled_at: Schema.Number,
    }),
  ),
  pendingEvents: Schema.Natural,
  legacyTables: Schema.Natural,
  nativeAlarm: Schema.NullOr(Schema.Number),
});

export const Seeded = Schema.Struct({
  runAt: Schema.Number,
  pendingEvents: Schema.Natural,
  alarmDeliveries: Schema.Natural,
});
