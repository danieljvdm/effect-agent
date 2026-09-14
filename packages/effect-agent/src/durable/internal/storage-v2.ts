import { Schema } from "effect";

import {
  ScheduleConfiguration,
  ScheduledEnvelope,
  ScheduleRecord,
  ScheduleRetry,
  ScheduleInstant,
} from "../Schedule.ts";
import {
  PreparedInput,
  SubscriptionConfiguration,
  SubscriptionDelivery,
  SubscriptionRecord,
} from "../Subscription.ts";

// Frozen beta49/beta50 persisted shapes, before 75898aef (#341). Decode with
// onExcessProperty:error: a patched or partially upgraded v2 store is not this contract.
const withoutAdmission = <F extends Schema.Struct.Fields>(fields: F) => {
  const { admissionGroup: _group, admissionFence: _fence, ...rest } = fields;

  return rest;
};

const {
  generation: _generation,
  automaticAttempts: _automatic,
  parked: _parked,
  ...retry
} = ScheduleRetry.fields;

export const V2Retry = Schema.Struct(retry);
const V2ScheduleConfiguration = Schema.Struct(withoutAdmission(ScheduleConfiguration.fields));
const V2Envelope = Schema.Struct(withoutAdmission(ScheduledEnvelope.fields));

export const V2Schedule = Schema.Struct({
  ...ScheduleRecord.fields,
  configuration: V2ScheduleConfiguration,
  pending: Schema.NullOr(Schema.Struct({ envelope: V2Envelope, retry: V2Retry })),
});

const V2Configuration = Schema.Struct({
  ...withoutAdmission(SubscriptionConfiguration.fields),
  expiresAtMillis: ScheduleInstant,
});

const {
  configurationRevision: _revision,
  configurationFingerprint: _fingerprint,
  creationConfiguration: _creation,
  ...registration
} = SubscriptionRecord.fields;

export const V2Subscription = Schema.Struct({
  ...registration,
  configuration: V2Configuration,
  state: Schema.Literals(["active", "consumed", "cancelled"]),
});

const {
  configurationRevision: _deliveryRevision,
  configuration: _configuration,
  observeSettlement: _observe,
  settledAtMillis: _settled,
  completedAtMillis: _completed,
  ...delivery
} = SubscriptionDelivery.fields;

export const V2Delivery = Schema.Struct({
  ...delivery,
  envelope: Schema.NullOr(Schema.Struct(withoutAdmission(PreparedInput.fields))),
  retry: V2Retry,
});
