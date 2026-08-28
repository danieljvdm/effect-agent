import { describe, expect, it } from "@effect/vitest";
import { Context } from "effect";
import type { Crypto, Effect, Layer, Schema } from "effect";

import {
  type DurableSubmitAgent,
  type ScheduleCreateOptions,
  type ScheduleAuthorizer,
  type ScheduleListOptions,
  type ScheduleManagementFailure,
  type ScheduleProcessFailure,
  type ScheduleScope,
  type ScheduleId,
  type ScheduledInputAdmission,
  Scheduling,
  ScheduleDriver,
  type ScheduleStorageError,
  type ScheduleStore,
  type ScheduleValidationError,
  type ScheduleWake,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

class InputEncodingContext extends Context.Service<
  InputEncodingContext,
  { readonly prefix: string }
>()("@effect-agent/session/test/InputEncodingContext") {}

declare const inputSchema: Schema.Codec<
  { readonly value: string },
  { readonly encoded: string },
  never,
  InputEncodingContext
>;
declare const agent: DurableSubmitAgent<typeof inputSchema>;
declare const createOptions: ScheduleCreateOptions;
declare const scope: ScheduleScope;
declare const scheduleId: ScheduleId;
declare const listOptions: ScheduleListOptions;

const proveServiceOperations = (
  service: Scheduling["Service"],
  driver: ScheduleDriver["Service"],
) => {
  const created = service.create(agent, { value: "value" }, createOptions);
  const updated = service.update(
    agent,
    { value: "value" },
    {
      ...createOptions,
      expectedRevision: 1,
    },
  );
  const got = service.get(scope, scheduleId);
  const listed = service.list(scope, listOptions);
  const paused = service.pause(scope, scheduleId, 1);
  const processed = driver.process({ owner: scope.owner, scheduleId });
  const due = driver.runDue(scope.owner);

  type CreateRequirementsProof = Assert<
    Equal<Effect.Services<typeof created>, InputEncodingContext>
  >;
  type CreateFailureProof = Assert<Equal<Effect.Error<typeof created>, ScheduleManagementFailure>>;
  type UpdateRequirementsProof = Assert<
    Equal<Effect.Services<typeof updated>, InputEncodingContext>
  >;
  type UpdateFailureProof = Assert<Equal<Effect.Error<typeof updated>, ScheduleManagementFailure>>;
  type GetFailureProof = Assert<Equal<Effect.Error<typeof got>, ScheduleManagementFailure>>;
  type ListFailureProof = Assert<Equal<Effect.Error<typeof listed>, ScheduleManagementFailure>>;
  type ControlFailureProof = Assert<Equal<Effect.Error<typeof paused>, ScheduleManagementFailure>>;
  type ProcessFailureProof = Assert<Equal<Effect.Error<typeof processed>, ScheduleProcessFailure>>;
  type RunDueFailureProof = Assert<Equal<Effect.Error<typeof due>, ScheduleStorageError>>;

  const proofs: readonly [
    CreateRequirementsProof,
    CreateFailureProof,
    UpdateRequirementsProof,
    UpdateFailureProof,
    GetFailureProof,
    ListFailureProof,
    ControlFailureProof,
    ProcessFailureProof,
    RunDueFailureProof,
  ] = [true, true, true, true, true, true, true, true, true];
  return proofs;
};

const schedulingLayer = Scheduling.layer();
type SchedulingLayerSuccessProof = Assert<Equal<Layer.Success<typeof schedulingLayer>, Scheduling>>;
type SchedulingLayerFailureProof = Assert<
  Equal<Layer.Error<typeof schedulingLayer>, ScheduleValidationError>
>;
type SchedulingLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof schedulingLayer>,
    ScheduleStore | ScheduleAuthorizer | ScheduleWake | Crypto.Crypto
  >
>;

const driverLayer = ScheduleDriver.layer();
type DriverRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof driverLayer>,
    ScheduleStore | ScheduleAuthorizer | ScheduledInputAdmission | Crypto.Crypto
  >
>;
type ManagementKeysProof = Assert<
  Equal<
    keyof Scheduling["Service"],
    "create" | "update" | "get" | "list" | "pause" | "resume" | "cancel"
  >
>;

describe("Scheduling public types", () => {
  it("keeps encoding services, typed failures, and Layer requirements visible", () => {
    const serviceProof: typeof proveServiceOperations = proveServiceOperations;
    const successProof: SchedulingLayerSuccessProof = true;
    const failureProof: SchedulingLayerFailureProof = true;
    const requirementsProof: SchedulingLayerRequirementsProof = true;

    const driverProof: DriverRequirementsProof = true;
    const managementProof: ManagementKeysProof = true;
    expect([driverProof, managementProof]).toEqual([true, true]);
    expect(serviceProof).toBe(proveServiceOperations);
    expect([successProof, failureProof, requirementsProof]).toEqual([true, true, true]);
  });
});
