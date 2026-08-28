import { ScheduleFailpointError, ScheduleId } from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { Effect, Layer, Schema } from "effect";

import {
  CloudflareSchedulingClient,
  ScheduleOwnerNamespace,
  makeConversationObjectClass,
  makeScheduleOwnerObjectClass,
  type ConversationObjectOptions,
  type ScheduleOwnerObjectRpc,
} from "../../src/index.ts";
import {
  DEPLOYMENT_ID,
  PRODUCER_PREFIX,
  TEST_DIGESTS,
  TEST_PRINCIPAL,
  decodeConversationId,
  fixtureReconcilerLayer,
  makeTestBindings,
  plannerDefinition,
} from "../fixtures.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      RESTART_CONVERSATIONS: DurableObjectNamespace<SchedulingRestartConversation>;
      RESTART_SCHEDULES: DurableObjectNamespace<SchedulingRestartOwner>;
    }
  }
}

const conversationOptions: ConversationObjectOptions = {
  namespaceBinding: "RESTART_CONVERSATIONS",
  deploymentId: DEPLOYMENT_ID,
  producerPrefix: PRODUCER_PREFIX,
  ownershipLeaseDuration: 250,
  leaseRenewalInterval: 50,
  wakeScanInterval: 100,
  settlementPollInterval: 25,
  abortPollInterval: 25,
  alarmBackoffBase: 10,
  alarmBackoffCap: 100,
  observationPollInterval: 10,
  bindings: () => makeTestBindings,
  toolReconciler: fixtureReconcilerLayer,
};

const SubmissionIdRow = Schema.Struct({ submission_id: Schema.String });

export class SchedulingRestartConversation extends makeConversationObjectClass(
  conversationOptions,
) {
  async submissionIds(): Promise<ReadonlyArray<string>> {
    const rows = this.ctx.storage.sql
      .exec("SELECT submission_id FROM effect_agent_submissions ORDER BY queue_sequence")
      .toArray();
    return Schema.decodeUnknownSync(Schema.Array(SubmissionIdRow))(rows).map(
      (row) => row.submission_id,
    );
  }
}

let alarmDeliveries = 0;
let completedAlarmDeliveries = 0;
let loseAdmissionReplies = false;
let admissionReplyFailures = 0;

const ScheduleOwnerBase = makeScheduleOwnerObjectClass({
  conversationNamespaceBinding: "RESTART_CONVERSATIONS",
  authorizer: {
    manage: () => Effect.void,
    prepare: () => Effect.succeed({ policyId: "restart-policy", decisionId: "restart-allow" }),
  },
  failpoint: () => ({
    hit: (point) => {
      if (point !== "schedule:admission:after" || !loseAdmissionReplies) return Effect.void;
      admissionReplyFailures += 1;
      return Effect.fail(ScheduleFailpointError.make({ point }));
    },
  }),
  limits: {
    maxSchedulesPerOwner: 10,
    minIntervalMillis: 60_000,
    maxInputBytes: 65_536,
    dueBatchSize: 4,
    admissionConcurrency: 2,
    retryBaseMillis: 10,
    retryMaxMillis: 100,
    admissionTimeoutMillis: 5_000,
    recoveryPollMillis: 100,
  },
});

export class SchedulingRestartOwner extends ScheduleOwnerBase {
  override async alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> {
    alarmDeliveries += 1;
    await super.alarm(alarmInfo);
    completedAlarmDeliveries += 1;
  }
}

const owner = { tenantId: "restart-tenant", ownerId: "restart-owner" } as const;
const scope = { owner, principal: TEST_PRINCIPAL };
const scheduleId = Schema.decodeSync(ScheduleId)("restart-schedule");
const conversation = "restart-scheduled-conversation";
const CreateRequest = Schema.Struct({ deadlineAtMillis: Schema.Number });

const schedulingLayer = (env: Cloudflare.Env) =>
  CloudflareSchedulingClient.layer.pipe(
    Layer.provide(
      ScheduleOwnerNamespace.layer(
        env.RESTART_SCHEDULES as unknown as DurableObjectNamespace<ScheduleOwnerObjectRpc>,
      ),
    ),
    Layer.provide(BrowserCrypto.layer),
  );

const runScheduling = <A, E>(
  env: Cloudflare.Env,
  effect: Effect.Effect<A, E, CloudflareSchedulingClient>,
): Promise<A> => Effect.runPromise(effect.pipe(Effect.provide(schedulingLayer(env))));

const handle = (request: Request, env: Cloudflare.Env) =>
  Effect.gen(function* () {
    const url = new URL(request.url);
    if (url.pathname === "/introspect") {
      return Response.json({
        alarmDeliveries,
        completedAlarmDeliveries,
        admissionReplyFailures,
      });
    }
    if (url.pathname === "/arm-lost-reply") {
      loseAdmissionReplies = true;
      return Response.json({ armed: true });
    }
    if (url.pathname === "/create") {
      const body = yield* Effect.tryPromise(() => request.json());
      const { deadlineAtMillis } = yield* Schema.decodeUnknownEffect(CreateRequest)(body);
      const snapshot = yield* Effect.promise(() =>
        runScheduling(
          env,
          Effect.gen(function* () {
            const client = yield* CloudflareSchedulingClient;
            return yield* client.create(
              { definition: plannerDefinition },
              { question: "persist across Miniflare restart", ref: conversation },
              {
                scope,
                scheduleId,
                timing: { _tag: "At", atMillis: deadlineAtMillis },
                destination: {
                  _tag: "ExistingConversation",
                  conversationId: decodeConversationId(conversation),
                },
                deliveryPrincipal: TEST_PRINCIPAL,
                definitions: TEST_DIGESTS,
              },
            );
          }),
        ),
      );
      return Response.json({ revision: snapshot.configurationRevision });
    }
    if (url.pathname === "/status") {
      const snapshot = yield* Effect.promise(() =>
        runScheduling(
          env,
          Effect.gen(function* () {
            const client = yield* CloudflareSchedulingClient;
            return yield* client.get(scope, scheduleId);
          }),
        ),
      );
      const submissionIds = yield* Effect.promise(() =>
        env.RESTART_CONVERSATIONS.getByName(conversation).submissionIds(),
      );
      return Response.json({
        delivered: snapshot.lastReceipt !== null,
        pending: snapshot.pending !== null,
        receiptSubmissionId: snapshot.lastReceipt?.receipt.submissionId ?? null,
        submissionIds,
      });
    }
    return new Response("not found", { status: 404 });
  });

export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(handle(request, env));
  },
};
