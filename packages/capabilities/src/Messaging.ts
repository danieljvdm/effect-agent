import type { AnyDefinition } from "@effect-agent/core/Agent";
import {
  InboxPage,
  MessageRef,
  MessageStatus,
  MessagingError,
  PeerName,
} from "@effect-agent/core/Messaging";
import { IdempotencyKey } from "@effect-agent/core/Receipt";
import { WorkerOperationTool } from "@effect-agent/core/SubagentContract";
import { MessagingHost } from "@effect-agent/engine/MessagingHost";
import { Crypto, Effect, Encoding, Option, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

export {
  InboxPage,
  MessageAdmission,
  MessageRef,
  MessageStatus,
  MessagingError,
} from "@effect-agent/core/Messaging";

/** A fixed host route and its destination-owned input Schema. Construction acquires no resources. */
export interface Peer<Name extends string, Input extends Schema.Top> {
  readonly name: Name;
  readonly target: AnyDefinition & { readonly input: Input };
}

export const peer = <const Name extends string, Input extends Schema.Top>(
  name: Name,
  options: { readonly target: AnyDefinition & { readonly input: Input } },
): Peer<Name, Input> => {
  Schema.decodeSync(PeerName)(name);

  return Object.freeze({ name, target: options.target });
};

const host: Effect.Effect<MessagingHost["Service"], MessagingError, MessagingHost> =
  Effect.serviceOption(MessagingHost).pipe(
    Effect.flatMap((value) =>
      Option.isSome(value)
        ? Effect.succeed(value.value)
        : MessagingError.make({ operation: "context", reason: "unavailable" }),
    ),
  );

const dispatch = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey; readonly inReplyTo?: MessageRef },
  operation: "send" | "reply",
) =>
  Effect.gen(function* () {
    const service = yield* host;
    const invalid = () => MessagingError.make({ operation, reason: "invalid-input" });

    const idempotencyKey = yield* Schema.decodeUnknownEffect(IdempotencyKey)(
      options.idempotencyKey,
    ).pipe(Effect.mapError(invalid));

    const encodedInput = yield* Schema.encodeEffect(declaration.target.input)(input).pipe(
      Effect.mapError(invalid),
    );

    const inReplyTo =
      options.inReplyTo === undefined
        ? undefined
        : yield* Schema.decodeUnknownEffect(MessageRef)(options.inReplyTo).pipe(
            Effect.mapError(invalid),
          );

    const request = {
      ...declaration,
      encodedInput,
      idempotencyKey,
      ...(inReplyTo === undefined ? {} : { inReplyTo }),
    };

    const result =
      operation === "reply"
        ? inReplyTo === undefined
          ? yield* invalid()
          : yield* service.reply({ ...request, inReplyTo })
        : yield* service.send(request);

    return yield* Schema.decodeUnknownEffect(MessageStatus)(result).pipe(
      Effect.mapError(() => MessagingError.make({ operation, reason: "corrupt" })),
    );
  });

/** Retain a durable message. Its status distinguishes outbound retention from destination acceptance. */
export const send = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey; readonly inReplyTo?: MessageRef },
) => dispatch(declaration, input, options, "send");

/** Reply to an authenticated inbound message; return routing still requires independent send authority. */
export const reply = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  inReplyTo: MessageRef,
  input: Input["Type"],
  options: { readonly idempotencyKey: IdempotencyKey },
) => dispatch(declaration, input, { ...options, inReplyTo }, "reply");

/** Return bounded provenance for messages from this peer, without granting permission to reply. */
export const inbox = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  options: { readonly after?: number; readonly limit?: number } = {},
) =>
  Effect.gen(function* () {
    const service = yield* host;

    return yield* service.inbox({ ...declaration, ...options, limit: options.limit ?? 20 });
  });

export const inspect = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  message: MessageRef,
) =>
  Effect.gen(function* () {
    const service = yield* host;

    return yield* service.inspect({ ...declaration, message });
  });

/** Explicitly renew a parked delivery's bounded retry budget, retaining its complete frozen envelope. */
export const retry = <Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
  message: MessageRef,
) =>
  Effect.gen(function* () {
    const service = yield* host;

    return yield* service.retry({ ...declaration, message });
  });

const modelKey = Effect.fn("Messaging.modelKey")(function* () {
  const source = yield* (yield* host).context;

  if (source._tag !== "tool")
    return yield* MessagingError.make({ operation: "send", reason: "denied" });
  const crypto = yield* Crypto.Crypto;

  const digest = yield* crypto
    .digest("SHA-256", new TextEncoder().encode(JSON.stringify(source)))
    .pipe(Effect.mapError(() => MessagingError.make({ operation: "send", reason: "unavailable" })));

  return Schema.decodeSync(IdempotencyKey)(`peer-tool:${Encoding.encodeHex(digest)}`);
});

const native = <
  const Name extends string,
  Parameters extends Schema.Top,
  Success extends Schema.Top,
  R,
>(
  name: Name,
  parameters: Parameters,
  success: Success,
  description: string,
  handler: (
    parameters: Parameters["Type"],
  ) => Effect.Effect<Success["Type"], MessagingError, R | MessagingHost | Crypto.Crypto>,
) => {
  const tool = Tool.make(name, { parameters, success, failure: MessagingError, description })
    .annotate(WorkerOperationTool, true)
    .addDependency(MessagingHost)
    .addDependency(Crypto.Crypto);

  const toolkit = Toolkit.make(tool);

  const build = Effect.gen(function* () {
    const captured = yield* Effect.context<Exclude<R, MessagingHost | Crypto.Crypto>>();

    const invoke = Effect.fn("Messaging.tool")(function* (value: Parameters["Type"]) {
      const service = yield* host;
      const crypto = yield* Crypto.Crypto;

      return yield* handler(value).pipe(
        Effect.provideService(MessagingHost, service),
        Effect.provideService(Crypto.Crypto, crypto),
        Effect.provide(captured),
      );
    });

    // The computed single key is precisely the native Tool's literal name.
    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<typeof toolkit.tools>;
  });

  return { tool, toolkit, layer: toolkit.toLayer(build) };
};

/** Install only the native operations chosen by the host; schemas come from the Peer declaration. */
export const sendTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_send` as const,
    declaration.target.input,
    MessageStatus,
    `Send input to ${declaration.name} through its authorized durable route.`,
    (input) =>
      Effect.gen(function* () {
        return yield* send(declaration, input, { idempotencyKey: yield* modelKey() });
      }),
  );

export const replyTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) => {
  const inputSchema: Schema.Codec<
    Input["Type"],
    Input["Encoded"],
    Input["DecodingServices"],
    Input["EncodingServices"]
  > = declaration.target.input;

  return native(
    `${declaration.name}_reply` as const,
    Schema.Struct({ inReplyTo: MessageRef, input: inputSchema }),
    MessageStatus,
    `Reply to a recorded inbound message from ${declaration.name}.`,
    ({ inReplyTo, input }) =>
      Effect.gen(function* () {
        return yield* reply(declaration, inReplyTo, input, { idempotencyKey: yield* modelKey() });
      }),
  );
};

export const inboxTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_inbox` as const,
    Schema.Struct({
      after: Schema.optionalKey(Schema.Natural),
      limit: Schema.optionalKey(
        Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
      ),
    }),
    InboxPage,
    `Read a bounded page of message provenance from ${declaration.name}.`,
    (options) => inbox(declaration, options),
  );

export const inspectTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_message_status` as const,
    Schema.Struct({ message: MessageRef }),
    MessageStatus,
    `Inspect delivery status of a message sent to ${declaration.name}.`,
    ({ message }) => inspect(declaration, message),
  );

export const retryTool = <const Name extends string, Input extends Schema.Top>(
  declaration: Peer<Name, Input>,
) =>
  native(
    `${declaration.name}_retry_message` as const,
    Schema.Struct({ message: MessageRef }),
    MessageStatus,
    `Renew bounded automatic retries for a parked message to ${declaration.name}.`,
    ({ message }) => retry(declaration, message),
  );
