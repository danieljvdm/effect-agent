import { EmailProofDelivery } from "@yielded/auth/Proofs";
import { DateTime, Effect, Redacted, Schema } from "effect";

const Code = Schema.String.check(Schema.isPattern(/^[0-9]{6}$/));

export const renderCodeEmail = (code: string, minutes: number) => ({
  subject: "Your Elsewhere sign-in code",
  text: `Your Elsewhere code is ${code}. It expires in ${minutes} minutes. Enter it in the browser where you requested it. If you did not request this code, ignore this email.`,
  html: `<!doctype html><html lang="en"><body style="margin:0;background:#faf8f3;color:#242922;font-family:Arial,sans-serif"><table role="presentation" width="100%"><tr><td align="center" style="padding:48px 16px"><table role="presentation" style="max-width:440px;background:white;border:1px solid #e3e5da;border-radius:16px"><tr><td style="padding:36px"><p style="color:#64764c">ELSEWHERE</p><h1 style="font-size:24px">Your next trip starts here.</h1><p>Enter this code in the browser where you requested it. It expires in ${minutes} minutes.</p><p style="font-family:monospace;font-size:36px;letter-spacing:8px">${code}</p><p style="font-size:13px;color:#677060">If you did not request this code, you can ignore this email.</p></td></tr></table></td></tr></table></body></html>`,
});

/** Await provider acceptance. An uncertain send is never automatically repeated. */
export const emailDeliveryLayer = (binding: SendEmail, from: string) =>
  EmailProofDelivery.layer(
    { vendorId: "cloudflare-email-sending", idempotencyMillis: 0 },
    Effect.fn("Auth.deliverEmail")(function* (message) {
      const code = yield* Schema.decodeUnknownEffect(Code)(Redacted.value(message.secret));
      const now = DateTime.toEpochMillis(yield* DateTime.now);
      const minutes = Math.max(1, Math.ceil((message.expiresAtMillis - now) / 60_000));

      const result = yield* Effect.tryPromise({
        try: () =>
          binding.send({
            from: { name: "Elsewhere", email: from },
            to: message.recipient.value,
            ...renderCodeEmail(code, minutes),
          }),
        catch: () => "SendUnavailable" as const,
      }).pipe(Effect.timeout("10 seconds"), Effect.result);

      return result._tag === "Success"
        ? { _tag: "Accepted" as const }
        : { _tag: "Ambiguous" as const };
    }),
  );
