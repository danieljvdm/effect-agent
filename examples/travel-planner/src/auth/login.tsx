import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Navigate } from "@tanstack/react-router";
import { Atom, AsyncResult } from "effect/unstable/reactivity";
import { useEffect, useState } from "react";

import {
  auth,
  completeGithub,
  consumeCallback,
  githubLogin,
  requestEmailCode,
  verifyEmailCode,
} from "./client";

export function Login({ callback = false }: { readonly callback?: boolean }) {
  const session = useAtomValue(auth.session);
  const [github, startGithub] = useAtom(githubLogin);
  const completed = useAtomValue(completeGithub);
  const consume = useAtomSet(consumeCallback);
  const [requested, requestCode] = useAtom(requestEmailCode);
  const [verified, verifyCode] = useAtom(verifyEmailCode);
  const [mode, setMode] = useState<"register" | "signin">("signin");
  const [email, setEmail] = useState("");
  const [code, setCode] = useState("");

  useEffect(() => {
    if (callback) consume();
  }, [callback, consume]);
  if (AsyncResult.isSuccess(session) && session.value !== null) return <Navigate to="/" replace />;

  const pending =
    AsyncResult.isSuccess(verified) && verified.value
      ? verified.value
      : AsyncResult.isSuccess(requested)
        ? requested.value
        : undefined;

  const busy =
    requested.waiting || verified.waiting || github.waiting || (callback && completed.waiting);

  const failed = [requested, verified, github, ...(callback ? [completed] : [])].some(
    (result) => result._tag === "Failure",
  );

  const cancelled =
    AsyncResult.isSuccess(completed) &&
    "_tag" in completed.value &&
    completed.value._tag === "Cancelled";

  return (
    <main className="login-page">
      <section className="login-card">
        <a className="wordmark" href="/login">
          elsewhere<span>↗</span>
        </a>
        <h1>Your next trip starts here.</h1>
        <p>Plan somewhere wonderful. Bring your own OpenAI key to start a conversation.</p>
        <button className="login-github" disabled={busy} onClick={() => startGithub()}>
          Continue with GitHub
        </button>
        <div className="login-divider">or use your email</div>
        {!pending ? (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              requestCode({ mode, email });
            }}
          >
            <div className="login-modes" aria-label="Email account action">
              <button
                type="button"
                aria-pressed={mode === "signin"}
                onClick={() => setMode("signin")}
              >
                Sign in
              </button>
              <button
                type="button"
                aria-pressed={mode === "register"}
                onClick={() => setMode("register")}
              >
                Create account
              </button>
            </div>
            <label htmlFor="login-email">Email address</label>
            <input
              id="login-email"
              type="email"
              autoComplete="email"
              required
              maxLength={254}
              value={email}
              onChange={(event) => setEmail(event.target.value)}
              disabled={busy}
            />
            <button className="primary" disabled={busy}>
              {busy
                ? "Sending…"
                : mode === "register"
                  ? "Create account with email"
                  : "Send sign-in code"}
            </button>
          </form>
        ) : (
          <form
            onSubmit={(event) => {
              event.preventDefault();
              verifyCode({ pending, code });
              setCode("");
            }}
          >
            <p role="status">
              {AsyncResult.isSuccess(verified) && verified.value
                ? "Your account is ready. We sent a new code to finish signing in."
                : pending.mode === "register"
                  ? "Enter the code to create your account. Then we’ll send a fresh sign-in code."
                  : "If this email has an account, a sign-in code is on its way."}
            </p>
            <p className="login-recipient">{pending.email}</p>
            <label htmlFor="login-code">Six-digit code</label>
            <input
              id="login-code"
              inputMode="numeric"
              autoComplete="one-time-code"
              pattern="[0-9]{6}"
              minLength={6}
              maxLength={6}
              required
              value={code}
              onChange={(event) => setCode(event.target.value)}
              disabled={busy}
            />
            <button className="primary" disabled={busy}>
              {busy
                ? "Checking…"
                : pending.mode === "register"
                  ? "Verify email and create account"
                  : "Sign in"}
            </button>
            <button
              className="quiet"
              type="button"
              disabled={busy}
              onClick={() => {
                requestCode(Atom.Reset);
                verifyCode(Atom.Reset);
                setCode("");
              }}
            >
              Start again or request another code
            </button>
            <p className="login-note">
              Codes expire after five minutes. Wait at least 30 seconds before requesting another.
            </p>
          </form>
        )}
        {failed && (
          <p className="error" role="alert">
            We couldn’t complete that step. Check your code or start a new sign-in attempt. If you
            just created an account, choose Sign in.
          </p>
        )}
        {cancelled && (
          <p role="status">GitHub sign-in was cancelled. You can start again when you’re ready.</p>
        )}
        <p className="login-note">
          Email and GitHub create separate accounts. Use the same method when you return.
        </p>
      </section>
    </main>
  );
}
