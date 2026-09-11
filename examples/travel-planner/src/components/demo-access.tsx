import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";

import {
  changeDemoAccessAtom,
  demoAccessAtom,
  demoEmailAtom,
  refreshDemoAccessAtom,
} from "../demo-access-state.ts";

export function DemoAccessForm() {
  const access = useAtomValue(demoAccessAtom);
  const current = Option.getOrNull(AsyncResult.value(access));
  const [email, setEmail] = useAtom(demoEmailAtom);
  const [result, change] = useAtom(changeDemoAccessAtom);
  const refresh = useAtomSet(refreshDemoAccessAtom);

  const error = Option.getOrNull(
    AsyncResult.isFailure(result) ? AsyncResult.error(result) : AsyncResult.error(access),
  );

  return (
    <details className="demo-access">
      <summary>
        Demo access <span>Admin</span>
      </summary>
      <p className="settings-help">
        Let selected people use voice and plan trips without their own OpenAI key. Their trips stay
        private. A connected personal key is used first.
      </p>
      <p className="connection-status" role="status">
        {current
          ? current.configured
            ? "Demo key ready"
            : "Demo key is not configured"
          : "Loading demo access…"}
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          change({ action: "grant" });
        }}
      >
        <label htmlFor="demo-email">Email address</label>
        <input
          id="demo-email"
          type="email"
          autoComplete="email"
          required
          maxLength={254}
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          disabled={result.waiting}
        />
        <div className="connection-actions">
          <button type="submit" disabled={!current?.configured || result.waiting || !email.trim()}>
            {result.waiting ? "Updating…" : "Add demo access"}
          </button>
          <button
            className="connection-remove"
            type="button"
            disabled={result.waiting || access.waiting}
            onClick={() => refresh()}
          >
            Refresh list
          </button>
        </div>
      </form>
      {error && (
        <p className="connection-error" role="alert">
          {error.message}
        </p>
      )}
      {AsyncResult.isSuccess(result) && !result.waiting && (
        <p className="connection-status" role="status">
          {result.value}
        </p>
      )}
      {current &&
        (current.emails.length === 0 ? (
          <p className="settings-help">No one has demo access yet.</p>
        ) : (
          <ul className="demo-access-list">
            {current.emails.map((entry) => (
              <li key={entry}>
                <span>{entry}</span>
                <button
                  type="button"
                  disabled={result.waiting}
                  aria-label={`Remove demo access for ${entry}`}
                  onClick={() => change({ action: "revoke", email: entry })}
                >
                  Remove
                </button>
              </li>
            ))}
          </ul>
        ))}
      <p className="settings-help">
        Removing an email stops new uses of the demo key. They can still use their own key and read
        their saved trips.
      </p>
    </details>
  );
}
