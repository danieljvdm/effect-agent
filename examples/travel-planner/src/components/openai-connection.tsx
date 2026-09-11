import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option, Redacted } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import { useState } from "react";

import {
  changeOpenAiConnectionAtom,
  openAiConnectionAtom,
  refreshOpenAiConnectionAtom,
} from "../state";

export function OpenAiConnectionForm() {
  const connection = useAtomValue(openAiConnectionAtom);
  const [result, change] = useAtom(changeOpenAiConnectionAtom);
  const refresh = useAtomSet(refreshOpenAiConnectionAtom);
  const [draft, setDraft] = useState("");
  const current = Option.getOrNull(AsyncResult.value(connection));
  const demo = current?.source === "demo";
  const personal = current?.connected && !demo;

  const error = Option.getOrNull(
    AsyncResult.error(AsyncResult.isFailure(result) ? result : connection),
  );

  const message = error && "message" in error ? String(error.message) : null;

  return (
    <section className="openai-connection" aria-label="OpenAI connection">
      <h3>Your OpenAI key</h3>
      <p className="settings-help">
        {demo
          ? "Demo access is included for your account. You can optionally connect your own key."
          : "Your key pays for voice, planning, research scouts, and the app editor. Hosting is included."}
      </p>
      <p className="connection-status" role="status">
        {demo
          ? "Demo access · included"
          : current?.connected
            ? `Connected · ending in ${current.lastFour}`
            : connection.waiting
              ? "Checking your connection…"
              : "Connect a key to start planning."}
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          change({ action: "connect", apiKey: Redacted.make(draft) });
          setDraft("");
        }}
      >
        <label htmlFor="openai-key">
          {personal ? "Replace API key" : demo ? "Your own API key (optional)" : "API key"}
        </label>
        <input
          id="openai-key"
          type="password"
          autoComplete="off"
          autoCapitalize="none"
          spellCheck={false}
          placeholder="sk-…"
          maxLength={512}
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          disabled={result.waiting}
        />
        <div className="connection-actions">
          <button type="submit" disabled={result.waiting || draft.trim().length < 16}>
            {result.waiting ? "Updating…" : personal ? "Replace key" : "Connect key"}
          </button>
          {personal && (
            <button
              type="button"
              className="connection-remove"
              disabled={result.waiting}
              onClick={() => change({ action: "disconnect" })}
            >
              Remove key
            </button>
          )}
          <button
            type="button"
            className="connection-remove"
            disabled={result.waiting || connection.waiting}
            onClick={() => refresh()}
          >
            Refresh access
          </button>
        </div>
      </form>
      {message && (
        <p className="connection-error" role="alert">
          {message}
        </p>
      )}
      <p className="settings-help">
        Personal keys are stored encrypted. Removing yours uses demo access if it has been granted;
        otherwise, connect another key to keep planning. Your trips remain saved.
      </p>
      <a
        className="settings-help"
        href="https://platform.openai.com/api-keys"
        target="_blank"
        rel="noreferrer"
      >
        Get an OpenAI API key ↗
      </a>
    </section>
  );
}
