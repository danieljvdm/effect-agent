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
  const personalKey = current?.connected && !current.serverFunded;

  const error = Option.getOrNull(
    AsyncResult.error(AsyncResult.isFailure(result) ? result : connection),
  );

  const message = error && "message" in error ? String(error.message) : null;

  return (
    <section className="openai-connection" aria-label="OpenAI connection">
      <h3>Your OpenAI key</h3>
      <p className="settings-help">
        {current?.serverFunded
          ? "A personal key is optional. Add one to use your own OpenAI account instead."
          : "Your key pays for the planner, research scouts, and app editor. Hosting is included."}
      </p>
      <p className="connection-status" role="status">
        {current?.serverFunded
          ? "Server-funded access · no personal key required"
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
        <label htmlFor="openai-key">{personalKey ? "Replace API key" : "API key"}</label>
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
            {result.waiting ? "Updating…" : personalKey ? "Replace key" : "Connect key"}
          </button>
          {personalKey && (
            <button
              type="button"
              className="connection-remove"
              disabled={result.waiting}
              onClick={() => change({ action: "disconnect" })}
            >
              Remove key
            </button>
          )}
        </div>
      </form>
      {message && (
        <p className="connection-error" role="alert">
          {message}{" "}
          <button type="button" disabled={result.waiting} onClick={() => refresh()}>
            Refresh connection
          </button>
        </p>
      )}
      <p className="settings-help">
        Stored encrypted on the server for background work. Removing a personal key uses
        server-funded access if you are allowlisted; otherwise new model requests stop. Your trips
        remain saved.
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
