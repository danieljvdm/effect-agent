import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import { useState } from "react";

import {
  changeFundingAtom,
  fundingDirectoryAtom,
  fundingPageAtom,
  fundingStatusAtom,
  refreshFundingAtom,
} from "../funding-state";

export function FundingPanel() {
  const status = useAtomValue(fundingStatusAtom);
  const directory = useAtomValue(fundingDirectoryAtom);
  const [result, change] = useAtom(changeFundingAtom);
  const [page, setPage] = useAtom(fundingPageAtom);
  const refresh = useAtomSet(refreshFundingAtom);
  const [kind, setKind] = useState<"email" | "github">("email");
  const [value, setValue] = useState("");

  if (!AsyncResult.isSuccess(status) || !status.value.admin) return null;
  const data = Option.getOrNull(AsyncResult.value(directory));

  const error =
    Option.getOrNull(AsyncResult.error(result)) ?? Option.getOrNull(AsyncResult.error(directory));

  return (
    <section className="openai-connection funding-panel" aria-label="Server-funded access">
      <h3>Server-funded access</h3>
      <p className="settings-help">
        Allow people to plan with the server’s OpenAI key. Your GitHub account has administrator
        access.
      </p>
      <p role="status" className="connection-status">
        {status.value.configured
          ? "Server key configured"
          : "Server key is not configured yet. Grants will apply once it is connected."}
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          change({ action: "grant", input: { kind, value } });
        }}
      >
        <label htmlFor="funding-kind">Allow by</label>
        <select
          id="funding-kind"
          value={kind}
          onChange={(event) => setKind(event.target.value === "github" ? "github" : "email")}
        >
          <option value="email">Verified email address</option>
          <option value="github">GitHub username</option>
        </select>
        <label htmlFor="funding-value">
          {kind === "email" ? "Email address" : "GitHub username"}
        </label>
        <input
          id="funding-value"
          type={kind === "email" ? "email" : "text"}
          value={value}
          onChange={(event) => setValue(event.target.value)}
          maxLength={320}
          placeholder={kind === "email" ? "name@gmail.com" : "octocat"}
          autoCapitalize="none"
          spellCheck={false}
          required
        />
        <div className="connection-actions">
          <button type="submit" disabled={result.waiting || !value.trim()}>
            {result.waiting ? "Updating…" : "Allow access"}
          </button>
        </div>
      </form>
      <p className="settings-help">
        Email grants apply after email-code verification. GitHub grants follow the verified GitHub
        account, even if its username changes.
      </p>
      {AsyncResult.isSuccess(result) && !result.waiting && <p role="status">Access updated.</p>}
      {error && (
        <p role="alert" className="connection-error">
          {"message" in error ? String(error.message) : "Could not load funding access."}
        </p>
      )}
      <div className="funding-heading">
        <h4>Allowlist</h4>
        <button type="button" onClick={() => refresh()} disabled={result.waiting}>
          Refresh
        </button>
      </div>
      {data?.grants.length === 0 && (
        <p className="settings-help">No additional people are allowlisted yet.</p>
      )}
      <ul className="funding-list">
        {data?.grants.map((grant) => (
          <li key={`${grant.kind}:${grant.target}`}>
            <span>
              <strong>{grant.kind === "github" ? `@${grant.label}` : grant.label}</strong>
              <small>
                {grant.kind === "account"
                  ? "Registered account"
                  : grant.kind === "github"
                    ? "GitHub account"
                    : "Verified email"}
              </small>
            </span>
            <button
              type="button"
              disabled={result.waiting}
              onClick={() =>
                change({ action: "revoke", input: { kind: grant.kind, target: grant.target } })
              }
            >
              Revoke
            </button>
          </li>
        ))}
      </ul>
      <h4>Registered users</h4>
      {directory.waiting && <p role="status">Loading users…</p>}
      <ul className="funding-list">
        {data?.users.map((user) => (
          <li key={user.subjectId}>
            <span>
              <strong>{user.displayName}</strong>
              <small>
                {user.emails.join(", ") ||
                  (user.githubIds.length
                    ? `GitHub account ${user.githubIds.join(", ")}`
                    : "Registered account")}
              </small>
              <small>
                {user.admin
                  ? "Administrator · server-funded"
                  : user.allowed
                    ? "Server-funded access"
                    : "Uses their own key"}
              </small>
            </span>
            {!user.allowed && (
              <button
                type="button"
                disabled={result.waiting}
                onClick={() =>
                  change({ action: "grant", input: { kind: "account", value: user.subjectId } })
                }
              >
                Allow
              </button>
            )}
          </li>
        ))}
      </ul>
      <div className="connection-actions">
        {page && (
          <button type="button" onClick={() => setPage(undefined)}>
            First page
          </button>
        )}
        {data?.next && (
          <button type="button" onClick={() => setPage(data.next ?? undefined)}>
            Next users
          </button>
        )}
      </div>
      <p className="settings-help">
        Revoking a grant stops new server-funded requests unless another grant still applies.
        Personal keys take priority.
      </p>
    </section>
  );
}
