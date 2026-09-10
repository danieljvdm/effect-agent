import { Dialog } from "@base-ui/react/dialog";
import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Option } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import { ArrowUp, ArrowUpRight, Clock3, Map, Menu, Plus, X } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { ActivityPanel } from "./components/activity-panel";
import { AgentProgress } from "./components/agent-progress.tsx";
import { MessageText } from "./components/message-text";
import { ResearchScoutCard } from "./components/research-scout-card.tsx";
import { TravelCards } from "./components/travel/travel-cards";
import { TripAppCard } from "./components/trip-app-card.tsx";
import { useMobileViewport } from "./components/use-mobile-viewport";
import type { Trip } from "./domain";
import {
  sessionAtom,
  membersAtom,
  memberEmailAtom,
  manageMemberAtom,
  refreshMembersAtom,
  activeTripAtom,
  draftAtom,
  plannerAtom,
  changeTripAppAtom,
  newTripAtom,
  selectTripAtom,
  selectionAtom,
  sendMessageAtom,
  progressAtom,
  settingsAtom,
  settingsStatusAtom,
  changeSettingsAtom,
  conversationStatusAtom,
  sidebarTripsAtom,
} from "./state";

function failure(result: AsyncResult.AsyncResult<unknown, unknown>): string | null {
  if (!AsyncResult.isFailure(result)) return null;
  const error = Option.getOrNull(AsyncResult.error(result));

  return error &&
    typeof error === "object" &&
    "message" in error &&
    typeof error.message === "string"
    ? error.message
    : "Couldn't connect. Reload the page to sign in again.";
}

export function Planner() {
  const sessionResult = useAtomValue(sessionAtom);
  const session = AsyncResult.isSuccess(sessionResult) ? sessionResult.value : null;
  const [manageAccess, setManageAccess] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);

  const selectTrip = useAtomSet(selectTripAtom);
  const newTrip = useAtomSet(newTripAtom);
  const selection = useAtomValue(selectionAtom);
  const [draft, setDraft] = useAtom(draftAtom);
  const result = useAtomValue(plannerAtom);
  const snapshot = Option.getOrNull(AsyncResult.value(result));
  const conversationStatus = useAtomValue(conversationStatusAtom);
  const savedTrips = useAtomValue(sidebarTripsAtom);
  const [sendResult, send] = useAtom(sendMessageAtom);
  const [publishResult, changeApp] = useAtom(changeTripAppAtom);
  const [inspect, setInspect] = useState(false);
  const [showTrip, setShowTrip] = useState(false);
  const trip = useAtomValue(activeTripAtom);
  const progressResult = useAtomValue(progressAtom);
  const progress = Option.getOrNull(AsyncResult.value(progressResult));

  const messages = snapshot?.messages.filter((message) => message.id !== "welcome") ?? [];

  const busy = sendResult.waiting || (snapshot?.pending ?? 0) > 0;
  const canSend = session !== null && !sendResult.waiting && draft.trim().length > 0;

  const live =
    progress?.submissionId && snapshot?.pendingSubmissionIds.includes(progress.submissionId)
      ? progress
      : null;

  const visibleProgress = live ?? (!busy ? progress : null);
  const error = failure(result) ?? failure(sendResult) ?? failure(publishResult);
  const transcript = useRef<HTMLDivElement>(null);
  const composerInput = useRef<HTMLTextAreaElement>(null);
  const lastMessageId = messages.at(-1)?.id;
  const followResponse = useRef(true);

  useMobileViewport(transcript);

  useEffect(() => {
    const input = composerInput.current;

    if (!input) return;
    input.style.height = "0px";
    input.style.height = `${Math.min(input.scrollHeight, 120)}px`;
  }, [draft]);

  useEffect(() => {
    followResponse.current = true;
  }, [selection.conversationId]);

  useEffect(() => {
    if (followResponse.current)
      transcript.current?.scrollTo({ top: transcript.current.scrollHeight });
  }, [selection.conversationId, lastMessageId, live?.revision, snapshot?.app?.revision]);

  useEffect(() => {
    const desktop = window.matchMedia("(min-width: 701px)");

    const closeOnDesktop = () => {
      if (desktop.matches) setMenuOpen(false);
    };

    desktop.addEventListener("change", closeOnDesktop);

    return () => desktop.removeEventListener("change", closeOnDesktop);
  }, []);

  const navigation = (
    <>
      <a className="wordmark" href="/">
        elsewhere
        <ArrowUpRight size={34} aria-hidden="true" />
      </a>
      <button
        className="new-trip"
        onClick={() => {
          newTrip();
          setMenuOpen(false);
        }}
      >
        <Plus size={21} aria-hidden="true" /> Plan a new trip
      </button>
      <div className="sidebar-heading">
        YOUR TRIPS <span>{savedTrips.length}</span>
      </div>
      <nav aria-label="Saved trips" className="trip-list">
        {savedTrips.map((saved) => (
          <button
            className={`trip-link ${selection.conversationId === saved.conversationId ? "selected" : ""}`}
            key={saved.conversationId}
            onClick={() => {
              selectTrip(saved);
              setMenuOpen(false);
            }}
          >
            <span className="trip-icon">
              <ArrowUpRight size={20} aria-hidden="true" />
            </span>
            <span>
              <strong>{saved.title}</strong>
              <small>{saved.destination}</small>
            </span>
          </button>
        ))}
        {!savedTrips.length && conversationStatus === "ready" && (
          <p className="sidebar-empty">
            The places you're dreaming of,
            <br />
            all in one place.
          </p>
        )}
      </nav>
      <div className="mobile-menu-actions">
        {trip && (
          <button
            onClick={() => {
              setShowTrip(!showTrip);
              setMenuOpen(false);
            }}
            aria-pressed={showTrip}
          >
            <Map size={19} aria-hidden="true" />
            {showTrip ? "Hide itinerary" : "View itinerary"}
          </button>
        )}
        <button
          onClick={() => {
            setInspect(!inspect);
            setManageAccess(false);
            setMenuOpen(false);
          }}
          aria-pressed={inspect}
        >
          <Clock3 size={19} aria-hidden="true" />
          Agent activity
        </button>
      </div>
      <div className="sidebar-bottom">
        {session ? (
          <div className="account">
            <span className="account-email">{session.email}</span>
            <div className="account-actions">
              {session.isAdmin && (
                <button
                  className="quiet"
                  onClick={() => {
                    setManageAccess(!manageAccess);
                    setMenuOpen(false);
                  }}
                >
                  Manage access
                </button>
              )}
              <a href="/cdn-cgi/access/logout">Sign out</a>
            </div>
          </div>
        ) : (
          <p className="session-status" role="status">
            {failure(sessionResult) ?? "Checking your session…"}
            {AsyncResult.isFailure(sessionResult) && <a href="/">Reload / sign in</a>}
          </p>
        )}
        <p className="powered">A travel companion built with Effect Agent</p>
      </div>
    </>
  );

  return (
    <div className="app-shell">
      <aside className="sidebar desktop-sidebar">{navigation}</aside>
      <main className="workspace">
        <header className="topbar">
          <Dialog.Root open={menuOpen} onOpenChange={setMenuOpen}>
            <Dialog.Trigger
              className="icon-button mobile-menu-trigger"
              aria-label="Open navigation"
            >
              <Menu size={22} aria-hidden="true" />
            </Dialog.Trigger>
            <Dialog.Portal>
              <Dialog.Backdrop className="navigation-backdrop" />
              <Dialog.Popup className="sidebar mobile-sidebar">
                <Dialog.Title className="sr-only">Your trips and account</Dialog.Title>
                <Dialog.Close
                  className="icon-button navigation-close"
                  aria-label="Close navigation"
                >
                  <X size={22} aria-hidden="true" />
                </Dialog.Close>
                {navigation}
              </Dialog.Popup>
            </Dialog.Portal>
          </Dialog.Root>
          <span className="topbar-title">
            {trip?.title ??
              savedTrips.find((saved) => saved.conversationId === selection.conversationId)
                ?.title ??
              "elsewhere"}
          </span>
          <div className="topbar-actions">
            {trip && (
              <button
                className={`desktop-toolbar-action inspect ${showTrip ? "active" : ""}`}
                aria-expanded={showTrip}
                onClick={() => setShowTrip(!showTrip)}
              >
                Itinerary
              </button>
            )}
            <button
              className={`desktop-toolbar-action inspect ${inspect ? "active" : ""}`}
              onClick={() => setInspect(!inspect)}
            >
              <Clock3 size={14} aria-hidden="true" /> Activity
            </button>
            <ModelControls />
          </div>
        </header>
        <div className={`content ${trip && showTrip ? "with-trip" : ""}`}>
          <section className="conversation" aria-label="Travel conversation">
            {conversationStatus !== "ready" ? (
              <div className="conversation-loading" role="status" aria-live="polite">
                {conversationStatus === "loading" ? (
                  <>
                    <span className="loading-line" />
                    <span className="loading-line" />
                    <span className="loading-line" />
                    <p>Loading your conversation…</p>
                  </>
                ) : (
                  <p>This conversation couldn't load. Retrying…</p>
                )}
              </div>
            ) : messages.length === 0 ? (
              <div className="welcome">
                <div className="compass" aria-hidden="true">
                  <ArrowUpRight size={43} aria-hidden="true" />
                </div>
                <p className="eyebrow">A LITTLE CURIOSITY GOES A LONG WAY</p>
                <h1>
                  {trip ? `Let's make ${trip.destination} yours.` : "Where do you want to go?"}
                </h1>
                <p>
                  Somewhere new. Somewhere familiar. Somewhere you've
                  <br className="desktop-break" /> been thinking about for years. Let's make a plan.
                </p>
                <div className="suggestions">
                  {[
                    "A slow weekend in Lisbon",
                    "A week of food and culture in Japan",
                    "Help me find my next adventure",
                  ].map((suggestion) => (
                    <button key={suggestion} onClick={() => setDraft(suggestion)}>
                      {suggestion}
                      <ArrowUpRight size={17} aria-hidden="true" />
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              <div
                className="messages"
                aria-live="polite"
                ref={transcript}
                onScroll={(event) => {
                  const area = event.currentTarget;

                  followResponse.current =
                    area.scrollHeight - area.scrollTop - area.clientHeight < 80;
                }}
              >
                {messages.map((message) => (
                  <article
                    key={message.id}
                    className={`message ${message.role}${message.content ? " has-cards" : ""}`}
                  >
                    <span className="message-label">
                      {message.role === "user" ? (
                        "YOU"
                      ) : (
                        <>
                          ELSEWHERE <ArrowUpRight size={13} aria-hidden="true" />
                        </>
                      )}
                    </span>
                    {message.content ? (
                      <TravelCards content={message.content} />
                    ) : message.role === "user" ? (
                      <p>{message.text}</p>
                    ) : (
                      <MessageText text={message.text} />
                    )}
                  </article>
                ))}
                <AgentProgress progress={visibleProgress} active={live !== null} busy={busy} />
              </div>
            )}
            <form
              className="composer"
              onSubmit={(event) => {
                event.preventDefault();
                if (canSend) send();
              }}
            >
              {!!snapshot?.scouts?.length && (
                <ResearchScoutCard key={selection.conversationId} scouts={snapshot.scouts} />
              )}
              {(snapshot?.app || snapshot?.editor) && (
                <TripAppCard
                  key={selection.conversationId}
                  app={snapshot.app ?? null}
                  editor={snapshot.editor ?? null}
                />
              )}
              {error && (
                <p className="error" role="alert">
                  {error}
                </p>
              )}
              <div className="input-wrap">
                <textarea
                  ref={composerInput}
                  aria-label="Message your travel planner"
                  placeholder={session ? "Tell me what you have in mind…" : "Signing you in…"}
                  disabled={!session}
                  value={draft}
                  onChange={(event) => setDraft(event.target.value)}
                  rows={1}
                  maxLength={4000}
                  onKeyDown={(event) => {
                    if (
                      event.key === "Enter" &&
                      !event.shiftKey &&
                      !event.nativeEvent.isComposing &&
                      event.keyCode !== 229
                    ) {
                      event.preventDefault();
                      if (canSend) send();
                    }
                  }}
                />
                <button className="send" aria-label="Send message" disabled={!canSend}>
                  <ArrowUp size={23} aria-hidden="true" />
                </button>
              </div>
              <p className="composer-note">
                <span className="composer-tagline">Dream it. Plan it. Make it yours.</span>
                <span>Check current prices and availability before booking.</span>
              </p>
            </form>
          </section>
          {trip && showTrip && (
            <TripDetail
              trip={trip}
              publishing={publishResult.waiting}
              onPublish={() => changeApp({ action: "create", tripId: trip.id })}
              onClose={() => setShowTrip(false)}
            />
          )}
        </div>
        {manageAccess && session?.isAdmin && <AccessPanel onClose={() => setManageAccess(false)} />}
        {inspect && !manageAccess && (
          <ActivityPanel
            snapshot={snapshot}
            progress={visibleProgress}
            onClose={() => setInspect(false)}
          />
        )}
      </main>
    </div>
  );
}

function ModelControls() {
  const settings = useAtomValue(settingsAtom);
  const status = useAtomValue(settingsStatusAtom);
  const change = useAtomSet(changeSettingsAtom);
  const [open, setOpen] = useState(false);
  const controls = useRef<HTMLDivElement>(null);
  const trigger = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;

    const dismiss = (event: PointerEvent) => {
      if (event.target instanceof Node && !controls.current?.contains(event.target)) setOpen(false);
    };

    document.addEventListener("pointerdown", dismiss);

    return () => document.removeEventListener("pointerdown", dismiss);
  }, [open]);

  return (
    <div
      className="model-controls"
      ref={controls}
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          setOpen(false);
          trigger.current?.focus();
        }
      }}
    >
      <button
        className={`icon-button speed-toggle ${settings.fast ? "active" : ""}`}
        aria-label={settings.fast ? "Fast mode enabled" : "Standard speed enabled"}
        aria-pressed={settings.fast}
        title={
          settings.fast
            ? "Fast mode · higher token rates. Click for Standard."
            : "Standard speed. Click for Fast mode at higher token rates."
        }
        onClick={() => change({ kind: "speed" })}
        disabled={status.loading}
      >
        <svg
          viewBox="0 0 24 24"
          width="18"
          height="18"
          fill={settings.fast ? "currentColor" : "none"}
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="m13 2-9 12h7l-1 8 10-13h-7z" />
        </svg>
      </button>
      <button
        className={`icon-button ${open ? "active" : ""}`}
        aria-label="Model settings"
        aria-expanded={open}
        aria-controls="model-settings"
        onClick={() => setOpen(!open)}
        ref={trigger}
      >
        <svg
          viewBox="0 0 24 24"
          width="18"
          height="18"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.7"
          strokeLinecap="round"
          strokeLinejoin="round"
          aria-hidden="true"
        >
          <path d="m9.5 3-.6 2-2 .9-1.9-.5-2 3.4 1.4 1.5v2.4L3 14.2l2 3.4 1.9-.5 2 .9.6 2h4.9l.6-2 2-.9 1.9.5 2-3.4-1.4-1.5v-2.4L21 8.8l-2-3.4-1.9.5-2-.9-.6-2z" />
          <circle cx="12" cy="11.5" r="3.2" />
        </svg>
      </button>
      {open && (
        <div
          className="model-settings"
          id="model-settings"
          role="dialog"
          aria-label="Planner settings"
        >
          <p className="eyebrow">MAKE IT YOURS</p>
          <h2>Planner settings</h2>
          <label htmlFor="planner-model">Model</label>
          <select
            id="planner-model"
            disabled={status.loading}
            value={settings.model}
            onChange={(event) => change({ kind: "model", value: event.target.value })}
          >
            <option value="gpt-5.6-luna">GPT-5.6 Luna</option>
            <option value="gpt-6-astra">GPT-6 Astra</option>
          </select>
          <label htmlFor="planner-reasoning">Reasoning</label>
          <select
            id="planner-reasoning"
            disabled={status.loading}
            value={settings.reasoningEffort}
            onChange={(event) => change({ kind: "reasoning", value: event.target.value })}
          >
            {settings.model === "gpt-5.6-luna" && <option value="none">None</option>}
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
            <option value="xhigh">Extra high</option>
            <option value="max">Maximum</option>
          </select>
          <p className="settings-help">More reasoning gives the planner more time to think.</p>
          <div className="settings-speed">
            <span>Processing</span>
            <strong>{settings.fast ? "Fast" : "Standard"}</strong>
          </div>
          <p className="settings-help">
            The lightning button switches speed. Fast uses higher token rates.
          </p>
          <p className="settings-footnote" role="status">
            {status.error ??
              (status.loading
                ? "Loading your preferences…"
                : status.saving
                  ? "Saving your preferences…"
                  : "Saved to your account. Changes apply to your next message.")}
          </p>
        </div>
      )}
    </div>
  );
}

function AccessPanel({ onClose }: { readonly onClose: () => void }) {
  const membersResult = useAtomValue(membersAtom);
  const members = Option.getOrNull(AsyncResult.value(membersResult));
  const [email, setEmail] = useAtom(memberEmailAtom);
  const [changeResult, change] = useAtom(manageMemberAtom);
  const refresh = useAtomSet(refreshMembersAtom);
  const error = failure(changeResult) ?? failure(membersResult);

  return (
    <aside className="activity-panel access-panel" aria-label="Manage access">
      <div className="activity-title">
        <h2>Manage access</h2>
        <button aria-label="Close access settings" onClick={onClose}>
          ×
        </button>
      </div>
      <p>
        Allow someone to sign in with their email, then share the{" "}
        <a href="/" target="_blank" rel="noreferrer">
          planner sign-in link
        </a>{" "}
        with them.
      </p>
      <form
        className="member-form"
        onSubmit={(event) => {
          event.preventDefault();
          change({ action: "invite" });
        }}
      >
        <label htmlFor="member-email">Email address</label>
        <input
          id="member-email"
          type="email"
          autoComplete="email"
          required
          maxLength={254}
          value={email}
          onChange={(event) => setEmail(event.target.value)}
          disabled={changeResult.waiting}
        />
        <button className="primary" disabled={!members || changeResult.waiting || !email.trim()}>
          {changeResult.waiting ? "Updating access…" : "Allow access"}
        </button>
      </form>
      {error && (
        <div className="error" role="alert">
          <p>{error}</p>
          <button
            className="quiet"
            disabled={membersResult.waiting || changeResult.waiting}
            onClick={() => refresh()}
          >
            {membersResult.waiting ? "Refreshing members…" : "Refresh members"}
          </button>
        </div>
      )}
      {AsyncResult.isSuccess(changeResult) && !changeResult.waiting && (
        <p role="status">{changeResult.value}</p>
      )}
      {!members && !error && <p role="status">Loading members…</p>}
      <ul className="member-list">
        {members?.emails.map((member) => (
          <li key={member}>
            <span>{member}</span>
            {member === members.adminEmail ? (
              <small>Administrator</small>
            ) : (
              <button
                className="quiet"
                aria-label={`Remove access for ${member}`}
                disabled={changeResult.waiting}
                onClick={() => change({ action: "remove", email: member })}
              >
                Remove
              </button>
            )}
          </li>
        ))}
      </ul>
      <p className="access-help">
        Removing an email prevents its next sign-in. Existing Cloudflare sessions can last up to 30
        days.
      </p>
    </aside>
  );
}

function TripDetail({
  trip,
  publishing,
  onPublish,
  onClose,
}: {
  readonly trip: Trip;
  readonly publishing: boolean;
  readonly onPublish: () => void;
  readonly onClose: () => void;
}) {
  const setDraft = useAtomSet(draftAtom);

  return (
    <aside className="trip-detail">
      <button
        className="icon-button trip-detail-close"
        aria-label="Close itinerary"
        onClick={onClose}
      >
        <X size={20} aria-hidden="true" />
      </button>
      <p className="eyebrow">YOUR NEXT CHAPTER</p>
      <h2>{trip.destination}</h2>
      <p className="trip-summary">{trip.summary}</p>
      <div className="trip-meta">
        <span>
          {trip.startDate ?? "Dates to discover"}
          {trip.endDate ? ` — ${trip.endDate}` : ""}
        </span>
        <span>
          {trip.travelers} {trip.travelers === 1 ? "traveler" : "travelers"}
        </span>
      </div>
      <div className="itinerary">
        {trip.days.map((day, index) => (
          <section key={index}>
            <span className="day-number">{String(index + 1).padStart(2, "0")}</span>
            <div>
              <h3>{day.title}</h3>
              <ul>
                {day.activities.map((activity, activityIndex) => (
                  <li key={activityIndex}>{activity}</li>
                ))}
              </ul>
            </div>
          </section>
        ))}
      </div>
      {trip.notes.length > 0 && (
        <div className="trip-notes">
          <h3>A few things to know</h3>
          <ul>
            {trip.notes.map((note, index) => (
              <li key={index}>
                <MessageText text={note} />
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="trip-actions">
        <button className="primary" disabled={publishing} onClick={onPublish}>
          {publishing ? "Creating your site…" : "Create trip website ↗"}
        </button>
        {trip.published && (
          <a href={trip.published.path} target="_blank" rel="noreferrer">
            View published trip · version {trip.published.revision} ↗
          </a>
        )}
        <button
          className="quiet"
          onClick={() => setDraft(`I'd like to change my ${trip.destination} trip. `)}
        >
          Keep shaping this trip
        </button>
        <p>Your app is public to anyone with the link. Ask the planner to customize it anytime.</p>
      </div>
    </aside>
  );
}
