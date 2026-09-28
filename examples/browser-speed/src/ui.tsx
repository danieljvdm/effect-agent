import { useAtom, useAtomSet, useAtomValue } from "@effect/atom-react";
import { Predicate, Schema } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  ArrowDown,
  ArrowUpRight,
  Check,
  ChevronRight,
  Circle,
  Download,
  Gauge,
  Layers,
  Play,
  Square,
  Terminal,
  X,
} from "lucide-react";
import { useState } from "react";

import {
  BrowserEngine,
  Grounding,
  ArticleTitle,
  defaultChallenge,
  racePrompt,
  ModelId,
  Reasoning,
  ServiceTier,
  modelChoices,
  Mode,
  scenarios,
  type Phase,
  type Report,
  type Scenario,
} from "./contract.ts";
import {
  cohort,
  comparisons,
  controlAtom,
  errorMessage,
  exportAtom,
  historyAtom,
  flowStarted,
  fromReady,
  runOutcome,
  verifiedMillis,
  percentile,
  progressAtom,
  runAtom,
  selectedAtom,
  snapshotAtom,
  submittedAtom,
  watchingAtom,
} from "./state.ts";

const seconds = (value: number | null | undefined) =>
  value === null || value === undefined ? "—" : `${(value / 1000).toFixed(2)}`;

const benchmarkReportUrl: unknown = import.meta.env.VITE_BROWSER_BENCHMARK_REPORT_URL;

const phaseNames: Record<Phase, string> = {
  setup: "Browser setup",
  model: "Model",
  decision: "Jev selection",
  action: "Action",
  observation: "Observation",
  wait: "Wait",
  capture: "Screenshot",
  verify: "Verification",
  cleanup: "Cleanup",
};

const Timeline = ({ report }: { report: Report | null }) => {
  const [expanded, setExpanded] = useState<number | null>(null);

  const extent = Math.max(
    1,
    report?.elapsed ?? 1,
    ...(report?.spans.map((span) => span.start + (span.duration ?? 0)) ?? []),
  );

  return (
    <section className="timeline">
      <div className="section-title">
        <div>
          <span className="eyebrow">UNDER THE HOOD</span>
          <h2>Where the time goes.</h2>
        </div>
        <span className="subtle mono">{report?.spans.length ?? 0} spans</span>
      </div>
      <div className="legend">
        {Object.entries(phaseNames)
          .filter(([key]) => key !== "wait")
          .map(([key, label]) => (
            <span key={key}>
              <i className={`phase-${key}`} />
              {label}
            </span>
          ))}
      </div>
      {!report?.spans.length ? (
        <div className="empty-trace">
          <Layers size={24} strokeWidth={1.3} />
          <p>Your run will unfold here.</p>
          <span>Model calls, browser actions, observations, and the time between them.</span>
        </div>
      ) : (
        <>
          <div className="timeline-axis">
            <span>OPERATION</span>
            <div>
              <span>0 s</span>
              <span>{seconds(extent / 2)} s</span>
              <span>{seconds(extent)} s</span>
            </div>
            <span>DURATION</span>
          </div>
          <div className="trace-scroll">
            {report.spans.map((span) => (
              <div key={span.id}>
                <button
                  className="trace-row"
                  onClick={() => setExpanded(expanded === span.id ? null : span.id)}
                  aria-expanded={expanded === span.id}
                >
                  <span className="trace-name">
                    <i className={`phase-${span.phase}`} />
                    {span.name}
                  </span>
                  <span className="track">
                    <span
                      className={`bar phase-${span.phase} ${span.outcome === "running" ? "pulsing" : ""}`}
                      style={{
                        left: `${(span.start / extent) * 100}%`,
                        width: `${Math.max(0.5, ((span.duration ?? extent - span.start) / extent) * 100)}%`,
                      }}
                    />
                  </span>
                  <span
                    className={`mono duration ${["failure", "defect"].includes(span.outcome) ? "error-text" : ""}`}
                  >
                    {span.duration === null ? "running" : `${Math.round(span.duration)} ms`}
                  </span>
                </button>
                {expanded === span.id && (
                  <div className="span-detail">
                    {phaseNames[span.phase]} · {span.outcome} · turn {span.turn}
                    {span.bytes === undefined
                      ? ""
                      : ` · ${span.bytes.toLocaleString()} observation bytes`}
                    {span.inputTokens === undefined ? "" : ` · ${span.inputTokens} input tokens`}
                    {span.outputTokens === undefined ? "" : ` · ${span.outputTokens} output tokens`}
                    {span.reasoningTokens === undefined
                      ? ""
                      : ` · ${span.reasoningTokens} reasoning tokens`}
                    {span.serviceTier === undefined ? "" : ` · served tier: ${span.serviceTier}`}
                    {span.model === undefined ? "" : ` · ${span.model}`}
                    {span.candidateCount === undefined
                      ? ""
                      : ` · ${span.candidateCount} candidate links`}
                    {span.questionCount === undefined ? "" : ` · ${span.questionCount} questions`}
                    {span.reasoningSummary && (
                      <p>
                        <strong>Reasoning summary</strong>
                        <br />
                        {span.reasoningSummary}
                      </p>
                    )}
                    {span.error && <div className="error-text">{span.error}</div>}
                    {span.choices?.map((choice) => (
                      <div key={`${choice.target}/${choice.ref}`}>
                        {choice.target} → #{choice.ref} · {(choice.probability * 100).toFixed(1)}%
                        probability
                      </div>
                    ))}
                    {span.decisionDistributions
                      ?.filter((value) => Math.abs(value.reportedMass - 1) > 1e-6)
                      .map((value) => (
                        <div key={value.question}>
                          Jev reported {(value.reportedProbability * 100).toFixed(1)}% for #
                          {value.ref}; distribution totaled {value.reportedMass.toFixed(2)}.
                          Two-decimal rounding normalized for validation; choice unchanged.
                        </div>
                      ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </>
      )}
      <p className="trace-note">
        Full request trace, including preparation and cleanup. The flow clock starts at{" "}
        {seconds(report?.readyAt)} s on this trace. Overlapping spans are not additive. Live View
        and screenshots can affect latency.
      </p>
    </section>
  );
};

export const App = () => {
  const [scenario, setScenario] = useState<Scenario>("wikipedia");
  const [wikiStart, setWikiStart] = useState(defaultChallenge.start);
  const [wikiTarget, setWikiTarget] = useState(defaultChallenge.target);
  const [prompt, setPrompt] = useState<string>(scenarios[0].prompt);
  const [mode, setMode] = useState<typeof Mode.Type>("agent");
  const [model, setModel] = useState<ModelId | "all">("gpt-6-luna");
  const [engine, setEngine] = useState<typeof BrowserEngine.Type | "all">("chromium");
  const [grounding, setGrounding] = useState<typeof Grounding.Type>("direct");
  const [wikiDriver, setWikiDriver] = useState<"model" | "jev">("model");
  const [reasoning, setReasoning] = useState<typeof Reasoning.Type>("none");
  const [serviceTier, setServiceTier] = useState<typeof ServiceTier.Type>("fast");
  const [screenshots, setScreenshots] = useState(false);
  const [liveView, setLiveView] = useState(true);
  const [repetitions, setRepetitions] = useState(1);
  const [run, start] = useAtom(runAtom);
  const [control, act] = useAtom(controlAtom);
  const snapshot = useAtomValue(snapshotAtom);
  const history = useAtomValue(historyAtom);
  const watching = useAtomValue(watchingAtom);
  const submitted = useAtomValue(submittedAtom);
  const progress = useAtomValue(progressAtom);
  const [selected, select] = useAtom(selectedAtom);
  const download = useAtomSet(exportAtom);
  const remote = AsyncResult.isSuccess(snapshot) ? snapshot.value : null;
  const selectedSample = history.find(({ report }) => report.input.id === selected);
  const report = selectedSample?.report ?? remote?.report ?? history.at(-1)?.report ?? null;
  const busy = watching || remote?.busy === true;

  const matches = report ? cohort(history, report) : [];
  const comparison = report ? comparisons(history, report) : [];
  const configuredModels = remote?.models.filter((candidate) => candidate.configured) ?? [];

  const modelConfigured =
    model === "all"
      ? configuredModels.length > 0
      : configuredModels.some((candidate) => candidate.id === model);

  const verified = matches.flatMap(({ report }) => {
    const time = verifiedMillis(report);

    return report.status === "passed" && time !== null ? [time] : [];
  });

  const started = matches.filter(({ report }) => flowStarted(report)).length;
  const preparing = busy && (Predicate.isNullish(remote?.report?.readyAt) || !remote?.busy);
  const legacy = report !== null && report.timing !== "page-ready-v1";

  const failure = errorMessage(run) ?? errorMessage(control) ?? errorMessage(snapshot);
  const connected = remote !== null;

  const isWiki = scenario === "wikipedia";
  const jevOnly = isWiki && wikiDriver === "jev";
  const effectivePrompt = isWiki ? racePrompt({ start: wikiStart, target: wikiTarget }) : prompt;

  const validChallenge =
    Schema.is(ArticleTitle)(wikiStart.trim()) &&
    Schema.is(ArticleTitle)(wikiTarget.trim()) &&
    wikiStart.trim() !== wikiTarget.trim();

  const canRun =
    (!isWiki || validChallenge) &&
    connected &&
    remote.browserConfigured &&
    !busy &&
    !control.waiting &&
    (jevOnly
      ? remote.jevConfigured
      : mode === "scripted"
        ? scenario !== "custom"
        : modelConfigured && (grounding === "direct" || remote.jevConfigured));

  const live = !selected && remote?.liveViewUrl;
  const captured = !selected && remote?.image;

  return (
    <div className="app-shell">
      <header className="app-header">
        <a href="/" className="brand">
          <span className="brand-icon">
            <Gauge size={20} />
          </span>
          <strong>
            Browser speed lab<span> / effect-agent</span>
          </strong>
        </a>
        <div className="header-right">
          <span className={`connection ${connected ? "connected" : ""}`}>
            <i />
            {connected
              ? remote.browserConfigured
                ? "Cloudflare Browser Run configured"
                : "Cloudflare setup needed"
              : "Connecting to Worker…"}
          </span>
          <span className="version mono">EXPERIMENT 001</span>
        </div>
      </header>
      <div className="workspace">
        <aside className="command-panel">
          <div className="panel-heading">
            <span className="eyebrow">CLOUDFLARE BROWSER RUN</span>
            <h1>
              Run a
              <br /> browser task<span>.</span>
            </h1>
            <p>
              Start on Mars. Let an agent find Nelson Mandela by following Wikipedia links. Watch
              its route and see where the time goes.
            </p>
          </div>
          <div className="presets">
            <div className="label-row">
              <span className="eyebrow">START WITH A BENCHMARK</span>
              <span className="mono">01—04</span>
            </div>
            <button
              disabled={busy}
              className={`preset ${isWiki ? "selected" : ""}`}
              onClick={() => {
                setScenario("wikipedia");
                setMode("agent");
              }}
            >
              <span className="preset-number mono">01</span>
              <span>
                <strong>Wikipedia race</strong>
                <small>Find a route through real article links</small>
              </span>
              <ChevronRight size={15} />
            </button>
            {scenarios.map((preset, index) => (
              <button
                key={preset.id}
                disabled={busy}
                className={`preset ${scenario === preset.id ? "selected" : ""}`}
                onClick={() => {
                  setScenario(preset.id);
                  setPrompt(preset.prompt);
                }}
              >
                <span className="preset-number mono">0{index + 2}</span>
                <span>
                  <strong>{preset.title}</strong>
                  <small>{preset.detail}</small>
                </span>
                <ChevronRight size={15} />
              </button>
            ))}
          </div>
          <form
            id="task-form"
            className="composer"
            onSubmit={(event) => {
              event.preventDefault();
              start({
                scenario,
                mode,
                prompt: effectivePrompt,
                ...(isWiki
                  ? { wikipedia: { start: wikiStart.trim(), target: wikiTarget.trim() } }
                  : {}),
                screenshots,
                liveView: liveView && engine !== "kitesurf",
                repetitions,
                ...(engine === "all" ? { compareEngines: ["chromium", "kitesurf"] } : { engine }),
                ...(jevOnly
                  ? { wikiDriver: "jev" }
                  : { grounding: mode === "scripted" ? "direct" : grounding }),
                ...(!jevOnly && mode !== "scripted" && !model.startsWith("@cf/")
                  ? { reasoning, serviceTier }
                  : {}),
                ...(jevOnly || mode === "scripted"
                  ? {}
                  : model === "all"
                    ? { compareModels: configuredModels.map((candidate) => candidate.id) }
                    : { model }),
              });
            }}
          >
            {isWiki && (
              <div className="race-fields">
                <label htmlFor="wiki-start">
                  Starting article
                  <input
                    id="wiki-start"
                    value={wikiStart}
                    disabled={busy}
                    maxLength={180}
                    onChange={(event) => setWikiStart(event.target.value)}
                  />
                </label>
                <label htmlFor="wiki-target">
                  Destination
                  <input
                    id="wiki-target"
                    value={wikiTarget}
                    disabled={busy}
                    maxLength={180}
                    onChange={(event) => setWikiTarget(event.target.value)}
                  />
                </label>
              </div>
            )}
            <label htmlFor="prompt" className="eyebrow">
              {scenario === "custom" ? "YOUR REQUEST · UNVERIFIED" : "TASK PROMPT"}
            </label>
            <textarea
              id="prompt"
              value={effectivePrompt}
              readOnly={isWiki}
              disabled={busy}
              onChange={(event) => {
                setPrompt(event.target.value);
                setScenario("custom");
              }}
              maxLength={2000}
            />
            <div className="composer-footer">
              <span>
                <Circle size={11} />{" "}
                {isWiki
                  ? "Article links only · 20 hops maximum"
                  : scenario === "custom"
                    ? "Free-form task"
                    : "Independent verification"}
              </span>
            </div>
          </form>
          <div className="configuration">
            <label className="eyebrow" htmlFor="browser-engine">
              BROWSER
            </label>
            <select
              id="browser-engine"
              value={engine}
              disabled={busy}
              onChange={(event) =>
                setEngine(
                  event.target.value === "all"
                    ? "all"
                    : Schema.decodeUnknownSync(BrowserEngine)(event.target.value),
                )
              }
            >
              <option value="kitesurf">Kitesurf · beta</option>
              <option value="chromium">Chromium</option>
              <option value="all">Compare both browsers</option>
            </select>
            {typeof benchmarkReportUrl === "string" && benchmarkReportUrl.length > 0 && (
              <p className="hint">
                <a href={benchmarkReportUrl}>View the published browser comparison →</a>
              </p>
            )}
            {isWiki && (
              <>
                <label className="eyebrow" htmlFor="wiki-driver">
                  ROUTE DECISIONS
                </label>
                <select
                  id="wiki-driver"
                  value={wikiDriver}
                  disabled={busy}
                  onChange={(event) =>
                    setWikiDriver(event.target.value === "jev" ? "jev" : "model")
                  }
                >
                  <option value="model">Model chooses the route</option>
                  <option value="jev" disabled={!remote?.jevConfigured}>
                    Jev only · no planner
                  </option>
                </select>
              </>
            )}
            {!jevOnly && (
              <>
                <label className="eyebrow" htmlFor="model">
                  MODEL
                </label>
                <select
                  id="model"
                  value={model}
                  disabled={busy || mode === "scripted"}
                  onChange={(event) =>
                    setModel(
                      event.target.value === "all"
                        ? "all"
                        : Schema.decodeUnknownSync(ModelId)(event.target.value),
                    )
                  }
                >
                  {modelChoices.map((candidate) => (
                    <option
                      key={candidate.id}
                      value={candidate.id}
                      disabled={!configuredModels.some((value) => value.id === candidate.id)}
                    >
                      {candidate.label}
                    </option>
                  ))}
                  <option value="all" disabled={configuredModels.length < 2}>
                    Compare configured models
                  </option>
                </select>
                <div className="config-pair">
                  <div>
                    <label className="eyebrow" htmlFor="service-tier">
                      OPENAI SPEED
                    </label>
                    <select
                      id="service-tier"
                      value={serviceTier}
                      disabled={busy || mode === "scripted" || model.startsWith("@cf/")}
                      onChange={(event) =>
                        setServiceTier(Schema.decodeUnknownSync(ServiceTier)(event.target.value))
                      }
                    >
                      <option value="fast">Fast</option>
                      <option value="default">Standard</option>
                    </select>
                  </div>
                  <div>
                    <label className="eyebrow" htmlFor="reasoning">
                      REASONING
                    </label>
                    <select
                      id="reasoning"
                      value={reasoning}
                      disabled={busy || mode === "scripted" || model.startsWith("@cf/")}
                      onChange={(event) =>
                        setReasoning(Schema.decodeUnknownSync(Reasoning)(event.target.value))
                      }
                    >
                      {Reasoning.literals.map((effort) => (
                        <option key={effort} value={effort}>
                          {effort}
                        </option>
                      ))}
                    </select>
                  </div>
                </div>
                <label className="eyebrow" htmlFor="grounding">
                  ELEMENT SELECTION
                </label>
                <select
                  id="grounding"
                  value={grounding}
                  disabled={busy || mode === "scripted"}
                  onChange={(event) =>
                    setGrounding(Schema.decodeUnknownSync(Grounding)(event.target.value))
                  }
                >
                  <option value="direct">Agent chooses the element</option>
                  <option value="jev" disabled={!remote?.jevConfigured}>
                    Jev · DecisionModel{remote?.jevConfigured ? "" : " · key needed"}
                  </option>
                </select>
              </>
            )}
            <label className="eyebrow" htmlFor="mode">
              EXECUTION
            </label>
            <select
              id="mode"
              value={mode}
              disabled={busy || isWiki}
              onChange={(event) => {
                const value = Schema.decodeUnknownSync(Mode)(event.target.value);

                setMode(value);
              }}
            >
              <option value="agent">Agent · individual actions</option>
              <option value="batched">Agent · batched actions</option>
              <option value="scripted">Scripted browser baseline</option>
            </select>
            <div>
              <label className="eyebrow" htmlFor="repeat">
                REPETITIONS
              </label>
              <select
                id="repeat"
                disabled={busy}
                value={repetitions}
                onChange={(event) => setRepetitions(Number(event.target.value))}
              >
                <option value={1}>1 run</option>
                <option value={3}>3 runs</option>
                <option value={10}>10 runs</option>
              </select>
            </div>
            <div className="toggle-row">
              <label>
                <input
                  type="checkbox"
                  checked={liveView && engine !== "kitesurf"}
                  disabled={busy || engine === "kitesurf"}
                  onChange={(event) => setLiveView(event.target.checked)}
                />{" "}
                Live View · Chromium
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={screenshots}
                  disabled={busy}
                  onChange={(event) => setScreenshots(event.target.checked)}
                />{" "}
                Tool screenshots
              </label>
            </div>
            <p className="hint">
              The clock starts when the starting page and its links are ready. Browser preparation
              is excluded.
            </p>
            <p className="hint">
              {jevOnly
                ? "Jev chooses the next article from all eligible links using the destination and route history. Large pages use grouped choices, then a final choice. No planner model calls."
                : grounding === "jev"
                  ? "The agent describes the control; Jev selects an observed element. Decision time is measured separately."
                  : model === "all"
                    ? "Each repetition runs every configured model in a rotating order."
                    : "Compare the same task across models and element selectors."}
            </p>
          </div>
          {failure && (
            <div className="error-banner" role="alert">
              {failure}
            </div>
          )}
          {connected && !remote.browserConfigured && (
            <div className="error-banner" role="alert">
              Cloudflare is not configured. Set CLOUDFLARE_ACCOUNT_ID and
              BROWSER_RENDERING_API_TOKEN in the Worker to start a real browser.
            </div>
          )}
          {connected && !jevOnly && mode !== "scripted" && !modelConfigured && (
            <div className="hint">
              Set OPENAI_API_KEY in the Worker, or choose the scripted baseline.
            </div>
          )}
          <div className="panel-bottom">
            <Terminal size={14} />
            <span>One agent. One tab. Real browser actions.</span>
          </div>
        </aside>
        <main className="main-panel">
          <div className="run-heading">
            <div>
              <span className="eyebrow">THE EXPERIMENT</span>
              <h2>
                {busy
                  ? `${preparing ? "Preparing browser" : Predicate.isNotNullish(report?.finishedAt) ? "Finishing" : "Running benchmark"} · ${progress.current || 1}/${progress.total || 1}`
                  : report
                    ? "Run complete"
                    : !remote?.browserConfigured
                      ? "Waiting for Cloudflare configuration"
                      : isWiki
                        ? `${wikiStart} → ${wikiTarget}`
                        : "Press Run task to start"}
              </h2>
            </div>
            <div className="run-buttons">
              <button
                className="text-button"
                disabled={!remote?.canClose || control.waiting}
                onClick={() => act("close")}
              >
                Close browser <X size={14} />
              </button>
              {busy ? (
                <button
                  className="stop-button"
                  onClick={() => act("stop")}
                  disabled={control.waiting}
                >
                  <Square size={12} fill="currentColor" /> Stop run
                </button>
              ) : (
                <>
                  <button
                    className="send-button"
                    disabled={!canRun || !effectivePrompt.trim()}
                    type="submit"
                    form="task-form"
                    aria-label={isWiki ? "Start race" : "Run task"}
                  >
                    <Play size={14} /> {isWiki ? "Start race" : "Run task"}
                  </button>
                  <span className={`status-pill ${report?.status ?? "idle"}`}>
                    {report?.status === "passed" ? <Check size={13} /> : <Circle size={10} />}{" "}
                    {report ? runOutcome(report) : "Standing by"}
                  </span>
                </>
              )}
            </div>
          </div>
          <div className="metrics">
            <div className="metric primary">
              <span>
                {legacy ? "LEGACY TASK TIME" : "FLOW TIME"} <small>worker</small>
              </span>
              <strong>
                {seconds(
                  report
                    ? (verifiedMillis(report) ??
                        fromReady(report, report.finishedAt ?? report.elapsed))
                    : null,
                )}
                <em>s</em>
              </strong>
              <p>
                {legacy
                  ? "Admission → verified (old timing)"
                  : report && !flowStarted(report)
                    ? "Flow clock has not started"
                    : report?.verifiedAt !== null && report?.verifiedAt !== undefined
                      ? "Starting page ready → verified success"
                      : report?.status === "failed" || report?.status === "cancelled"
                        ? "Page ready → stopped without success"
                        : report?.status === "running"
                          ? "Page ready → running now"
                          : "Starting page ready → verified success"}
              </p>
            </div>
            <div className="metric">
              <span>
                FIRST ACTION <small>worker</small>
              </span>
              <strong>
                {seconds(report ? fromReady(report, report.firstActionAt) : null)}
                <em>s</em>
              </strong>
              <p>{legacy ? "Admission" : "Page ready"} → first completed interaction</p>
            </div>
            <div className="metric">
              <span>
                PREPARATION <small>excluded</small>
              </span>
              <strong>
                {seconds(
                  report && !legacy
                    ? (report.readyAt ?? report.finishedAt ?? report.elapsed)
                    : null,
                )}
                <em>s</em>
              </strong>
              <p>Browser, starting page and initial observation</p>
            </div>
            <div className="metric compact">
              <span>PLANNER / JEV CALLS</span>
              <strong>
                {report
                  ? `${report.spans.filter((span) => span.phase === "model").length} / ${report.spans.filter((span) => span.phase === "decision").length}`
                  : "—"}
              </strong>
              <p>
                {report
                  ? `${report.spans.filter((span) => span.phase === "action").length} browser actions`
                  : "The cost of deciding what’s next"}
              </p>
            </div>
          </div>
          {(report?.race || isWiki) && (
            <section className="race-route" aria-label="Wikipedia route">
              <div className="label-row">
                <span className="eyebrow">
                  {report?.race ? `ROUTE TO ${report.race.target.toUpperCase()}` : "THE CHALLENGE"}
                </span>
                <span className="mono">
                  {report?.race
                    ? `${report.race.path.length - 1} / ${report.race.maxHops} hops`
                    : "20 hops maximum"}
                </span>
              </div>
              {report?.race ? (
                <ol>
                  {report.race.path.map((hop, index) => (
                    <li key={`${index}-${hop.url}`}>
                      <span className="mono">{index}</span>
                      <a href={hop.url} target="_blank" rel="noreferrer">
                        {hop.title}
                      </a>
                      <small>
                        {index === 0
                          ? "Start"
                          : `${seconds(report ? fromReady(report, hop.at) : null)} s`}
                      </small>
                    </li>
                  ))}
                </ol>
              ) : (
                <p>
                  {effectivePrompt} Click <strong>Start race</strong> to watch the agent find its
                  own way.
                </p>
              )}
            </section>
          )}
          <section className="browser-panel">
            <div className="browser-chrome">
              <div className="traffic">
                <i />
                <i />
                <i />
              </div>
              <div className="address">
                <span>◈</span> Cloudflare Browser Run
              </div>
              <div className="browser-label">
                <i className={busy ? "active-dot" : ""} />
                {live
                  ? "LIVE · READ ONLY"
                  : captured
                    ? "BROWSER SCREENSHOT"
                    : "NO BROWSER VIEW YET"}
              </div>
            </div>
            <div className="browser-content">
              {live ? (
                <iframe
                  key={live}
                  title="Read-only agent browser"
                  src={live}
                  referrerPolicy="no-referrer"
                  allow="clipboard-read; clipboard-write"
                />
              ) : captured ? (
                <img src={captured} alt="Latest screenshot of the agent’s browser" />
              ) : (
                <div className="browser-empty">
                  <Play size={32} strokeWidth={1.3} />
                  <h3>
                    {busy
                      ? "Connecting to the Cloudflare browser…"
                      : "Your remote browser appears here"}
                  </h3>
                  <p>
                    {isWiki
                      ? "A real Cloudflare browser will open Wikipedia. The agent chooses links, one hop at a time. No search or URL shortcuts."
                      : "Each run opens a task board inside Cloudflare Chrome. The agent creates and edits tasks so you can compare the same workload across runs."}
                  </p>
                  <span>
                    {isWiki
                      ? "Mars → … → Nelson Mandela"
                      : "1. Choose a task → 2. Run task → 3. Watch the browser and timings"}
                  </span>
                </div>
              )}
              {!live && !captured && (
                <div className="preview-badge">
                  {busy
                    ? "Browser running · visual capture off or connecting"
                    : "Cloudflare-hosted browser · real API calls"}
                </div>
              )}
            </div>
            {live && (
              <a className="live-link" href={live} target="_blank" rel="noreferrer">
                Open Live View in a new tab <ArrowUpRight size={13} />
              </a>
            )}
          </section>
          {remote?.notice && <div className="notice">{remote.notice}</div>}
          {submitted && !selected && (
            <div className="activity">
              <span className="activity-icon">
                {busy ? (
                  <Play size={13} />
                ) : failure || report?.status === "failed" ? (
                  <X size={14} />
                ) : (
                  <Check size={14} />
                )}
              </span>
              <div>
                <strong>
                  {busy
                    ? preparing
                      ? "Preparing the starting page · benchmark clock has not started"
                      : (report?.message ?? "Running benchmark")
                    : (failure ?? report?.message ?? "Waiting for the Worker")}
                </strong>
                <p>{submitted}</p>
              </div>
              {busy && <span className="working-dots">•••</span>}
            </div>
          )}
          <Timeline report={report} />
          <section className="results">
            <div className="section-title">
              <div>
                <span className="eyebrow">REPEAT. COMPARE. IMPROVE.</span>
                <h2>Run history</h2>
              </div>
              <button className="text-button" disabled={!history.length} onClick={() => download()}>
                <Download size={14} /> Export JSON
              </button>
            </div>
            {history.length ? (
              <>
                <div className="cohort">
                  <span>
                    Matching preset runs <b>{matches.length}</b>
                  </span>
                  <span>
                    Verified{" "}
                    <b>
                      {verified.length}/{started}
                    </b>
                  </span>
                  <span>
                    Preparation failures{" "}
                    <b>
                      {
                        matches.filter(
                          ({ report }) => !flowStarted(report) && report.status !== "running",
                        ).length
                      }
                    </b>
                  </span>
                  <span>
                    Median <b>{seconds(percentile(verified, 0.5))} s</b>
                  </span>
                  <span>
                    p95{" "}
                    <b>
                      {verified.length >= 20
                        ? `${seconds(percentile(verified, 0.95))} s`
                        : "20 successes needed"}
                    </b>
                  </span>
                </div>
                <div className="history-table">
                  {report?.race && comparison.length > 1 && (
                    <p className="hint">
                      Strategy comparison: Jev-only sees all eligible links; model planners see 80
                      per page. Compare success rate and hops alongside time.
                    </p>
                  )}
                  {comparison.length > 1 && (
                    <table aria-label="Run comparison">
                      <thead>
                        <tr>
                          <th>Model</th>
                          <th>Browser</th>
                          <th>Strategy</th>
                          <th>Reasoning</th>
                          <th>Tier requested / served</th>
                          <th>Verified / started</th>
                          <th>Preparation failed</th>
                          <th>Median flow</th>
                          {report?.race && <th>Median hops</th>}
                        </tr>
                      </thead>
                      <tbody>
                        {comparison.map((group) => (
                          <tr key={group.key}>
                            <td>
                              {group.wikiDriver === "jev"
                                ? "Jev only"
                                : (modelChoices.find((value) => value.id === group.model)?.label ??
                                  group.model)}
                            </td>
                            <td
                              title={[group.browserVersion, group.browserRevision]
                                .filter(Boolean)
                                .join(" · ")}
                            >
                              {group.engine === "kitesurf" ? "Kitesurf" : "Chromium"}
                            </td>
                            <td>
                              {group.wikiDriver === "jev"
                                ? "All links"
                                : group.grounding === "jev"
                                  ? "Jev elements"
                                  : "Model elements"}
                            </td>
                            <td>{group.reasoning}</td>
                            <td>
                              {group.serviceTier} / {group.servedTier}
                            </td>
                            <td>
                              {group.passed}/{group.started}
                            </td>
                            <td>{group.preparationFailed}</td>
                            <td className="mono">{seconds(group.median)} s</td>
                            {report?.race && <td>{group.hops ?? "—"}</td>}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                  <table>
                    <thead>
                      <tr>
                        <th>Task</th>
                        <th>Mode</th>
                        <th>Model / elements</th>
                        <th>Browser</th>
                        <th>Reasoning / speed</th>
                        <th>Timing</th>
                        <th>Flow time</th>
                        <th>Outcome</th>
                        <th />
                      </tr>
                    </thead>
                    <tbody>
                      {[...history].reverse().map(({ report: row }) => (
                        <tr
                          key={row.input.id}
                          className={selected === row.input.id ? "selected-row" : ""}
                        >
                          <td>
                            {row.input.scenario === "wikipedia"
                              ? `${row.race?.start ?? row.input.wikipedia?.start ?? defaultChallenge.start} → ${row.race?.target ?? row.input.wikipedia?.target ?? defaultChallenge.target}${row.race ? ` (${row.race.path.length - 1} hops)` : ""}`
                              : (scenarios.find((value) => value.id === row.input.scenario)
                                  ?.title ?? "Free-form request")}
                          </td>
                          <td>{row.input.mode}</td>
                          <td>
                            {row.input.wikiDriver === "jev"
                              ? "Jev only"
                              : (modelChoices.find((value) => value.id === row.model)?.label ??
                                row.model)}
                            <small>
                              {" "}
                              /{" "}
                              {row.input.wikiDriver === "jev"
                                ? "all links"
                                : row.input.grounding === "jev"
                                  ? "Jev"
                                  : "Agent"}
                            </small>
                          </td>
                          <td
                            title={[row.browserVersion, row.browserRevision]
                              .filter(Boolean)
                              .join(" · ")}
                          >
                            {row.input.engine === "kitesurf" ? "Kitesurf" : "Chromium"}
                          </td>
                          <td>
                            {row.input.wikiDriver === "jev" ||
                            row.model.startsWith("@cf/") ||
                            row.input.mode === "scripted"
                              ? "n/a"
                              : `${row.input.reasoning ?? "provider-default"} / ${row.input.serviceTier ?? "provider-default"}`}
                          </td>
                          <td>
                            {row.timing === "page-ready-v1" ? "Page ready" : "Legacy admission"}
                          </td>
                          <td className="mono">{seconds(verifiedMillis(row))} s</td>
                          <td>
                            <span className={`result-status ${row.status}`}>{runOutcome(row)}</span>
                          </td>
                          <td>
                            <button
                              className="icon-button"
                              aria-label="Inspect run"
                              onClick={() =>
                                select(selected === row.input.id ? null : row.input.id)
                              }
                            >
                              <ArrowUpRight size={15} />
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </>
            ) : (
              <div className="empty-history">
                <ArrowDown size={18} />
                <p>No runs yet. Start with a preset to establish your baseline.</p>
              </div>
            )}
            <p className="trace-note">
              Statistics use successful runs of the same challenge and configuration. Live Wikipedia
              content and routes can change between runs. Flow failures stay in the success
              denominator; preparation failures are counted separately. Legacy admission timings are
              never pooled with page-ready timings. History is kept in this tab.
            </p>
          </section>
          <footer className="main-footer">
            <span>BROWSER SPEED LAB</span>
            <span>Measure the work. Then make it faster.</span>
            <span className="mono">v0.1</span>
          </footer>
        </main>
      </div>
    </div>
  );
};
