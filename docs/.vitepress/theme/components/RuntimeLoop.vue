<template>
  <figure class="runtime-loop" aria-labelledby="runtime-loop-caption">
    <div class="loop-entry"><span>Input</span> Decode Schema · evaluate instructions</div>
    <ol class="loop-steps" aria-label="Agent turn sequence">
      <li>
        <a class="loop-step" href="#context-and-compaction">
          <span class="step-number">01</span>
          <span><strong>Prepare context</strong><small>Build the next model prompt</small></span>
        </a>
        <div class="loop-note">
          <a href="#context-and-compaction">Compact when needed</a>
          <span>Prune old results, then summarize. Keep canonical history.</span>
        </div>
      </li>
      <li>
        <a class="loop-step" href="#validate-before-running-tools">
          <span class="step-number">02</span>
          <span
            ><strong>Call the model</strong
            ><small>Stream, then validate the full response</small></span
          >
        </a>
        <div class="loop-note loop-exit">
          <a href="#budgets-and-stopping">Final response → complete</a>
          <span>Decode output. Check for queued input before finishing.</span>
        </div>
      </li>
      <li>
        <a class="loop-step" href="#validate-before-running-tools">
          <span class="step-number">03</span>
          <span
            ><strong>Run the tool batch</strong
            ><small>Preflight all calls · execute within limits</small></span
          >
        </a>
        <div class="loop-note">
          <a href="#subagents">Tools can delegate</a>
          <span>A child runs its own loop. Join its bounded result here.</span>
        </div>
      </li>
      <li>
        <a class="loop-step" href="#apply-input-between-turns">
          <span class="step-number">04</span>
          <span
            ><strong>Commit and continue</strong
            ><small>Ordered results · steering · policy checks</small></span
          >
        </a>
        <div class="loop-note">
          <a href="#budgets-and-stopping">Budget remaining?</a>
          <span>Continue, finalize within policy, or fail with a typed error.</span>
        </div>
      </li>
    </ol>
    <div class="loop-return">
      <span aria-hidden="true">↖</span> Next turn uses the updated context
    </div>
    <a class="loop-storage" href="#storage-and-recovery">
      <strong>Durability &amp; storage</strong>
      <span>Record progress at the boundaries. Recover the same run across attempts.</span>
      <span aria-hidden="true">↗</span>
    </a>
    <figcaption id="runtime-loop-caption">
      The tool path repeats. A final response or completion tool can finish the run; policy can stop
      it.
    </figcaption>
  </figure>
</template>

<style scoped>
.runtime-loop {
  container-type: inline-size;
  margin: 24px 0;
  padding: 24px;
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  background: var(--vp-c-bg-elv);
  font-size: 14px;
  line-height: 1.5;
}
.loop-entry {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 16px;
  margin: 0 0 22px 24px;
  color: var(--vp-c-text-2);
}
.loop-entry > span,
.step-number {
  color: var(--vp-c-brand-1);
  font-family: var(--vp-font-family-mono);
  font-size: 12px;
}
.runtime-loop .loop-steps {
  position: relative;
  margin: 0;
  padding: 0 0 0 24px;
  list-style: none;
}
.loop-steps::before {
  position: absolute;
  top: 36px;
  bottom: -22px;
  left: 0;
  width: 16px;
  border: solid var(--vp-c-brand-1);
  border-width: 2px 0 2px 2px;
  border-radius: 10px 0 0 10px;
  content: "";
}
.loop-steps::after {
  position: absolute;
  top: 32px;
  left: 10px;
  width: 9px;
  height: 9px;
  border: solid var(--vp-c-brand-1);
  border-width: 2px 2px 0 0;
  transform: rotate(45deg);
  content: "";
}
.runtime-loop .loop-steps > li {
  display: grid;
  grid-template-columns: minmax(0, 1.3fr) minmax(0, 1fr);
  align-items: center;
  gap: 24px;
  margin: 0;
  padding: 0 0 24px;
}
.runtime-loop .loop-steps > li:last-child {
  padding-bottom: 0;
}
.runtime-loop a {
  text-decoration: none;
}
.runtime-loop a:focus-visible {
  outline: 2px solid var(--vp-c-brand-1);
  outline-offset: 4px;
}
.runtime-loop .loop-step {
  position: relative;
  display: flex;
  align-items: baseline;
  gap: 12px;
  height: 100%;
  padding: 16px;
  border: 1px solid var(--vp-c-border);
  border-radius: 6px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
}
.loop-step strong {
  display: block;
  font-size: 16px;
  font-weight: 600;
}
.loop-step small {
  display: block;
  margin-top: 3px;
  color: var(--vp-c-text-2);
  font-size: 13px;
  font-weight: 400;
}
.loop-steps > li:not(:last-child) .loop-step::after {
  position: absolute;
  bottom: -24px;
  left: 24px;
  height: 24px;
  color: var(--vp-c-brand-1);
  line-height: 24px;
  content: "↓";
}
.loop-note {
  position: relative;
  padding: 4px 0;
}
.loop-steps > li:nth-child(2) .loop-step::after {
  font-size: 12px;
  content: "↓ tool calls";
}
.loop-note > a {
  font-weight: 600;
}
.loop-note > span {
  display: block;
  margin-top: 4px;
  color: var(--vp-c-text-2);
  font-size: 13px;
}
.loop-exit::before {
  position: absolute;
  top: 9px;
  left: -22px;
  color: var(--vp-c-brand-1);
  content: "→";
}
.loop-return {
  margin: 10px 0 22px 24px;
  color: var(--vp-c-brand-1);
  font-size: 12px;
}
.runtime-loop .loop-storage {
  display: flex;
  align-items: baseline;
  gap: 12px;
  padding: 14px 16px;
  border-radius: 6px;
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-text-2);
  font-size: 13px;
  font-weight: 400;
}
.loop-storage strong {
  flex-shrink: 0;
  color: var(--vp-c-brand-1);
}
.runtime-loop a:hover {
  color: var(--vp-c-brand-1);
  text-decoration: underline;
  text-underline-offset: 3px;
}
.runtime-loop figcaption {
  margin-top: 14px;
  color: var(--vp-c-text-2);
  font-size: 12px;
}
@media (max-width: 640px) {
  .runtime-loop {
    padding: 16px;
  }
}
@container (max-width: 560px) {
  .loop-entry {
    margin-left: 20px;
  }
  .runtime-loop .loop-steps {
    padding-left: 20px;
  }
  .runtime-loop .loop-steps > li {
    grid-template-columns: minmax(0, 1fr);
    gap: 8px;
  }
  .loop-note {
    margin-left: 16px;
    padding-left: 12px;
    border-left: 1px solid var(--vp-c-divider);
  }
  .loop-steps > li:not(:last-child) .loop-step::after,
  .loop-steps > li:nth-child(2) .loop-step::after,
  .loop-exit::before {
    content: none;
  }
  .runtime-loop .loop-storage {
    flex-direction: column;
    gap: 4px;
  }
  .loop-storage > span:last-child {
    display: none;
  }
}
</style>
