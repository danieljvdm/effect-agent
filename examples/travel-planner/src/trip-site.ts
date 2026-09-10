import type { Trip } from "./domain.ts";

const escapeHtml = (value: string) =>
  value.replace(/[&<>"']/g, (character) => {
    switch (character) {
      case "&":
        return "&amp;";
      case "<":
        return "&lt;";
      case ">":
        return "&gt;";
      case '"':
        return "&quot;";
      default:
        return "&#39;";
    }
  });

/** Standalone, script-free publication; all trip content is rendered as text. */
export const renderTripSite = (trip: Trip): string => `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="referrer" content="no-referrer"><title>${escapeHtml(trip.title)} — Travel journal</title>
<style>
:root{color-scheme:light;font-family:Georgia,serif;color:#25382f;background:#f5f2e9}*{box-sizing:border-box}body{margin:0}main{max-width:880px;margin:auto;padding:72px 24px}header{border-bottom:1px solid #c8cdbc;padding-bottom:36px;margin-bottom:40px}.eyebrow,small,footer{font:12px/1.6 system-ui,sans-serif;letter-spacing:.12em;text-transform:uppercase;color:#657666}h1{font-size:clamp(40px,8vw,76px);line-height:1.05;letter-spacing:-.04em;margin:20px 0}h2{font-size:28px;font-weight:normal}p,li{font-size:18px;line-height:1.75;overflow-wrap:anywhere}.summary{max-width:680px}.details{font:14px/1.8 system-ui,sans-serif;color:#657666}.day{padding:8px 0 24px;border-bottom:1px solid #d8dbce;margin-bottom:28px}li{padding-left:8px;margin-bottom:8px}aside{background:#e9eddf;padding:18px 28px;margin-top:36px}footer{margin-top:48px;letter-spacing:.04em}@media print{main{padding:20px}.day{break-inside:avoid}}
</style></head><body><main><header><div class="eyebrow">A journey to ${escapeHtml(trip.destination)}</div>
<h1>${escapeHtml(trip.title)}</h1><p class="summary">${escapeHtml(trip.summary)}</p>
<div class="details">${trip.travelers} traveler${trip.travelers === 1 ? "" : "s"}${trip.startDate ? ` · ${escapeHtml(trip.startDate)}` : ""}${trip.endDate ? ` — ${escapeHtml(trip.endDate)}` : ""}</div></header>
${trip.days.map((day, index) => `<section class="day"><small>Day ${index + 1}</small><h2>${escapeHtml(day.title)}</h2><ul>${day.activities.map((activity) => `<li>${escapeHtml(activity)}</li>`).join("")}</ul></section>`).join("")}
${trip.notes.length ? `<aside><h2>Before you go</h2><ul>${trip.notes.map((note) => `<li>${escapeHtml(note)}</li>`).join("")}</ul></aside>` : ""}
<footer>Travel journal · Published revision ${trip.revision}</footer></main></body></html>`;
