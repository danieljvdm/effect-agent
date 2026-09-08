import { createHash } from "node:crypto";
import { cpus, totalmem } from "node:os";

/** Synchronous Node-only measurement boundary; no I/O runs during import. */
export const environment = () => ({
  runtime: process.version,
  platform: process.platform,
  arch: process.arch,
  cpu: cpus()[0]?.model ?? "unknown",
  logicalCpus: cpus().length,
  ramBytes: totalmem(),
});

export const nowMillis = () => performance.now();
export const memoryUsage = () => process.memoryUsage();
export const collectGarbage = () => global.gc?.();
export const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");
