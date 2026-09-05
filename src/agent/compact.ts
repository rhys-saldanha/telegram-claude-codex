import { classifyOutcome } from "./errors";
import type {
  AgentEvent,
  AgentProvider,
  CompactEvent,
  ProviderId,
  ProviderSpec,
} from "./types";

/**
 * A provider's compaction, dressed as a run spec. Compaction has the same shape
 * as an `sdk` run — one AbortSignal, one event stream — so the generic runner,
 * the per-user single-flight registry and the concurrency cap all apply to it
 * unchanged; only the folding of the stream differs (see foldCompactEvents).
 */
export const compactSpec = (
  id: ProviderId,
  compact: NonNullable<AgentProvider["compact"]>
): ProviderSpec => ({ id, kind: "sdk", run: compact });

/**
 * Fold a compaction run's events into its terminal event. A run that ends
 * without one — interrupted, at capacity, provider crash — becomes a failure
 * carrying the run's own copy, so every path has words for the user. A terminal
 * event already seen wins over a later error: compaction that ran, ran.
 */
export const foldCompactEvents = async (
  events: AsyncIterable<AgentEvent>
): Promise<CompactEvent> => {
  let outcome: CompactEvent | undefined;
  let failure = "";
  for await (const event of events) {
    if (event.kind === "compact_done" || event.kind === "compact_failed") {
      outcome ??= event;
    } else if (event.kind === "error") {
      failure = event.class ? classifyOutcome(event.class).copy : event.message;
    }
  }
  return (
    outcome ?? {
      kind: "compact_failed",
      reason: failure || "Compaction ended without a result.",
    }
  );
};
