import { Exit, Queue } from "effect";
import { runtime } from "../runtime";
import { compactSpec, foldCompactEvents } from "./compact";
import type { InterruptReason } from "./errors";
import { getProvider } from "./registry";
import { hasRun, startRun, stopAllRuns, stopRun } from "./run-registry";
import { setSession } from "./session-store";
import type {
  AgentEvent,
  CompactEvent,
  ProviderId,
  ProviderSpec,
  RunOptions,
} from "./types";

/** Start a run and drain its queue as normalized events. Stays an AsyncGenerator. */
async function* streamRun(
  spec: ProviderSpec,
  opts: RunOptions
): AsyncGenerator<AgentEvent> {
  const queue = await runtime.runPromise(startRun(spec, opts));
  try {
    while (true) {
      const exit = await runtime.runPromiseExit(Queue.take(queue));
      if (Exit.isFailure(exit)) {
        return; // Cause.Done (end) or interrupt of the take => stream over
      }
      yield exit.value;
    }
  } finally {
    // Consumer abandoned early (e.g. telegram broke on plan_ready) => tear the
    // producer down. No-op if it already ended.
    runtime.runFork(stopRun(opts.userId, "stopped"));
  }
}

/** Run a provider agent, yielding normalized events. Stays an AsyncGenerator. */
export async function* runAgent(
  providerId: ProviderId,
  opts: RunOptions
): AsyncGenerator<AgentEvent> {
  for await (const event of streamRun(getProvider(providerId), opts)) {
    // Persist the session id as soon as it exists — on session_init AND result,
    // not result-only — so an interrupted run's session is still resumable.
    if (event.kind === "session_init" || event.kind === "result") {
      runtime.runFork(
        setSession({
          project: opts.projectDir,
          provider: providerId,
          sessionId: event.sessionId,
        })
      );
    }
    yield event;
  }
}

/**
 * Compact `opts.sessionId` in place for a provider, resolving to the run's one
 * terminal event. It goes through the same registry as a prompt — single-flight
 * per user, /stop-able, capacity-bounded — but its stream is folded into an
 * outcome instead of streamed to chat. The session store is never touched, so
 * the session id is unchanged whatever the outcome.
 */
export async function compactAgent(
  providerId: ProviderId,
  opts: RunOptions
): Promise<CompactEvent> {
  const provider = getProvider(providerId);
  if (!provider.compact) {
    return {
      kind: "compact_failed",
      reason: `${provider.displayName} does not support compaction.`,
    };
  }
  return await foldCompactEvents(
    streamRun(compactSpec(provider.id, provider.compact), opts)
  );
}

/** Stop the active run for a user; returns whether one was running. */
export const stopAgent = (
  userId: number,
  reason: InterruptReason = "stopped"
) => runtime.runSync(stopRun(userId, reason));

/** Whether a user has an active run. */
export const hasActiveProcess = (userId: number) =>
  runtime.runSync(hasRun(userId));

/** Interrupt all runs and await settle (shutdown). */
export const stopAll = () => runtime.runPromise(stopAllRuns);

/** List all stored sessions for a provider */
export function listAllSessions(providerId: ProviderId) {
  return getProvider(providerId).listAllSessions();
}

/** Look up a session's project path for a provider */
export function getSessionProject(providerId: ProviderId, sessionId: string) {
  return getProvider(providerId).getSessionProject(sessionId);
}

/** Clear a provider's session-to-project cache */
export function clearSessionCache(providerId: ProviderId) {
  getProvider(providerId).clearSessionCache();
}

/** Get a provider's capabilities */
export function getCapabilities(providerId: ProviderId) {
  return getProvider(providerId).capabilities;
}

/** Whether a provider can compact a session in place (gates /compact) */
export function supportsCompaction(providerId: ProviderId) {
  return getProvider(providerId).capabilities.compaction;
}

/** Get a provider's selectable models */
export function getModels(providerId: ProviderId) {
  return getProvider(providerId).models;
}

/** Get a provider's selectable reasoning-effort levels */
export function getEffortLevels(providerId: ProviderId) {
  return getProvider(providerId).effortLevels;
}

/** Get a provider's default reasoning-effort id (used when the user has not chosen) */
export function getDefaultEffort(providerId: ProviderId) {
  return getProvider(providerId).defaultEffort;
}
