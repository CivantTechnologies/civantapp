import AsyncStorage from "@react-native-async-storage/async-storage";

const TRIAGE_QUEUE_KEY = "civant_mobile_triage_queue_v1";

export type QueuedTriageAction = {
  id: string;
  predictionId: string;
  action: "save" | "ignore" | "assign";
  queuedAt: string;
  attempts: number;
  lastError: string | null;
};

function nowIso() {
  return new Date().toISOString();
}

function makeQueueId() {
  return `queued_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
}

function toErrorMessage(error: unknown) {
  return error instanceof Error ? error.message : String(error || "unknown_error");
}

async function readQueue() {
  const raw = await AsyncStorage.getItem(TRIAGE_QUEUE_KEY);
  if (!raw) return [] as QueuedTriageAction[];

  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [] as QueuedTriageAction[];
    return parsed.filter((item) => item && typeof item === "object") as QueuedTriageAction[];
  } catch {
    return [] as QueuedTriageAction[];
  }
}

async function writeQueue(queue: QueuedTriageAction[]) {
  await AsyncStorage.setItem(TRIAGE_QUEUE_KEY, JSON.stringify(queue));
}

export async function getQueuedTriageCount() {
  const queue = await readQueue();
  return queue.length;
}

export async function enqueueTriageAction(input: {
  predictionId: string;
  action: "save" | "ignore" | "assign";
}) {
  const queue = await readQueue();
  queue.push({
    id: makeQueueId(),
    predictionId: input.predictionId,
    action: input.action,
    queuedAt: nowIso(),
    attempts: 0,
    lastError: null,
  });
  await writeQueue(queue);
  return queue.length;
}

export async function flushQueuedTriageActions(params: {
  accessToken: string;
  maxBatch?: number;
  isRetryableError: (error: unknown) => boolean;
  sendAction: (
    accessToken: string,
    payload: { predictionId: string; action: "save" | "ignore" | "assign" },
  ) => Promise<void>;
}) {
  const queue = await readQueue();
  const maxBatch = Math.max(1, Math.min(Number(params.maxBatch || 20), 100));
  const batch = queue.slice(0, maxBatch);
  const tail = queue.slice(maxBatch);
  const remaining: QueuedTriageAction[] = [];

  let sent = 0;
  let dropped = 0;

  for (let idx = 0; idx < batch.length; idx += 1) {
    const item = batch[idx];
    try {
      await params.sendAction(params.accessToken, {
        predictionId: item.predictionId,
        action: item.action,
      });
      sent += 1;
    } catch (error) {
      const errorMessage = toErrorMessage(error);

      if (params.isRetryableError(error)) {
        remaining.push({
          ...item,
          attempts: item.attempts + 1,
          lastError: errorMessage,
        });
        remaining.push(...batch.slice(idx + 1));
        remaining.push(...tail);
        await writeQueue(remaining);
        return {
          sent,
          dropped,
          remaining: remaining.length,
          stoppedOnRetryableError: true,
        };
      }

      // Non-retryable item is discarded to avoid blocking the queue forever.
      dropped += 1;
    }
  }

  remaining.push(...tail);
  await writeQueue(remaining);

  return {
    sent,
    dropped,
    remaining: remaining.length,
    stoppedOnRetryableError: false,
  };
}
