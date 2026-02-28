import { getPublicEnv } from "../config/env";
import { TodayFeedResponse } from "../types";

const REQUEST_TIMEOUT_MS = 15000;

function getApiUrl(path: string) {
  const { apiBaseUrl } = getPublicEnv();
  return `${apiBaseUrl.replace(/\/$/, "")}${path}`;
}

async function readErrorMessage(response: Response) {
  try {
    const payload = (await response.json()) as { error?: string };
    if (payload?.error) return payload.error;
  } catch {
    // fall through to status text
  }
  return response.statusText || "request_failed";
}

async function fetchWithTimeout(
  url: string,
  init: RequestInit,
  timeoutMs = REQUEST_TIMEOUT_MS,
) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

// Tenant must be derived server-side from auth context. Do not send tenant_id from mobile clients.
export async function fetchTodayFeed(accessToken: string): Promise<TodayFeedResponse> {
  const response = await fetchWithTimeout(getApiUrl("/api/mobile/today"), {
    method: "GET",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      "X-Client-Platform": "ios-mobile",
    },
  });

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`today_feed_failed (${response.status}): ${message}`);
  }

  return (await response.json()) as TodayFeedResponse;
}

export async function postTriageAction(
  accessToken: string,
  payload: { predictionId: string; action: "save" | "ignore" | "assign" },
): Promise<void> {
  const response = await fetchWithTimeout(getApiUrl("/api/mobile/triage"), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Client-Platform": "ios-mobile",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`triage_action_failed (${response.status}): ${message}`);
  }
}

export async function registerPushSubscription(
  accessToken: string,
  payload: {
    deviceId: string;
    platform: "ios" | "android";
    expoPushToken?: string | null;
    appVersion?: string | null;
    enabled: boolean;
  },
): Promise<void> {
  const response = await fetchWithTimeout(getApiUrl("/api/mobile/push/register"), {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Client-Platform": "ios-mobile",
    },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(`push_register_failed (${response.status}): ${message}`);
  }
}
