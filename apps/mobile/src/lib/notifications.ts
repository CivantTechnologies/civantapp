import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Notifications from "expo-notifications";
import Constants from "expo-constants";
import { Platform } from "react-native";
import { Opportunity } from "../types";

const DAILY_DEDUP_PREFIX = "civant_mobile_notified";
const DEVICE_ID_KEY = "civant_mobile_device_id_v1";

Notifications.setNotificationHandler({
  handleNotification: async () => ({
    shouldShowAlert: true,
    shouldPlaySound: true,
    shouldSetBadge: false,
    shouldShowBanner: true,
    shouldShowList: true,
  }),
});

function todayStamp() {
  return new Date().toISOString().slice(0, 10);
}

function daysUntil(dateIso: string) {
  const raw = String(dateIso || "").trim();
  if (!raw) return null;
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  const diffMs = parsed.getTime() - Date.now();
  return Math.floor(diffMs / (24 * 60 * 60 * 1000));
}

function shouldNotify(opportunity: Opportunity) {
  const highSignal =
    opportunity.confidenceBand === "Very High" || opportunity.confidenceBand === "High";
  const untilDue = daysUntil(opportunity.dueDate);
  const dueSoon = untilDue !== null && untilDue <= 14;
  return highSignal || dueSoon;
}

function buildDedupKey(opportunityId: string) {
  return `${DAILY_DEDUP_PREFIX}:${todayStamp()}:${opportunityId}`;
}

function getPlatform() {
  return Platform.OS === "android" ? "android" : "ios";
}

async function getOrCreateDeviceId() {
  const existing = await AsyncStorage.getItem(DEVICE_ID_KEY);
  if (existing) return existing;

  const generated = `device_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  await AsyncStorage.setItem(DEVICE_ID_KEY, generated);
  return generated;
}

async function getExpoPushToken() {
  const projectId =
    Constants.expoConfig?.extra?.eas?.projectId ?? Constants.easConfig?.projectId;

  const response = projectId
    ? await Notifications.getExpoPushTokenAsync({ projectId })
    : await Notifications.getExpoPushTokenAsync();

  return String(response.data || "").trim();
}

export async function ensureNotificationPermission() {
  const existing = await Notifications.getPermissionsAsync();
  if (existing.status === "granted") return true;

  const requested = await Notifications.requestPermissionsAsync();
  return requested.status === "granted";
}

export async function sendHighSignalLocalNotifications(opportunities: Opportunity[]) {
  const canNotify = await ensureNotificationPermission();
  if (!canNotify) return { sent: 0, skipped: opportunities.length, reason: "permission_denied" };

  const candidates = opportunities.filter(shouldNotify).slice(0, 3);
  let sent = 0;
  let skipped = 0;

  for (const opportunity of candidates) {
    const dedupKey = buildDedupKey(opportunity.id);
    const alreadySent = await AsyncStorage.getItem(dedupKey);
    if (alreadySent) {
      skipped += 1;
      continue;
    }

    await Notifications.scheduleNotificationAsync({
      content: {
        title: "Civant mobile alert",
        body: `${opportunity.buyer}: ${opportunity.title}`,
        data: {
          predictionId: opportunity.id,
          confidenceBand: opportunity.confidenceBand,
        },
      },
      trigger: null,
    });

    await AsyncStorage.setItem(dedupKey, new Date().toISOString());
    sent += 1;
  }

  return { sent, skipped, reason: "ok" as const };
}

export async function registerRemotePushSubscription(params: {
  accessToken: string;
  enabled: boolean;
  registerSubscription: (
    accessToken: string,
    payload: {
      deviceId: string;
      platform: "ios" | "android";
      expoPushToken?: string | null;
      appVersion?: string | null;
      enabled: boolean;
    },
  ) => Promise<void>;
}) {
  const deviceId = await getOrCreateDeviceId();
  const platform = getPlatform();
  const appVersion = String(Constants.expoConfig?.version || "").trim() || null;

  if (!params.enabled) {
    await params.registerSubscription(params.accessToken, {
      deviceId,
      platform,
      enabled: false,
      appVersion,
    });
    return { success: true, enabled: false, reason: "disabled" as const };
  }

  const canNotify = await ensureNotificationPermission();
  if (!canNotify) {
    return { success: false, enabled: false, reason: "permission_denied" as const };
  }

  const expoPushToken = await getExpoPushToken();
  if (!expoPushToken) {
    return { success: false, enabled: true, reason: "token_unavailable" as const };
  }

  await params.registerSubscription(params.accessToken, {
    deviceId,
    platform,
    expoPushToken,
    enabled: true,
    appVersion,
  });

  return { success: true, enabled: true, reason: "registered" as const };
}
