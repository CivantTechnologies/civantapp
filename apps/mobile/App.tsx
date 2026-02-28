import { StatusBar } from "expo-status-bar";
import { useEffect, useState } from "react";
import {
  ActivityIndicator,
  Pressable,
  SafeAreaView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from "react-native";
import { Session } from "@supabase/supabase-js";
import { CivantMobileHome } from "./src/CivantMobileHome";
import { criticalAlerts, opportunities, quickActions } from "./src/data/mockData";
import {
  postTriageAction,
  fetchTodayFeed,
  registerPushSubscription,
} from "./src/lib/civantApi";
import {
  ensureNotificationPermission,
  registerRemotePushSubscription,
  sendHighSignalLocalNotifications,
} from "./src/lib/notifications";
import {
  enqueueTriageAction,
  flushQueuedTriageActions,
  getQueuedTriageCount,
} from "./src/lib/offlineQueue";
import { supabase } from "./src/lib/supabase";
import { theme } from "./src/theme";
import { TodayFeedResponse } from "./src/types";

type TriageAction = "save" | "ignore" | "assign";

const emptyFeed: TodayFeedResponse = {
  tenantId: "",
  generatedAt: "",
  criticalAlerts,
  quickActions,
  opportunities,
};

function LoginScreen(props: {
  email: string;
  password: string;
  loading: boolean;
  error: string | null;
  onEmailChange: (value: string) => void;
  onPasswordChange: (value: string) => void;
  onSubmit: () => Promise<void>;
}) {
  const { email, password, loading, error, onEmailChange, onPasswordChange, onSubmit } = props;
  return (
    <View style={styles.loginContainer}>
      <Text style={styles.loginTitle}>Civant Mobile</Text>
      <Text style={styles.loginSubtitle}>
        Sign in with your Civant account to load tenant-scoped opportunities.
      </Text>

      <TextInput
        style={styles.input}
        placeholder="Email"
        value={email}
        autoCapitalize="none"
        keyboardType="email-address"
        onChangeText={onEmailChange}
      />
      <TextInput
        style={styles.input}
        placeholder="Password"
        value={password}
        secureTextEntry
        onChangeText={onPasswordChange}
      />
      {error ? <Text style={styles.errorText}>{error}</Text> : null}
      <Pressable style={styles.loginButton} disabled={loading} onPress={() => onSubmit()}>
        <Text style={styles.loginButtonText}>
          {loading ? "Signing in..." : "Sign in"}
        </Text>
      </Pressable>
    </View>
  );
}

export default function App() {
  const [session, setSession] = useState<Session | null>(null);
  const [isAuthLoading, setIsAuthLoading] = useState(true);
  const [isFeedLoading, setIsFeedLoading] = useState(false);
  const [isSigningIn, setIsSigningIn] = useState(false);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [errorText, setErrorText] = useState<string | null>(null);
  const [infoText, setInfoText] = useState<string | null>(null);
  const [feed, setFeed] = useState<TodayFeedResponse>(emptyFeed);
  const [notificationsEnabled, setNotificationsEnabled] = useState(true);
  const [queuedActionCount, setQueuedActionCount] = useState(0);
  const [remotePushSyncing, setRemotePushSyncing] = useState(false);
  const [actionState, setActionState] = useState<{
    predictionId: string;
    action: TriageAction;
  } | null>(null);

  async function refreshQueuedActionCount() {
    const count = await getQueuedTriageCount();
    setQueuedActionCount(count);
    return count;
  }

  function isRetryableNetworkError(error: unknown) {
    const message = String(error instanceof Error ? error.message : error || "").toLowerCase();
    if (!message) return false;

    if (message.includes("network request failed")) return true;
    if (message.includes("failed to fetch")) return true;
    if (message.includes("abort")) return true;
    if (message.includes("timed out")) return true;

    if (message.includes("triage_action_failed (5")) return true;
    if (message.includes("triage_action_failed (429")) return true;

    return false;
  }

  async function syncQueuedTriageActions(activeSession: Session) {
    const beforeCount = await refreshQueuedActionCount();
    if (beforeCount === 0) return;

    const result = await flushQueuedTriageActions({
      accessToken: activeSession.access_token,
      sendAction: postTriageAction,
      isRetryableError: isRetryableNetworkError,
      maxBatch: 20,
    });

    const afterCount = await refreshQueuedActionCount();
    if (result.sent > 0) {
      setInfoText(`Synced ${result.sent} queued action${result.sent > 1 ? "s" : ""}.`);
    } else if (result.dropped > 0) {
      setInfoText(`Dropped ${result.dropped} invalid queued action${result.dropped > 1 ? "s" : ""}.`);
    } else if (afterCount > 0 && result.stoppedOnRetryableError) {
      setInfoText(`${afterCount} action${afterCount > 1 ? "s" : ""} still queued (offline).`);
    }
  }

  async function syncRemotePushSubscription(activeSession: Session, enabled: boolean) {
    if (remotePushSyncing) return;
    setRemotePushSyncing(true);
    try {
      const result = await registerRemotePushSubscription({
        accessToken: activeSession.access_token,
        enabled,
        registerSubscription: registerPushSubscription,
      });

      if (result.reason === "permission_denied") {
        setInfoText("Push permission denied. Alerts will stay local only.");
      } else if (result.reason === "token_unavailable") {
        setInfoText("Push token unavailable on this build.");
      }
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Remote push registration failed";
      setInfoText(message);
    } finally {
      setRemotePushSyncing(false);
    }
  }

  useEffect(() => {
    let mounted = true;
    void refreshQueuedActionCount();
    supabase.auth
      .getSession()
      .then(({ data, error }) => {
        if (!mounted) return;
        if (error) {
          setErrorText(error.message);
        }
        setSession(data.session ?? null);
      })
      .finally(() => {
        if (mounted) setIsAuthLoading(false);
      });

    const { data } = supabase.auth.onAuthStateChange((_, currentSession) => {
      setSession(currentSession);
      setErrorText(null);
      if (!currentSession) {
        setFeed(emptyFeed);
      }
    });

    return () => {
      mounted = false;
      data.subscription.unsubscribe();
    };
  }, []);

  async function loadTodayFeed(activeSession: Session, options?: { silent?: boolean }) {
    const silent = options?.silent === true;
    if (!silent) {
      setIsFeedLoading(true);
    }
    setInfoText(null);
    try {
      const payload = await fetchTodayFeed(activeSession.access_token);
      setFeed(payload);
      if (notificationsEnabled) {
        const result = await sendHighSignalLocalNotifications(payload.opportunities);
        if (result.reason === "permission_denied") {
          setInfoText("Notification permission not granted on this device.");
        } else if (result.sent > 0) {
          setInfoText(`Sent ${result.sent} high-signal mobile alert${result.sent > 1 ? "s" : ""}.`);
        }
      }
      await syncQueuedTriageActions(activeSession);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to load live feed";
      setInfoText(`${message}.`);
      setFeed((current) =>
        current.generatedAt
          ? current
          : {
              ...emptyFeed,
              generatedAt: new Date().toISOString(),
            },
      );
    } finally {
      if (!silent) {
        setIsFeedLoading(false);
      }
    }
  }

  useEffect(() => {
    if (!session) return;
    void loadTodayFeed(session);
  }, [session]);

  useEffect(() => {
    if (!session) return;
    void syncRemotePushSubscription(session, notificationsEnabled);
  }, [session, notificationsEnabled]);

  async function handleSignIn() {
    const normalizedEmail = email.trim();
    if (!normalizedEmail || !password) {
      setErrorText("Enter your email and password.");
      return;
    }
    setIsSigningIn(true);
    setErrorText(null);
    try {
      const { error } = await supabase.auth.signInWithPassword({
        email: normalizedEmail,
        password,
      });
      if (error) {
        setErrorText(error.message);
      } else {
        setPassword("");
      }
    } finally {
      setIsSigningIn(false);
    }
  }

  async function handleSignOut() {
    const { error } = await supabase.auth.signOut();
    if (error) {
      setInfoText(error.message);
      return;
    }
    setInfoText(null);
  }

  async function handleTriageAction(predictionId: string, action: TriageAction) {
    if (!session) return;
    setActionState({ predictionId, action });
    setInfoText(null);
    try {
      await postTriageAction(session.access_token, { predictionId, action });
      setInfoText(`Action recorded: ${action}`);
      await loadTodayFeed(session, { silent: true });
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unable to record action";
      if (isRetryableNetworkError(error)) {
        const queuedCount = await enqueueTriageAction({ predictionId, action });
        setQueuedActionCount(queuedCount);
        setInfoText(`Offline. Action queued (${queuedCount} pending).`);
      } else {
        setInfoText(message);
      }
    } finally {
      setActionState(null);
    }
  }

  async function handleToggleNotifications() {
    if (!notificationsEnabled) {
      const allowed = await ensureNotificationPermission();
      if (!allowed) {
        setInfoText("Notifications stay off until permission is granted.");
        return;
      }
      setNotificationsEnabled(true);
      setInfoText("Mobile alerts enabled.");
      if (session) {
        await syncRemotePushSubscription(session, true);
        await loadTodayFeed(session, { silent: true });
      }
      return;
    }

    setNotificationsEnabled(false);
    setInfoText("Mobile alerts paused.");
    if (session) {
      await syncRemotePushSubscription(session, false);
    }
  }

  if (isAuthLoading) {
    return (
      <SafeAreaView style={styles.centeredScreen}>
        <StatusBar style="dark" />
        <ActivityIndicator />
      </SafeAreaView>
    );
  }

  if (!session) {
    return (
      <SafeAreaView style={{ flex: 1, backgroundColor: theme.colors.background }}>
        <StatusBar style="dark" />
        <LoginScreen
          email={email}
          password={password}
          loading={isSigningIn}
          error={errorText}
          onEmailChange={setEmail}
          onPasswordChange={setPassword}
          onSubmit={handleSignIn}
        />
      </SafeAreaView>
    );
  }

  const quickActionsWithQueue = queuedActionCount > 0
    ? [
        ...feed.quickActions,
        {
          id: "queued-sync",
          label: "Queued Sync",
          detail: `${queuedActionCount} pending`,
        },
      ]
    : feed.quickActions;

  return (
    <SafeAreaView style={{ flex: 1, backgroundColor: theme.colors.background }}>
      <StatusBar style="dark" />
      {isFeedLoading ? (
        <View style={styles.centeredScreen}>
          <ActivityIndicator />
          <Text style={styles.loadingText}>Loading your tenant feed...</Text>
        </View>
      ) : (
        <CivantMobileHome
          criticalAlerts={feed.criticalAlerts}
          quickActions={quickActionsWithQueue}
          opportunities={feed.opportunities}
          generatedAt={feed.generatedAt}
          infoText={infoText}
          onRefresh={() => loadTodayFeed(session)}
          onSignOut={handleSignOut}
          notificationsEnabled={notificationsEnabled}
          onToggleNotifications={handleToggleNotifications}
          onAction={handleTriageAction}
          actionState={actionState}
        />
      )}
    </SafeAreaView>
  );
}

const styles = StyleSheet.create({
  centeredScreen: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    gap: theme.spacing.sm,
    backgroundColor: theme.colors.background,
  },
  loadingText: {
    color: theme.colors.textMuted,
    fontSize: 14,
  },
  loginContainer: {
    flex: 1,
    justifyContent: "center",
    padding: theme.spacing.lg,
    gap: theme.spacing.sm,
  },
  loginTitle: {
    color: theme.colors.text,
    fontSize: 30,
    fontWeight: "800",
  },
  loginSubtitle: {
    color: theme.colors.textMuted,
    fontSize: 14,
    marginBottom: theme.spacing.sm,
  },
  input: {
    backgroundColor: theme.colors.surface,
    borderColor: theme.colors.border,
    borderWidth: 1,
    borderRadius: theme.radius.md,
    paddingHorizontal: theme.spacing.md,
    paddingVertical: 12,
    color: theme.colors.text,
  },
  loginButton: {
    marginTop: theme.spacing.xs,
    backgroundColor: theme.colors.primary,
    borderRadius: theme.radius.md,
    paddingVertical: 12,
    alignItems: "center",
  },
  loginButtonText: {
    color: theme.colors.surface,
    fontSize: 15,
    fontWeight: "700",
  },
  errorText: {
    color: theme.colors.danger,
    fontSize: 13,
  },
});
