import { ScrollView, StyleSheet, Text, View } from "react-native";
import { AlertCard } from "./components/AlertCard";
import { OpportunityCard } from "./components/OpportunityCard";
import { QuickActionGrid } from "./components/QuickActionGrid";
import { theme } from "./theme";
import { CriticalAlert, Opportunity, QuickAction } from "./types";

type Props = {
  criticalAlerts: CriticalAlert[];
  quickActions: QuickAction[];
  opportunities: Opportunity[];
  generatedAt: string;
  infoText: string | null;
  onRefresh: () => void | Promise<void>;
  onSignOut: () => void | Promise<void>;
  notificationsEnabled: boolean;
  onToggleNotifications: () => void | Promise<void>;
  onAction: (
    predictionId: string,
    action: "save" | "ignore" | "assign",
  ) => void | Promise<void>;
  actionState: {
    predictionId: string;
    action: "save" | "ignore" | "assign";
  } | null;
};

function formatGeneratedAt(value: string) {
  if (!value) return "";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleString();
}

export function CivantMobileHome({
  criticalAlerts,
  quickActions,
  opportunities,
  generatedAt,
  infoText,
  onRefresh,
  onSignOut,
  notificationsEnabled,
  onToggleNotifications,
  onAction,
  actionState,
}: Props) {
  return (
    <ScrollView contentContainerStyle={styles.container}>
      <View style={styles.header}>
        <Text style={styles.product}>Civant</Text>
        <Text style={styles.headline}>Today</Text>
        <Text style={styles.subhead}>
          Mobile pipeline triage focused on high-confidence opportunities.
        </Text>
        {infoText ? <Text style={styles.infoText}>{infoText}</Text> : null}
        {generatedAt ? (
          <Text style={styles.lastSync}>Synced {formatGeneratedAt(generatedAt)}</Text>
        ) : null}
        <View style={styles.headerActions}>
          <Text style={styles.headerActionText} onPress={() => onRefresh()}>
            Refresh
          </Text>
          <Text style={styles.headerActionText} onPress={() => onToggleNotifications()}>
            {notificationsEnabled ? "Alerts On" : "Alerts Off"}
          </Text>
          <Text style={styles.headerActionText} onPress={() => onSignOut()}>
            Sign out
          </Text>
        </View>
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Critical Alerts</Text>
        {criticalAlerts.length > 0 ? (
          criticalAlerts.map((alert) => <AlertCard key={alert.id} alert={alert} />)
        ) : (
          <Text style={styles.emptyText}>No urgent alerts right now.</Text>
        )}
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Quick Actions</Text>
        <QuickActionGrid actions={quickActions} />
      </View>

      <View style={styles.section}>
        <Text style={styles.sectionTitle}>Opportunities</Text>
        {opportunities.length > 0 ? (
          opportunities.map((opportunity) => (
            <OpportunityCard
              key={opportunity.id}
              opportunity={opportunity}
              onAction={onAction}
              actionInFlight={
                actionState?.predictionId === opportunity.id ? actionState.action : null
              }
            />
          ))
        ) : (
          <Text style={styles.emptyText}>No opportunities found for your tenant yet.</Text>
        )}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: {
    padding: theme.spacing.md,
    paddingBottom: 48,
    gap: theme.spacing.lg,
  },
  header: {
    backgroundColor: theme.colors.surface,
    borderWidth: 1,
    borderColor: theme.colors.border,
    borderRadius: theme.radius.lg,
    padding: theme.spacing.md,
    gap: 4,
  },
  product: {
    color: theme.colors.primary,
    fontSize: 13,
    letterSpacing: 0.8,
    textTransform: "uppercase",
    fontWeight: "700",
  },
  headline: {
    color: theme.colors.text,
    fontSize: 28,
    fontWeight: "800",
  },
  subhead: {
    color: theme.colors.textMuted,
    fontSize: 14,
    lineHeight: 19,
  },
  infoText: {
    color: theme.colors.warningText,
    fontSize: 12,
    fontWeight: "600",
  },
  lastSync: {
    color: theme.colors.textMuted,
    fontSize: 12,
  },
  headerActions: {
    flexDirection: "row",
    gap: 14,
    paddingTop: 2,
  },
  headerActionText: {
    color: theme.colors.primary,
    fontSize: 13,
    fontWeight: "700",
  },
  section: {
    gap: theme.spacing.sm,
  },
  sectionTitle: {
    color: theme.colors.text,
    fontSize: 19,
    fontWeight: "700",
  },
  emptyText: {
    color: theme.colors.textMuted,
    fontSize: 14,
  },
});
