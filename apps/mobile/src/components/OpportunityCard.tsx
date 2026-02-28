import { Pressable, StyleSheet, Text, View } from "react-native";
import { Opportunity } from "../types";
import { theme } from "../theme";
import { ConfidencePill } from "./ConfidencePill";

type Props = {
  opportunity: Opportunity;
  onAction: (
    predictionId: string,
    action: "save" | "ignore" | "assign",
  ) => void | Promise<void>;
  actionInFlight: "save" | "ignore" | "assign" | null;
};

export function OpportunityCard({ opportunity, onAction, actionInFlight }: Props) {
  const isBusy = actionInFlight !== null;
  return (
    <View style={styles.card}>
      <View style={styles.headerRow}>
        <Text style={styles.buyer}>{opportunity.buyer}</Text>
        <ConfidencePill
          band={opportunity.confidenceBand}
          score={opportunity.confidenceScore}
        />
      </View>

      <Text style={styles.title}>{opportunity.title}</Text>
      <Text style={styles.meta}>
        Due {opportunity.dueDate} • {opportunity.valueBand}
      </Text>
      {opportunity.scorecard ? (
        <Text style={styles.scorecardMeta}>
          Score {opportunity.scorecard.totalScore}/100 • Data quality{" "}
          {opportunity.scorecard.dataQualityScore}/20
        </Text>
      ) : null}

      <View style={styles.signalList}>
        {opportunity.signalSummary.map((signal) => (
          <Text key={signal} style={styles.signal}>
            • {signal}
          </Text>
        ))}
      </View>

      <View style={styles.actionRow}>
        <Pressable
          style={styles.secondaryButton}
          onPress={() => onAction(opportunity.id, "save")}
          disabled={isBusy}
        >
          <Text style={styles.secondaryButtonText}>Save</Text>
        </Pressable>
        <Pressable
          style={styles.secondaryButton}
          onPress={() => onAction(opportunity.id, "ignore")}
          disabled={isBusy}
        >
          <Text style={styles.secondaryButtonText}>Ignore</Text>
        </Pressable>
        <Pressable
          style={styles.primaryButton}
          onPress={() => onAction(opportunity.id, "assign")}
          disabled={isBusy}
        >
          <Text style={styles.primaryButtonText}>
            {actionInFlight === "assign" ? "Assigning..." : "Assign"}
          </Text>
        </Pressable>
      </View>

      <Text style={styles.nextAction}>Next: {opportunity.nextAction}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: theme.colors.surface,
    borderRadius: theme.radius.lg,
    padding: theme.spacing.md,
    borderColor: theme.colors.border,
    borderWidth: 1,
    gap: theme.spacing.sm,
  },
  headerRow: {
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center",
    gap: theme.spacing.sm,
  },
  buyer: {
    flex: 1,
    color: theme.colors.textMuted,
    fontSize: 12,
    fontWeight: "600",
    textTransform: "uppercase",
    letterSpacing: 0.8,
  },
  title: {
    color: theme.colors.text,
    fontSize: 17,
    lineHeight: 22,
    fontWeight: "700",
  },
  meta: {
    color: theme.colors.textMuted,
    fontSize: 13,
    fontWeight: "500",
  },
  scorecardMeta: {
    color: theme.colors.textMuted,
    fontSize: 12,
    fontWeight: "500",
  },
  signalList: {
    gap: 4,
  },
  signal: {
    color: theme.colors.text,
    fontSize: 13,
    lineHeight: 18,
  },
  actionRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: theme.spacing.xs,
  },
  secondaryButton: {
    paddingVertical: 8,
    paddingHorizontal: 12,
    backgroundColor: theme.colors.primarySoft,
    borderRadius: theme.radius.sm,
  },
  secondaryButtonText: {
    color: theme.colors.primary,
    fontSize: 13,
    fontWeight: "600",
  },
  primaryButton: {
    marginLeft: "auto",
    paddingVertical: 8,
    paddingHorizontal: 14,
    backgroundColor: theme.colors.primary,
    borderRadius: theme.radius.sm,
  },
  primaryButtonText: {
    color: theme.colors.surface,
    fontSize: 13,
    fontWeight: "700",
  },
  nextAction: {
    color: theme.colors.warningText,
    backgroundColor: theme.colors.warningSoft,
    paddingVertical: 8,
    paddingHorizontal: 10,
    borderRadius: theme.radius.sm,
    fontSize: 12,
    fontWeight: "600",
  },
});
