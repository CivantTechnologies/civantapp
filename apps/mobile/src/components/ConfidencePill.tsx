import { StyleSheet, Text, View } from "react-native";
import { ConfidenceBand } from "../types";
import { theme } from "../theme";

const bandColorMap: Record<ConfidenceBand, string> = {
  "Very High": theme.colors.success,
  High: theme.colors.success,
  Medium: theme.colors.caution,
  Low: theme.colors.danger,
};

type Props = {
  band: ConfidenceBand;
  score: number;
};

export function ConfidencePill({ band, score }: Props) {
  return (
    <View style={styles.container}>
      <View style={[styles.dot, { backgroundColor: bandColorMap[band] }]} />
      <Text style={styles.text}>
        {band} {score}%
      </Text>
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flexDirection: "row",
    alignItems: "center",
    alignSelf: "flex-start",
    paddingHorizontal: theme.spacing.sm,
    paddingVertical: theme.spacing.xs,
    backgroundColor: theme.colors.primarySoft,
    borderRadius: 999,
  },
  dot: {
    width: 8,
    height: 8,
    borderRadius: 99,
    marginRight: theme.spacing.xs,
  },
  text: {
    color: theme.colors.text,
    fontWeight: "700",
    fontSize: 12,
  },
});
