import { Pressable, StyleSheet, Text, View } from "react-native";
import { QuickAction } from "../types";
import { theme } from "../theme";

type Props = {
  actions: QuickAction[];
};

export function QuickActionGrid({ actions }: Props) {
  return (
    <View style={styles.grid}>
      {actions.map((action) => (
        <Pressable key={action.id} style={styles.item}>
          <Text style={styles.label}>{action.label}</Text>
          <Text style={styles.detail}>{action.detail}</Text>
        </Pressable>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  grid: {
    flexDirection: "row",
    flexWrap: "wrap",
    justifyContent: "space-between",
    gap: theme.spacing.sm,
  },
  item: {
    width: "48%",
    backgroundColor: theme.colors.surface,
    borderColor: theme.colors.border,
    borderWidth: 1,
    borderRadius: theme.radius.md,
    padding: theme.spacing.md,
  },
  label: {
    color: theme.colors.text,
    fontSize: 15,
    fontWeight: "700",
    marginBottom: 4,
  },
  detail: {
    color: theme.colors.textMuted,
    fontSize: 13,
  },
});
