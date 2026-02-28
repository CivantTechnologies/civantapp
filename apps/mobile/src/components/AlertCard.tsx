import { StyleSheet, Text, View } from "react-native";
import { CriticalAlert } from "../types";
import { theme } from "../theme";

type Props = {
  alert: CriticalAlert;
};

export function AlertCard({ alert }: Props) {
  return (
    <View style={styles.card}>
      <Text style={styles.title}>{alert.title}</Text>
      <Text style={styles.description}>{alert.description}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    backgroundColor: theme.colors.warningSoft,
    borderRadius: theme.radius.md,
    borderColor: "#F2D4A5",
    borderWidth: 1,
    padding: theme.spacing.md,
    gap: 6,
  },
  title: {
    color: theme.colors.warningText,
    fontSize: 14,
    fontWeight: "700",
  },
  description: {
    color: theme.colors.text,
    fontSize: 13,
    lineHeight: 18,
  },
});
