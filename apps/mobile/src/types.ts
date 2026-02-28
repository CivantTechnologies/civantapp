export type ConfidenceBand = "Very High" | "High" | "Medium" | "Low";

export type Opportunity = {
  id: string;
  buyer: string;
  title: string;
  dueDate: string;
  valueBand: string;
  confidenceScore: number;
  confidenceBand: ConfidenceBand;
  signalSummary: string[];
  nextAction: string;
  scorecard?: {
    totalScore: number;
    dataQualityScore: number;
  };
};

export type CriticalAlert = {
  id: string;
  title: string;
  description: string;
};

export type QuickAction = {
  id: string;
  label: string;
  detail: string;
};

export type TodayFeedResponse = {
  tenantId: string;
  generatedAt: string;
  criticalAlerts: CriticalAlert[];
  quickActions: QuickAction[];
  opportunities: Opportunity[];
};
