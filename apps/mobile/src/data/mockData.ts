import { CriticalAlert, Opportunity, QuickAction } from "../types";

export const criticalAlerts: CriticalAlert[] = [
  {
    id: "alert-1",
    title: "Likely Renewal Window Open",
    description:
      "HSE framework for managed support appears to be entering the historical renewal cycle.",
  },
  {
    id: "alert-2",
    title: "Buyer Activity Spike",
    description:
      "Dublin City procurement notices are up 34% week-over-week in your tracked categories.",
  },
];

export const quickActions: QuickAction[] = [
  { id: "action-1", label: "Triage Inbox", detail: "12 items waiting" },
  { id: "action-2", label: "Follow Watchlist", detail: "7 high-signal updates" },
  { id: "action-3", label: "Prep Brief", detail: "3 opportunities due soon" },
  { id: "action-4", label: "Team Mentions", detail: "5 new notes" },
];

export const opportunities: Opportunity[] = [
  {
    id: "opp-1",
    buyer: "Health Service Executive",
    title: "Clinical Devices Lifecycle Program",
    dueDate: "Mar 18",
    valueBand: "EUR 2M - 5M",
    confidenceScore: 82,
    confidenceBand: "High",
    signalSummary: [
      "Prior award cycle cadence matches 2026 pattern",
      "Budget increase signal from buyer board release",
      "Recent related hiring for implementation staff",
    ],
    nextAction: "Assign capture owner today",
  },
  {
    id: "opp-2",
    buyer: "Department of Education",
    title: "Digital Learning Infrastructure Refresh",
    dueDate: "Apr 02",
    valueBand: "EUR 800K - 1.2M",
    confidenceScore: 67,
    confidenceBand: "Medium",
    signalSummary: [
      "Framework expiration expected in ~60 days",
      "Mixed vendor award history in past two cycles",
    ],
    nextAction: "Request competitor landscape snapshot",
  },
  {
    id: "opp-3",
    buyer: "Cork City Council",
    title: "Urban Mobility Data Platform",
    dueDate: "Apr 11",
    valueBand: "EUR 500K - 900K",
    confidenceScore: 49,
    confidenceBand: "Low",
    signalSummary: [
      "Policy signal present but no matching budget line yet",
      "Low recent purchase frequency in this CPV cluster",
    ],
    nextAction: "Monitor for funding confirmation",
  },
];
