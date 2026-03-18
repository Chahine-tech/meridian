const COLORS = [
  "#f87171", "#fb923c", "#fbbf24", "#a3e635",
  "#34d399", "#22d3ee", "#818cf8", "#e879f9",
];

export const colorForClient = (clientId: number): string =>
  COLORS[clientId % COLORS.length] ?? COLORS[0]!;
