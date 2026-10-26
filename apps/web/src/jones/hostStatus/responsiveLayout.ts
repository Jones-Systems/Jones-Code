import { HOST_STATUS_IDS, HOST_STATUS_NAMES } from "../../hostStatus";

export type HostPairLayout = "inline" | "diagonal" | "vertical";
export interface HostDisplayStage {
  names: string[];
  swapUnits: boolean[];
  percentUnits: boolean[];
  layouts: HostPairLayout[];
}

function displayStages() {
  const stages: HostDisplayStage[] = [];
  let stage: HostDisplayStage = {
    names: HOST_STATUS_IDS.map((id) => HOST_STATUS_NAMES[id]),
    swapUnits: HOST_STATUS_IDS.map(() => true),
    percentUnits: HOST_STATUS_IDS.map(() => true),
    layouts: HOST_STATUS_IDS.map(() => "inline"),
  };
  const change = (patch: Partial<HostDisplayStage>) => {
    stage = { ...stage, ...patch };
    stages.push(stage);
  };
  change({});
  for (const index of [3, 2]) {
    change({ swapUnits: stage.swapUnits.map((value, i) => (i === index ? false : value)) });
  }
  for (const index of [3, 2, 1, 0]) {
    change({ percentUnits: stage.percentUnits.map((value, i) => (i === index ? false : value)) });
  }
  for (const [index, name] of [
    [3, "Lap"],
    [1, "T"],
    [2, "M"],
    [0, "V"],
    [3, "L"],
  ] as const) {
    change({ names: stage.names.map((value, i) => (i === index ? name : value)) });
  }
  for (const layout of ["diagonal", "vertical"] as const) {
    for (const index of [3, 2, 1, 0]) {
      change({ layouts: stage.layouts.map((value, i) => (i === index ? layout : value)) });
    }
  }
  for (const index of [3, 2, 1, 0]) {
    change({ names: stage.names.map((value, i) => (i === index ? "" : value)) });
  }
  return stages;
}

export const HOST_DISPLAY_STAGES = displayStages();

export function hostDisplayValues(
  cpu: string,
  ram: string,
  stage: HostDisplayStage,
  index: number,
) {
  return {
    cpu: stage.percentUnits[index] ? cpu : cpu.replace(/%$/, ""),
    ram: stage.swapUnits[index] ? ram : ram.replace(/ mb\/s$/, ""),
  };
}

export interface HostLayoutMeasurement {
  name: (name: string, inline: boolean) => number;
  cpu: (withUnit: boolean) => number;
  ram: (withUnit: boolean) => number;
  dot: number;
}

export interface HostLayoutFit {
  stage: number;
  padding: number;
  gap: number;
  scale: number;
}

function textWidth(stage: HostDisplayStage, measurements: HostLayoutMeasurement[]) {
  return measurements.reduce((total, host, index) => {
    const layout = stage.layouts[index];
    const cpu = host.cpu(stage.percentUnits[index]!);
    const ram = host.ram(stage.swapUnits[index]!);
    const name = stage.names[index]!;
    const label = name ? host.name(name, layout === "inline") : 0;
    const pair = layout === "inline" ? cpu + host.dot + ram : Math.max(cpu, ram);
    return (
      total +
      label +
      pair +
      (layout === "diagonal" ? 10 : 0) +
      (name && layout !== "inline" ? 3 : 0)
    );
  }, 0);
}

/** Compress only the space needed; never distribute spare width between the readings. */
export function fitHostStatus(
  width: number,
  measurements: HostLayoutMeasurement[],
): HostLayoutFit | null {
  if (measurements.length !== HOST_STATUS_IDS.length || !Number.isFinite(width)) return null;
  const borders = 8;
  const minimumSpacing = borders + 4 * 6 + 3 * 2;
  const fullSpacing = borders + 4 * 16 + 3 * 8;
  for (const [stage, candidate] of HOST_DISPLAY_STAGES.entries()) {
    const text = textWidth(candidate, measurements);
    if (text + minimumSpacing > width) continue;
    const density = Math.min(1, (width - text - minimumSpacing) / (fullSpacing - minimumSpacing));
    return { stage, padding: 3 + density * 5, gap: 2 + density * 6, scale: 1 };
  }
  const stage = HOST_DISPLAY_STAGES.length - 1;
  const text = textWidth(HOST_DISPLAY_STAGES[stage]!, measurements);
  const scale = Math.min(1, (width - minimumSpacing) / text);
  return scale >= 10 / 12 ? { stage, padding: 3, gap: 2, scale } : null;
}
