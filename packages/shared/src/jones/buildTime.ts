// @effect-diagnostics globalDate:off - Every date is derived from a validated build stamp; no ambient clock is read.
import { readJonesBuildStartedAt } from "@t3tools/contracts/jones/fleet-updates";

export const JONES_BUILD_TIME_ZONE = "America/New_York";
export const parseJonesBuildStartedAt = (value: unknown): string | undefined =>
  readJonesBuildStartedAt({ jonesBuildStartedAt: value });
export type JonesBuildLabelForm = "month-day" | "day";
export interface JonesBuildRef {
  version: string;
  startedAt?: string | undefined;
  sourceSha?: string | undefined;
}
export interface JonesBuildLabel {
  text: string;
  timed: boolean;
  spoken: string;
  detail: string;
}

const numeric = new Intl.DateTimeFormat("en-US", {
  timeZone: JONES_BUILD_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hourCycle: "h23",
});
const calendar = new Intl.DateTimeFormat("en-US", {
  timeZone: JONES_BUILD_TIME_ZONE,
  weekday: "short",
  month: "short",
  day: "numeric",
  year: "numeric",
});
const spokenDate = new Intl.DateTimeFormat("en-US", {
  timeZone: JONES_BUILD_TIME_ZONE,
  month: "long",
  day: "numeric",
});
const shortSource = (ref: JonesBuildRef) =>
  ref.sourceSha && /^[a-f0-9]{40}$/i.test(ref.sourceSha) ? ref.sourceSha.slice(0, 7) : undefined;

function buildParts(ref: JonesBuildRef) {
  const stamp = parseJonesBuildStartedAt(ref.startedAt);
  if (stamp === undefined) return undefined;
  const date = new Date(stamp);
  const parts = Object.fromEntries(
    numeric.formatToParts(date).map(({ type, value }) => [type, value]),
  );
  const { year, month, day, hour, minute, second } = parts;
  const wall = Date.parse(`${year!.padStart(4, "0")}-${month}-${day}T${hour}:${minute}:${second}Z`);
  const offset = (wall - Math.floor(date.getTime() / 1000) * 1000) / 60000;
  const absolute = Math.abs(offset);
  const zone =
    offset === -240
      ? "EDT"
      : offset === -300
        ? "EST"
        : `ET (UTC${offset < 0 ? "-" : "+"}${String(Math.floor(absolute / 60)).padStart(2, "0")}:${String(absolute % 60).padStart(2, "0")})`;
  return { stamp, date, month, day, hour, minute, second, zone };
}

export function jonesBuildLabel(
  ref: JonesBuildRef,
  form: JonesBuildLabelForm = "month-day",
): JonesBuildLabel {
  const parts = buildParts(ref);
  if (!parts)
    return {
      text: shortSource(ref) ?? ref.version,
      timed: false,
      spoken: "Build time not recorded",
      detail: "Build time not recorded",
    };
  return {
    text: `${form === "month-day" ? parts.month : ""}${parts.day}·${parts.hour}${parts.minute}`,
    timed: true,
    spoken: `Built ${spokenDate.format(parts.date)}, ${parts.hour}:${parts.minute} Eastern`,
    detail: `Built ${calendar.format(parts.date)}, ${parts.hour}:${parts.minute}:${parts.second} ${parts.zone} (${parts.stamp})`,
  };
}

const differentBuild = (a: JonesBuildRef, b: JonesBuildRef) =>
  a.version !== b.version ||
  (a.sourceSha !== undefined && b.sourceSha !== undefined && a.sourceSha !== b.sourceSha);

export function jonesBuildLabels(
  input: readonly JonesBuildRef[],
  form: JonesBuildLabelForm = "month-day",
): JonesBuildLabel[] {
  const refs = input.map((ref) =>
    input.find((other) => ref.version === other.version && ref.sourceSha === other.sourceSha)!,
  );
  const labels = refs.map((ref) => jonesBuildLabel(ref, form));
  const parts = refs.map(buildParts);
  const collisions = () =>
    labels.map((label, i) =>
      labels.some(
        (other, j) => i !== j && label.text === other.text && differentBuild(refs[i]!, refs[j]!),
      ),
    );
  let colliding = collisions();
  if (form === "day") {
    labels.forEach((label, i) => {
      if (colliding[i]) label.text = jonesBuildLabel(refs[i]!).text;
    });
  }
  colliding = collisions();
  const zoneNeeded = labels.map(
    (label, i) =>
      colliding[i] &&
      parts[i] &&
      labels.some(
        (other, j) => label.text === other.text && parts[j] && parts[i]!.zone !== parts[j]!.zone,
      ),
  );
  labels.forEach((label, i) => {
    if (zoneNeeded[i]) label.text += ` ${parts[i]!.zone}`;
  });
  colliding = collisions();
  labels.forEach((label, i) => {
    if (colliding[i] && parts[i]) label.text += `:${parts[i]!.second}`;
  });
  colliding = collisions();
  labels.forEach((label, i) => {
    const source = shortSource(refs[i]!);
    if (colliding[i] && source) label.text += `· ${source}`;
  });
  colliding = collisions();
  labels.forEach((label, i) => {
    if (colliding[i]) label.text = refs[i]!.version;
  });
  colliding = collisions();
  labels.forEach((label, i) => {
    if (colliding[i]) label.text += `· ${refs[i]!.sourceSha ?? "source not recorded"}`;
  });
  return labels;
}
