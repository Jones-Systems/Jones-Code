import {
  THREAD_MONITOR_THEME_KEYS,
  type ThreadMonitorTheme,
} from "@t3tools/contracts/jones/desktopThreadMonitor";
const safeValue = (value: string) =>
  value.length <= 512 &&
  !/[;{}<>\\\u0000-\u001f]/.test(value) &&
  !/(?:url|expression|@import)\s*\(/i.test(value);
export function readMonitorTheme(): ThreadMonitorTheme {
  const root = document.documentElement;
  const style = getComputedStyle(root);
  const mode = root.classList.contains("dark") || style.colorScheme === "dark" ? "dark" : "light";
  const values: Record<string, string> = {};
  for (const key of THREAD_MONITOR_THEME_KEYS) {
    const value = style.getPropertyValue(key).trim();
    if (value && safeValue(value)) values[key] = value;
  }
  const font = getComputedStyle(document.body).fontFamily;
  if (!values["--font-sans"] && safeValue(font)) values["--font-sans"] = font;
  const roles =
    mode === "dark"
      ? {
          working: "oklch(.828 .111 230.318 / .8)",
          input: "oklch(.785 .115 274.713 / .9)",
          approval: "oklch(.879 .169 91.605 / .9)",
          plan: "oklch(.811 .111 293.571 / .9)",
          done: "oklch(.845 .143 164.978 / .9)",
        }
      : {
          working: "oklch(.588 .158 241.966)",
          input: "oklch(.511 .262 276.966)",
          approval: "oklch(.666 .179 58.318)",
          plan: "oklch(.541 .281 293.009)",
          done: "oklch(.596 .145 163.225)",
        };
  for (const [role, value] of Object.entries(roles)) values[`--status-${role}`] ??= value;
  return { mode, values };
}
export function observeMonitorTheme(listener: (theme: ThreadMonitorTheme) => void): () => void {
  let scheduled = false;
  let previous = "";
  const update = () => {
    scheduled = false;
    const theme = readMonitorTheme();
    const key = JSON.stringify(theme);
    if (previous !== key) {
      previous = key;
      listener(theme);
    }
  };
  const schedule = () => {
    if (!scheduled) {
      scheduled = true;
      queueMicrotask(update);
    }
  };
  const observer = new MutationObserver(schedule);
  observer.observe(document.documentElement, {
    attributes: true,
    attributeFilter: ["class", "style", "data-theme"],
  });
  observer.observe(document.body, { attributes: true, attributeFilter: ["class", "style"] });
  observer.observe(document.head, {
    childList: true,
    subtree: true,
    characterData: true,
    attributes: true,
  });
  const media = matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", schedule);
  document.fonts?.addEventListener("loadingdone", schedule);
  update();
  return () => {
    observer.disconnect();
    media.removeEventListener("change", schedule);
    document.fonts?.removeEventListener("loadingdone", schedule);
  };
}
export function applyMonitorTheme(theme: ThreadMonitorTheme) {
  const root = document.documentElement;
  for (const key of THREAD_MONITOR_THEME_KEYS) {
    const value = theme.values[key];
    if (value !== undefined && safeValue(value)) root.style.setProperty(key, value);
    else root.style.removeProperty(key);
  }
  root.dataset.monitorTheme = theme.mode;
  root.style.colorScheme = theme.mode;
}
