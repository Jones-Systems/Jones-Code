import {
  BUILT_IN_THEMES,
  getThemeColorsForAppearance,
  type ThemeAppearance,
  type ThemeColorRole,
  type ThemeColors,
} from "@t3tools/shared/themePalettes";
import {
  applyThemeColorPreview,
  applyThemePalette,
  getStandardThemeColors,
  themeColorToHex,
} from "../../themePalette";

export type ReviewTheme = "app" | ThemeAppearance;

export const REVIEW_THEME_OPTIONS = [
  { value: "app", label: "App theme" },
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
] as const satisfies ReadonlyArray<{ value: ReviewTheme; label: string }>;

const HOST_THEME_STYLE = "t3-theme";
const HOST_ROLES = {
  canvas: "--background",
  text: "--foreground",
  surface: "--card",
  surfaceOverlay: "--popover",
  muted: "--muted",
  mutedForeground: "--muted-foreground",
  secondary: "--secondary",
  secondaryForeground: "--secondary-foreground",
  border: "--border",
  input: "--input",
  focus: "--ring",
  messageAction: "--primary",
  messageActionForeground: "--primary-foreground",
  accent: "--accent",
  accentForeground: "--accent-foreground",
  accentSurface: "--accent-surface",
  accentSurfaceForeground: "--accent-surface-foreground",
  error: "--destructive",
  errorForeground: "--destructive-foreground",
  errorSurface: "--destructive-surface",
  warning: "--warning",
  warningForeground: "--warning-foreground",
  warningSurface: "--warning-surface",
  codeBackground: "--code-background",
  codeForeground: "--code-foreground",
} as const satisfies Partial<Record<ThemeColorRole, string>>;

function hostThemeRule() {
  const style = document.getElementById(HOST_THEME_STYLE);
  if (!(style instanceof HTMLStyleElement)) return null;
  const rule = style.sheet?.cssRules[0];
  return rule instanceof CSSStyleRule ? rule.style : null;
}

function colorsFromHost(style: CSSStyleDeclaration, appearance: ThemeAppearance): ThemeColors {
  const colors = getStandardThemeColors(appearance);
  const projected: Partial<Record<keyof typeof HOST_ROLES, string>> = {};
  for (const role of Object.keys(HOST_ROLES) as Array<keyof typeof HOST_ROLES>) {
    const value = style.getPropertyValue(HOST_ROLES[role]).trim();
    if (CSS.supports("color", value)) projected[role] = value;
  }

  // The inline protocol sends a subset of the product palette. Recover the complete palette
  // when that subset identifies a built-in theme, including its sidebar-specific surfaces.
  const entries = Object.entries(projected) as Array<[keyof typeof HOST_ROLES, string]>;
  const match = [
    colors,
    ...BUILT_IN_THEMES.map((theme) => getThemeColorsForAppearance(theme, appearance)),
  ].find(
    (candidate) =>
      candidate !== null &&
      entries.length > 0 &&
      entries.every(
        ([role, color]) => (themeColorToHex(candidate[role]) ?? candidate[role]) === color,
      ),
  );
  if (match) return match;

  // Older viewers do not send custom sidebar roles. Carry the supplied palette through the
  // surrounding surfaces and use those same colors for the sidebar defaults.
  return {
    ...colors,
    ...projected,
    chrome: projected.canvas ?? colors.chrome,
    surfaceRaised: projected.surface ?? colors.surfaceRaised,
    sidebar: projected.surface ?? colors.sidebar,
    sidebarForeground: projected.text ?? colors.sidebarForeground,
    sidebarMutedForeground: projected.mutedForeground ?? colors.sidebarMutedForeground,
    sidebarControlSurface: projected.muted ?? colors.sidebarControlSurface,
    sidebarRowHover: projected.accentSurface ?? colors.sidebarRowHover,
    sidebarRowActive: projected.accentSurface ?? colors.sidebarRowActive,
    sidebarRowSelected: projected.accentSurface ?? colors.sidebarRowSelected,
    sidebarBorder: projected.border ?? colors.sidebarBorder,
  };
}

export function applyReviewTheme(theme: ReviewTheme): void {
  const root = document.documentElement;
  const host = hostThemeRule();
  const hostScheme = host?.getPropertyValue("color-scheme").trim();
  const appAppearance =
    hostScheme === "light" || hostScheme === "dark"
      ? hostScheme
      : window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light";
  const appearance = theme === "app" ? appAppearance : theme;

  if (theme === "app" && host) {
    applyThemeColorPreview(colorsFromHost(host, appearance), appearance);
  } else {
    applyThemePalette("system", appearance);
  }
  root.classList.toggle("dark", appearance === "dark");
  root.style.colorScheme = appearance;
  root.style.removeProperty("background-color");
  document.body.style.removeProperty("background-color");

  // Keep the walkthrough chrome legible on the surrounding thread in either comparison theme.
  for (const token of [
    "--foreground",
    "--muted-foreground",
    "--border",
    "--background",
    "--primary",
    "--primary-foreground",
    "--accent-surface",
    "--accent-surface-foreground",
    "--ring",
    "--font-sans",
    "--font-mono",
  ]) {
    const value = host?.getPropertyValue(token).trim();
    if (value) root.style.setProperty(`--review-host-${token.slice(2)}`, value);
  }
  for (const font of ["--font-sans", "--font-mono"]) {
    const value = host?.getPropertyValue(font).trim();
    if (value && CSS.supports("font-family", value)) root.style.setProperty(font, value);
  }
}

/** The inline viewer rewrites its existing theme sheet on load and when the reader changes theme. */
export function followReviewHostTheme(getSelection: () => ReviewTheme): () => void {
  const style = document.getElementById(HOST_THEME_STYLE);
  if (style === null) return () => undefined;
  const observer = new MutationObserver(() => applyReviewTheme(getSelection()));
  observer.observe(style, { childList: true, characterData: true, subtree: true });
  return () => observer.disconnect();
}
