import { applyThemePalette } from "../../themePalette";

export type ReviewTheme = "light" | "dark";

export const REVIEW_THEME_OPTIONS: ReadonlyArray<{ value: ReviewTheme; label: string }> = [
  { value: "light", label: "Light" },
  { value: "dark", label: "Dark" },
];

/** The app's default theme follows the system, so a review starts from the viewer's scheme. */
export function preferredReviewTheme(): ReviewTheme {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

/** Applies the default palette in one appearance, as the app's theme hook does. */
export function applyReviewTheme(theme: ReviewTheme): void {
  applyThemePalette("system", theme);
  document.documentElement.classList.toggle("dark", theme === "dark");
  // The theme hook paints the page background inline once when it loads, from the scheme at that
  // moment. Clearing it lets the product stylesheet's chrome background follow this appearance.
  document.documentElement.style.removeProperty("background-color");
  document.body.style.removeProperty("background-color");
}
