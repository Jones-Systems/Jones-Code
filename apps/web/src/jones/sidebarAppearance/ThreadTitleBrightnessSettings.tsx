import { SettingsRow, SettingsSection } from "../../components/settings/settingsLayout";
import { sidebarAppearanceScope } from "./model";
import { ThreadTitleBrightnessSlider } from "./ThreadTitleBrightnessSlider";
import { useSidebarAppearanceStore } from "./useSidebarAppearanceStore";
import "./sidebarAppearance.cards.css";

export function ThreadTitleBrightnessSettings() {
  const store = useSidebarAppearanceStore();
  const scope = sidebarAppearanceScope(store.stored);
  return (
    <SettingsSection title="Sidebar">
      <SettingsRow
        title="Sidebar thread title brightness"
        description="Brightens dimmed thread titles in the sidebar. Chat text and interface font are not affected."
      >
        <ThreadTitleBrightnessSlider
          value={store.stored.threadTitleBrightnessBoost}
          onChange={(threadTitleBrightnessBoost) => store.update({ threadTitleBrightnessBoost })}
        />
        <div
          {...scope.attributes}
          style={scope.variables}
          className="mt-3"
          aria-label="Thread title previews"
        >
          <div data-jones-sidebar-card className="group/sidebar-row" tabIndex={0}>
            <div
              data-jones-sidebar-card-content
              className="h-[4.875rem] px-(--sidebar-row-content-inset) py-(--sidebar-content-inset)"
            >
              <span
                data-jones-thread-title="primary"
                data-jones-thread-title-branch="card"
                className="text-sm font-medium text-foreground"
              >
                Selected thread
              </span>
            </div>
          </div>
          <div data-jones-sidebar-card className="group/sidebar-row" tabIndex={0}>
            <div
              data-jones-sidebar-card-content
              className="h-[4.875rem] px-(--sidebar-row-content-inset) py-(--sidebar-content-inset)"
            >
              <span
                data-jones-thread-title="receded"
                data-jones-thread-title-branch="card"
                className="text-sm font-normal text-foreground/75 group-hover/sidebar-row:text-foreground group-focus-visible/sidebar-row:text-foreground"
              >
                Unselected read thread
              </span>
            </div>
          </div>
        </div>
        <p role="status">
          {store.saveFailed ? "Couldn't save; showing the saved appearance." : ""}
        </p>
      </SettingsRow>
    </SettingsSection>
  );
}
