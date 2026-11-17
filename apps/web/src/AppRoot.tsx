import { RouterProvider } from "@tanstack/react-router";

import { ElectronBrowserHost } from "./browser/ElectronBrowserHost";
import { QuitHoldOverlay } from "./components/QuitHoldOverlay";
import { FleetController } from "./jones/fleetUpdates/FleetController";
import { InstallPlanController } from "./jones/installStatus/InstallPlanController";
import { CompanionController } from "./jones/previewCompanion/CompanionController";
import { MonitorController } from "./jones/threadMonitor/MonitorController";
import { I18nProvider } from "./i18n/I18nProvider";
import { AppAtomRegistryProvider } from "./rpc/atomRegistry";
import type { AppRouter } from "./router";

// Keep webviews outside the router but inside its atom registry so route changes preserve them.
export function AppRoot({ router }: { readonly router: AppRouter }) {
  return (
    <I18nProvider>
      <AppAtomRegistryProvider>
        <RouterProvider router={router} />
        <MonitorController router={router} />
        <CompanionController />
        <FleetController />
        <InstallPlanController />
        <ElectronBrowserHost />
        <QuitHoldOverlay />
      </AppAtomRegistryProvider>
    </I18nProvider>
  );
}
