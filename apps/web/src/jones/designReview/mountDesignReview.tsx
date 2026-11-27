import {
  createMemoryHistory,
  createRootRoute,
  createRouter,
  RouterProvider,
} from "@tanstack/react-router";
import { createContext, StrictMode, use, type ReactNode } from "react";
import { createRoot } from "react-dom/client";
import { AppAtomRegistryProvider } from "../../rpc/atomRegistry";
import {
  DESIGN_REVIEW_MOUNTED_ATTRIBUTE,
  isolationReport,
  renderReviewTextFallback,
} from "./isolation";
import { connectionHeldOffline } from "./offline-connection";

const ReviewContent = createContext<ReactNode>(null);

function ReviewRoute() {
  return use(ReviewContent);
}

/**
 * Mounts a review into #root with the app's atom registry and an in-memory router, which product
 * links and navigation hooks need. A review that cannot be isolated shows text instead.
 */
export function mountDesignReview(content: ReactNode): void {
  const container = document.getElementById("root");
  if (container === null) throw new Error("A design review needs a #root element.");
  const report = isolationReport();
  const failures = [
    ...report.failures,
    ...(connectionHeldOffline ? [] : ["The app connection started before isolation."]),
  ];
  if (failures.length > 0) {
    renderReviewTextFallback(
      container,
      `This design review did not render product components.\n${failures.join("\n")}`,
    );
    return;
  }
  const router = createRouter({
    routeTree: createRootRoute({ component: ReviewRoute }),
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  container.setAttribute(DESIGN_REVIEW_MOUNTED_ATTRIBUTE, "");
  createRoot(container).render(
    <StrictMode>
      <AppAtomRegistryProvider>
        <ReviewContent value={content}>
          <RouterProvider router={router} />
        </ReviewContent>
      </AppAtomRegistryProvider>
    </StrictMode>,
  );
}
