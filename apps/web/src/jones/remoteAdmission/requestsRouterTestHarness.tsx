import {
  createBrowserHistory,
  createHashHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from "@tanstack/react-router";
import type { ReactNode } from "react";
import { render } from "../../../test/render";
import { TooltipProvider } from "../../components/ui/tooltip";
import { Route } from "../../routes/requests";

export async function renderRequestsRouter(
  historyType: "browser" | "hash",
  { initial = "/requests", alongside }: { initial?: string; alongside?: ReactNode } = {},
) {
  const before = { url: window.location.href, state: window.history.state };
  const initialUrl = new URL(historyType === "hash" ? `#${initial}` : initial, before.url);
  if (historyType === "hash") initialUrl.search = "";
  window.history.replaceState(null, "", initialUrl);
  const history = historyType === "hash" ? createHashHistory() : createBrowserHistory();
  const cleanup = () => {
    history.destroy();
    window.history.replaceState(before.state, "", before.url);
  };
  try {
    const root = createRootRoute({
      component: () => (
        <TooltipProvider>
          {alongside}
          <Outlet />
        </TooltipProvider>
      ),
    });
    const requests = createRoute({
      getParentRoute: () => root,
      path: "/requests",
      validateSearch: Route.options.validateSearch!,
      component: Route.options.component!,
    });
    const index = createRoute({ getParentRoute: () => root, path: "/", component: () => null });
    const thread = createRoute({
      getParentRoute: () => root,
      path: "/$environmentId/$threadId",
      component: () => {
        const { environmentId, threadId } = thread.useParams();
        return (
          <p>
            Thread {threadId} on {environmentId}
          </p>
        );
      },
    });
    const router = createRouter({
      history,
      routeTree: root.addChildren([index, requests, thread]),
      disableGlobalCatchBoundary: true,
    });
    const screen = await render(<RouterProvider router={router} />, { router: false });
    return {
      screen,
      router,
      async [Symbol.asyncDispose]() {
        try {
          await screen[Symbol.asyncDispose]();
        } finally {
          cleanup();
        }
      },
    };
  } catch (error) {
    cleanup();
    throw error;
  }
}
