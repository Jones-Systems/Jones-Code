// Import order is load-bearing: isolation, then the offline connection, before any product module.
import "./install-isolation";
import "./offline-connection";
import "@fontsource-variable/inter";
import "../../index.css";
import "../sidebarAppearance/sidebarAppearance.cards.css";
import "../sidebarAppearance/sidebarAppearance.workstreams.css";
import { mountDesignReview } from "./mountDesignReview";
import { resolveDesignReviewSource } from "./reviewSource";
import { applyReviewTheme, preferredReviewTheme } from "./reviewTheme";
import { ThreadCardBrightnessReview } from "./ThreadCardBrightnessReview";

const initialTheme = preferredReviewTheme();
applyReviewTheme(initialTheme);
mountDesignReview(
  <ThreadCardBrightnessReview
    initialTheme={initialTheme}
    nowMs={Date.now()}
    source={resolveDesignReviewSource(document.getElementById("root"))}
  />,
);
