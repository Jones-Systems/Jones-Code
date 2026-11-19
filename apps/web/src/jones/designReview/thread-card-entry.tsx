// Import order is load-bearing: isolation, then the offline connection, before any product module.
import "./install-isolation";
import "./offline-connection";
import "@fontsource-variable/inter";
import "../../index.css";
import "../sidebarAppearance/sidebarAppearance.cards.css";
import "../sidebarAppearance/sidebarAppearance.workstreams.css";
import { mountDesignReview } from "./mountDesignReview";
import { resolveDesignReviewSource } from "./reviewSource";
import { applyReviewTheme } from "./reviewTheme";
import { ThreadCardBrightnessReview } from "./ThreadCardBrightnessReview";

applyReviewTheme("app");
mountDesignReview(
  <ThreadCardBrightnessReview
    nowMs={Date.now()}
    source={resolveDesignReviewSource(document.getElementById("root"))}
  />,
);
