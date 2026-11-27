// An entry imports this first: ES modules evaluate in import order, so product modules that read
// storage or the desktop bridge while they evaluate find the isolated replacements.
import { installDesignReviewIsolation, reportStartupFailures } from "./isolation";

reportStartupFailures();
installDesignReviewIsolation();
