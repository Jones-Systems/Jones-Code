import { typecheckTask } from "../../scripts/jones/tsrs/task-cache.ts";
import packageJson from "./package.json" with { type: "json" };
import { defineConfig } from "vite-plus";
import "vite-plus/test/config";

const reactTests = [
  "src/jones/descendantApproval/DescendantApprovalCard.test.tsx",
  "src/state/use-selected-thread-git-state.test.tsx",
  "src/features/workstreams/Controls.operations.test.tsx",
  "src/features/workstreams/useWorkstreams.authorization.test.tsx",
  "src/features/pull-requests/usePullRequestEnvironmentFallback.test.tsx",
  "src/lib/filePreview.test.ts",
  "src/features/threads/thread-inspector-navigation.test.tsx",
  "src/features/diffs/nativeReviewDiffSurface.test.tsx",
  "src/features/pull-requests/usePullRequestDiff.test.tsx",
  "src/components/ui/Dialog.android.test.tsx",
  "src/features/settings/SettingsServerControlsRouteScreen.test.tsx",
  "src/features/settings/components/BranchNamingSettings.test.tsx",
  "src/features/threads/UsageLimitRecoveryCard.test.tsx",
  "src/native/NativeHeaderToolbar.test.tsx",
  "src/native/useRestingTopOffset.test.tsx",
  "src/native/useRetryingImageSource.test.tsx",
  "src/state/queries.debounce.test.tsx",
  "src/state/use-selected-thread-git-actions.test.tsx",
  "src/features/terminal/useTerminalExitNavigation.test.tsx",
];

export default defineConfig({
  run: { tasks: { "tsrs:typecheck": typecheckTask(packageJson.scripts["typecheck:native"]) } },
  test: {
    hookTimeout: 60_000,
    testTimeout: 60_000,
    projects: [
      {
        extends: true,
        test: {
          name: "unit",
          include: ["**/*.test.{ts,tsx,mjs}"],
          exclude: ["**/node_modules/**", ...reactTests],
          setupFiles: [
            "../../packages/shared/src/testing/longTempDir.ts",
            "../../packages/shared/src/testing/providerEnv.ts",
          ],
        },
      },
      {
        extends: true,
        test: {
          name: "react",
          include: reactTests,
          // Native components mock React Native and render through react-dom in these tests.
          environment: "jsdom",
          setupFiles: ["../../packages/shared/src/testing/providerEnv.ts"],
        },
      },
    ],
  },
});
