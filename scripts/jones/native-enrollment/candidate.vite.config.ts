import { defineConfig, mergeConfig } from "vite-plus";
import baseConfig from "../../../vite.config.ts";

// Root project discovery would override this qualification's dedicated include set.
export default defineConfig(
  mergeConfig(
    { ...baseConfig, test: { ...baseConfig.test, projects: undefined } },
    {
      test: {
        fileParallelism: false,
        setupFiles: [new URL("./candidate.setup.ts", import.meta.url).pathname],
        include: [
          "apps/server/src/jones/workstreams/enrollment/*.test.ts",
          "apps/server/src/environment/NativeStoreAuthority.test.ts",
          "apps/server/src/environment/nativeStoreAuthorityPersistence.test.ts",
          "packages/shared/src/nodeSqliteClient.test.ts",
        ],
      },
    },
  ),
);
