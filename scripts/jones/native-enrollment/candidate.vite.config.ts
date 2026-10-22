import { defineConfig, mergeConfig } from "vite-plus";
import baseConfig from "../../../vite.config.ts";

export default defineConfig(
  mergeConfig(baseConfig, {
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
  }),
);
