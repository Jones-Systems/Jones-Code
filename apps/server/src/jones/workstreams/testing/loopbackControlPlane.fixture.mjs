// Test-only ESM bridge: web's composite TypeScript project does not own server sources.
// The declaration exposes only fixture controls; runtime uses the actual server helper.
import { startLoopbackControlPlane, startEnvironment } from "./loopbackControlPlane.ts";

export async function startWireFixture() {
  const control = await startLoopbackControlPlane();
  try {
    const environment = await startEnvironment(control, "source-a");
    return {
      control,
      origin: environment.origin,
      close: async () => {
        try {
          await environment.close();
        } finally {
          await control.close();
        }
      },
    };
  } catch (error) {
    await control.close();
    throw error;
  }
}
