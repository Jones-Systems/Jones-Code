import { act as rendererAct } from "react-test-renderer";

export async function act<T>(callback: () => T | Promise<T>): Promise<T> {
  const environment = globalThis as typeof globalThis & {
    IS_REACT_ACT_ENVIRONMENT: boolean | undefined;
  };
  const previous = environment.IS_REACT_ACT_ENVIRONMENT;
  environment.IS_REACT_ACT_ENVIRONMENT = true;
  try {
    let result!: T;
    await rendererAct(async () => {
      result = await callback();
    });
    return result;
  } finally {
    environment.IS_REACT_ACT_ENVIRONMENT = previous;
  }
}
