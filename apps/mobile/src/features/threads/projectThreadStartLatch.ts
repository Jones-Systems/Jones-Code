export function createProjectThreadStartLatch() {
  let submitting = false;

  return {
    async run(submit: () => Promise<void>): Promise<void> {
      if (submitting) return;
      submitting = true;
      try {
        await submit();
      } finally {
        submitting = false;
      }
    },
  };
}
