export default async function customScenario(ctx) {
  await ctx.step("Resize native window and verify dark theme", async () => {
    const bounds = await ctx.setWindowSize(720, 600);
    ctx.assert(
      bounds.width === 720 && bounds.height === 600,
      "Native window size matches requested narrow dimensions",
    );
    await ctx.setTheme("dark");
  });
  await ctx.capture("narrow-dark");
}
