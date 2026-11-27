// Written the way an agent with only the skill would write it.
const { workflow, agent, stop, webhook, WebhookRequest } = T3;
const decodeDeploy = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Struct({ status: Schema.String, service: Schema.String })),
);

export default [
  workflow(
    "deploy-watch",
    {
      version: 1,
      payload: WebhookRequest,
      triggers: [webhook("deploys", { key: (request) => request.headers["x-delivery-id"] })],
    },
    Effect.fn(function* (request) {
      const event = yield* Effect.orDie(decodeDeploy(request.body));
      if (event.status !== "failed") return yield* stop("the deploy did not fail");
      yield* agent("investigate", {
        project: "proj-1",
        brief: `The deploy of ${event.service} failed. Find out why and report the cause.`,
      });
    }),
  ),
];
