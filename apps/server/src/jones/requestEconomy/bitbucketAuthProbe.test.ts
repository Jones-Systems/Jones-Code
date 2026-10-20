import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it, vi } from "@effect/vitest";
import * as BitbucketApi from "@t3tools/source-control-bitbucket/server/BitbucketApi";
import * as ConfigProvider from "effect/ConfigProvider";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { HttpClient, type HttpClientRequest, HttpClientResponse } from "effect/http";

import * as ServerSettings from "../../serverSettings.ts";
import * as ServerSourceControlHost from "../../sourceControl/ServerSourceControlHost.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as VcsDriverRegistry from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";

// Discovery probes Bitbucket auth on every `discover` call, so an unconfigured
// server must answer without a request that can only be rejected.
it.effect("asks Bitbucket for the account only once a credential is configured", () => {
  // Like Bitbucket, `/user` rejects a request without credentials.
  const execute = vi.fn((request: HttpClientRequest.HttpClientRequest) =>
    Effect.succeed(
      HttpClientResponse.fromWeb(
        request,
        request.headers.authorization === undefined
          ? new Response(null, { status: 401 })
          : Response.json({ username: "bitbucket-user" }),
      ),
    ),
  );
  const layer = BitbucketApi.layer.pipe(
    Layer.provide(ServerSourceControlHost.layer),
    Layer.provide(Layer.mock(VcsProcess.VcsProcess)({})),
    Layer.provide(Layer.mock(VcsDriverRegistry.VcsDriverRegistry)({})),
    Layer.provide(Layer.mock(GitVcsDriver.GitVcsDriver)({})),
    Layer.provide(
      Layer.succeed(
        HttpClient.HttpClient,
        HttpClient.make((request) => execute(request)),
      ),
    ),
    Layer.provide(
      ConfigProvider.layer(
        ConfigProvider.fromEnv({
          env: { T3CODE_BITBUCKET_API_BASE_URL: "https://api.test.local/2.0" },
        }),
      ),
    ),
    Layer.provideMerge(ServerSettings.layerTest()),
    Layer.provideMerge(NodeServices.layer),
  );

  return Effect.gen(function* () {
    const bitbucket = yield* BitbucketApi.BitbucketApi;
    const settings = yield* ServerSettings.ServerSettingsService;

    const unconfigured = yield* bitbucket.probeAuth;
    assert.strictEqual(unconfigured.status, "unauthenticated");
    assert.deepStrictEqual(unconfigured.account, Option.none());
    assert.deepStrictEqual(unconfigured.host, Option.some("bitbucket.org"));
    assert.isTrue(Option.isSome(unconfigured.detail));
    assert.strictEqual(execute.mock.calls.length, 0);

    yield* settings.updateSettings({
      sourceControlHosts: { bitbucket: { accessToken: "saved-access-token" } },
    });
    assert.deepStrictEqual(yield* bitbucket.probeAuth, {
      status: "authenticated",
      account: Option.some("bitbucket-user"),
      host: Option.some("bitbucket.org"),
      detail: Option.none(),
    });
    assert.strictEqual(execute.mock.calls.length, 1);
    const request = execute.mock.calls.at(0)?.[0];
    assert.strictEqual(request?.url, "https://api.test.local/2.0/user");
    assert.strictEqual(request?.headers.authorization, "Bearer saved-access-token");
  }).pipe(Effect.provide(layer));
});
