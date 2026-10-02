import * as NativeCreationRepositoryLayer from "./persistence/Layers/NativeCreationRepository.ts";
import { NativeCreationAuthorityUnavailable } from "./orchestration/NativeCreationAuthority.ts";
import * as AuthSessions from "./persistence/AuthSessions.ts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeHttp from "node:http";

import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  EnvironmentHttpApi,
  ProviderDriverKind,
  type RepositoryIdentity,
} from "@t3tools/contracts";
import type { RelayManagedEndpointRuntimeConfig } from "@t3tools/contracts/relay";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Duration from "effect/Duration";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Random from "effect/Random";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { FetchHttpClient, HttpRouter, HttpServer } from "effect/unstable/http";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";

import * as BackgroundPolicy from "./background/BackgroundPolicy.ts";
import * as HostPowerMonitor from "./background/HostPowerMonitor.ts";
import * as ServerConfig from "./config.ts";
import {
  otlpTracesProxyRouteLayer,
  assetRouteLayer,
  attachmentUploadRouteLayer,
  serverEnvironmentHttpApiLayer,
  staticAndDevRouteLayer,
  browserApiCorsLayer,
  httpCompressionLayer,
  untracedRequestsLayer,
} from "./http.ts";
import { guardHttpResponseWriteErrors } from "./httpResponseErrorGuard.ts";
import { fixPath } from "./os-jank.ts";
import { websocketRpcRouteLayer } from "./ws.ts";
import * as ExternalLauncher from "./process/externalLauncher.ts";
import * as NodePtyAdapter from "./terminal/NodePtyAdapter.ts";
import {
  workstreamGatewayLayerLive,
  workstreamHttpApiLayer,
  workstreamResponseHeadersLayer,
} from "./workstreams/http.ts";
import { jonesUpdatesHttpApiLayer } from "./jonesUpdates/http.ts";
import * as JonesUpdates from "./jonesUpdates/service.ts";
import { voiceReviewHttpApiLayer, voiceReviewResponseHeadersLayer } from "./voiceReview/http.ts";
