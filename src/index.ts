/**
 * sinwan-grpc — gRPC support for sinwan-engine
 *
 * Implements the `GRPCProvider` interface from sinwan-engine using
 * @grpc/grpc-js and @grpc/proto-loader.
 *
 * Usage:
 *
 *   import { Sinwan } from "sinwan-engine";
 *   import { sinwanGRPC } from "sinwan-grpc";
 *
 *   const app = new Sinwan();
 *   app.register(sinwanGRPC);
 *   // now app.grpc(), app.listenGRPC(), app.connectGRPC() work
 */

import type { Runtime, Sinwan, SinwanModule } from "sinwan-engine";
import { type GRPCProvider, registerGRPCProvider } from "sinwan-engine";

import {
  GRPCRouter,
  type GRPCServiceConfig,
  type GRPCListenOptions,
  type GRPCServerHandle,
  type GRPCHook,
  type GRPCAfterHook,
  type GRPCErrorHook,
} from "./server";
import { GRPCClient, type GRPCClientConfig } from "./client";

// ─── Module Augmentation ────────────────────────────────────
// When sinwan-grpc is imported, these typed overloads replace the
// `unknown` defaults on the Sinwan class from sinwan-engine.

declare module "sinwan-engine" {
  interface Sinwan {
    grpc(name: string, config: GRPCServiceConfig): this;
    listenGRPC(options?: GRPCListenOptions): Promise<GRPCServerHandle>;
    listenGRPC(
      name: string,
      options?: GRPCListenOptions,
    ): Promise<GRPCServerHandle>;
    connectGRPC<
      S extends Record<string, (...args: unknown[]) => unknown> = Record<
        string,
        (...args: unknown[]) => unknown
      >,
    >(
      config: GRPCClientConfig,
    ): GRPCClient<S>;
    beforeGRPC(event: "call", handler: GRPCHook): this;
    beforeGRPC(event: "finish", handler: GRPCAfterHook): this;
    beforeGRPC(event: "error", handler: GRPCErrorHook): this;
  }
}

class SinwanGRPCProvider implements GRPCProvider {
  private readonly router: GRPCRouter;

  constructor() {
    this.router = new GRPCRouter();
  }

  registerService(name: string, config: unknown): void {
    this.router.grpc(name, config as GRPCServiceConfig);
  }

  listen(runtime: Runtime, options?: unknown): Promise<unknown>;
  listen(runtime: Runtime, name: string, options?: unknown): Promise<unknown>;
  async listen(
    runtime: Runtime,
    nameOrOptions?: string | unknown,
    options?: unknown,
  ): Promise<unknown> {
    if (typeof nameOrOptions === "string") {
      return this.router.listen(
        runtime,
        nameOrOptions,
        options as GRPCListenOptions | undefined,
      );
    }
    return this.router.listen(
      runtime,
      nameOrOptions as GRPCListenOptions | undefined,
    );
  }

  connect(config: unknown): unknown {
    return GRPCClient.create(config as GRPCClientConfig);
  }

  async stop(): Promise<void> {
    await this.router.stop();
  }
}

let registered = false;

/**
 * Register sinwan-grpc as the gRPC provider for sinwan-engine.
 * Safe to call multiple times — subsequent calls are no-ops.
 */
export function registerSinwanGRPC(): void {
  if (registered) return;
  registerGRPCProvider(new SinwanGRPCProvider());
  registered = true;
}

/**
 * SinwanModule for gRPC — pass to `app.register()` to enable gRPC.
 *
 * ```ts
 * import { Sinwan } from "sinwan-engine";
 * import { sinwanGRPC } from "sinwan-grpc";
 *
 * const app = new Sinwan();
 * app.register(sinwanGRPC);
 * ```
 */
export const sinwanGRPC: SinwanModule = {
  name: "sinwan-grpc",
  register() {
    registerSinwanGRPC();
  },
};

// ─── Re-exports ────────────────────────────────────────────

export {
  GRPCRouter,
  DEFAULT_GRPC_LOADER_OPTIONS,
  collectGRPCStream,
  createGRPCError,
  createGRPCMetadata,
  grpcBidi,
  grpcClientStream,
  grpcServerStream,
  grpcUnary,
  loadGRPCService,
  mergeGRPCLoaderOptions,
  resolveGRPCServerCredentials,
  writeGRPCMessage,
} from "./server";

export type {
  GRPCAfterHook,
  GRPCAnyHandler,
  GRPCBaseMethodRoute,
  GRPCBidiStreamHandler,
  GRPCBidiStreamRoute,
  GRPCCallInfo,
  GRPCClientStreamHandler,
  GRPCClientStreamRoute,
  GRPCErrorHook,
  GRPCHook,
  GRPCHooks,
  GRPCListenOptions,
  GRPCLoadedService,
  GRPCMetadataInit,
  GRPCMethodConfig,
  GRPCMethodKind,
  GRPCMethodRoute,
  GRPCProtoPath,
  GRPCRouterDefaults,
  GRPCServerCredentialsInput,
  GRPCServerHandle,
  GRPCServerStreamHandler,
  GRPCServerStreamRoute,
  GRPCServiceConfig,
  GRPCServiceTarget,
  GRPCUnaryHandler,
  GRPCUnaryRoute,
} from "./server";

export {
  GRPCClient,
  createGRPCClient,
  resolveGRPCClientCredentials,
} from "./client";

export type {
  GRPCCallOptions,
  GRPCClientConfig,
  GRPCClientCredentialsInput,
  GRPCClientStreamCall,
  GRPCReconnectOptions,
  GRPCConnectionState,
  GRPCConnectionListener,
} from "./client";

// Health checks
export { GRPCHealthChecker } from "./health";
export type { GRPCHealthOptions, HealthServingStatus } from "./health";

// Interceptors
export {
  composeServerInterceptors,
  composeClientInterceptors,
  createLoggingInterceptor,
  createMetadataInjectionInterceptor,
} from "./interceptors";
export type {
  GRPCServerInterceptor,
  GRPCClientInterceptor,
} from "./interceptors";

// Cancellation
export { makeAbortablePromise, isCallCancelled } from "./cancellation";
export type { GRPCAbortablePromise } from "./cancellation";

// Compression
export { resolveCompressionOptions } from "./compression";
export type { GRPCCompressionConfig } from "./compression";

// Propagation
export {
  runWithPropagationContext,
  getPropagationContext,
  extractDeadlineFromMetadata,
  createPropagationInterceptor,
} from "./propagation";
export type {
  GRPCPropagationConfig,
  GRPCPropagationContext,
} from "./propagation";

// Server reflection
export { createReflectionService, addReflectionToServer } from "./reflection";
export type { GRPCReflectionOptions } from "./reflection";

// grpc-web proxy
export { startGRPCWebProxy } from "./grpc-web";
export type { GRPCWebProxyOptions, GRPCWebProxyHandle } from "./grpc-web";

// Tracing
export {
  createTracingServerInterceptor,
  createTracingClientInterceptor,
} from "./tracing";
export type { GRPCTracingConfig } from "./tracing";

// Load balancing
export {
  resolveLoadBalancingOptions,
  formatLoadBalancingTarget,
} from "./load-balancing";
export type {
  GRPCLoadBalancingPolicy,
  GRPCLoadBalancingConfig,
} from "./load-balancing";

// TLS certificate rotation
export {
  readCertData,
  createServerCredentialsFromData,
  createChannelCredentialsFromData,
  watchCertificates,
} from "./tls-rotation";
export type {
  GRPCTLSCertPaths,
  GRPCTLSCertData,
  GRPCTLSCertRotationOptions,
} from "./tls-rotation";

// Channel pooling
export { GRPCChannelPool } from "./channel-pool";
export type {
  GRPCChannelPoolStrategy,
  GRPCChannelPoolOptions,
} from "./channel-pool";
