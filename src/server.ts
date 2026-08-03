/**
 * SinwanJS Core Runtime - GRPCRouter
 *
 * A thin, DX-focused adapter over @grpc/grpc-js:
 *  - loads services from .proto files with production-friendly defaults
 *  - supports unary, server-streaming, client-streaming, and bidi methods
 *  - runs each RPC through Sinwan's EventBus (grpc:call / grpc:finish / grpc:error)
 *  - normalizes thrown values into gRPC status errors
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import type { Context, Runtime } from "sinwan-engine";
import { GRPCHealthChecker, type GRPCHealthOptions } from "./health";
import {
  type GRPCServerInterceptor,
  composeServerInterceptors,
} from "./interceptors";
import {
  type GRPCCompressionConfig,
  resolveCompressionOptions,
} from "./compression";
import {
  runWithPropagationContext,
  extractDeadlineFromMetadata,
  type GRPCPropagationConfig,
} from "./propagation";
import { createReflectionService, addReflectionToServer } from "./reflection";

export type GRPCProtoPath = string | string[];

export type GRPCMethodKind = "unary" | "serverStream" | "clientStream" | "bidi";

export type GRPCMetadataInit =
  | grpc.Metadata
  | Record<string, string | Buffer | Array<string | Buffer>>
  | undefined;

export type GRPCServerCredentialsInput =
  | grpc.ServerCredentials
  | "insecure"
  | {
      rootCerts?: Buffer | null;
      keyCertPairs: grpc.KeyCertPair[];
      checkClientCertificate?: boolean;
    };

export interface GRPCServiceTarget {
  /** .proto file path or paths. */
  proto: GRPCProtoPath;
  /** Proto package name, e.g. "users.v1". Optional if service is fully qualified. */
  package?: string;
  /** Service name, e.g. "UserService" or "users.v1.UserService". */
  service: string;
  /** Overrides for proto-loader. Merged with Sinwan defaults. */
  loader?: protoLoader.Options;
}

export interface GRPCCallInfo {
  name: string;
  package?: string;
  service: string;
  method: string;
  path: string;
  kind: GRPCMethodKind;
  request?: unknown;
  metadata: grpc.Metadata;
  peer?: string;
  data: unknown;
}

export type GRPCHook = (
  ctx: Context,
  info: GRPCCallInfo,
) => Promise<void> | void;

export type GRPCAfterHook = (
  ctx: Context,
  info: GRPCCallInfo & { durationMs: number },
) => Promise<void> | void;

export type GRPCErrorHook = (
  ctx: Context,
  error: unknown,
  info: GRPCCallInfo,
) => Promise<unknown | void> | unknown | void;

export interface GRPCHooks {
  /** Runs after the grpc:call bus event and before the method handler. */
  beforeCall?: GRPCHook;
  /** Runs after a successful method handler. */
  afterCall?: GRPCAfterHook;
  /** Runs when middleware or a method handler throws. Return an error to override. */
  onError?: GRPCErrorHook;
}

export type GRPCUnaryHandler<Request = unknown, Response = unknown> = (
  ctx: Context,
  request: Request,
  call: grpc.ServerUnaryCall<Request, Response>,
) => Promise<Response | void> | Response | void;

export type GRPCServerStreamHandler<Request = unknown, Response = unknown> = (
  ctx: Context,
  request: Request,
  call: grpc.ServerWritableStream<Request, Response>,
) =>
  | Promise<void | Iterable<Response> | AsyncIterable<Response>>
  | void
  | Iterable<Response>
  | AsyncIterable<Response>;

export type GRPCClientStreamHandler<Request = unknown, Response = unknown> = (
  ctx: Context,
  call: grpc.ServerReadableStream<Request, Response>,
) => Promise<Response | void> | Response | void;

export type GRPCBidiStreamHandler<Request = unknown, Response = unknown> = (
  ctx: Context,
  call: grpc.ServerDuplexStream<Request, Response>,
) =>
  | Promise<void | Iterable<Response> | AsyncIterable<Response>>
  | void
  | Iterable<Response>
  | AsyncIterable<Response>;

export type GRPCAnyHandler =
  | GRPCUnaryHandler<any, any>
  | GRPCServerStreamHandler<any, any>
  | GRPCClientStreamHandler<any, any>
  | GRPCBidiStreamHandler<any, any>;

export interface GRPCBaseMethodRoute {
  /** Normally inferred from the .proto method definition. */
  type?: GRPCMethodKind;
  handler: GRPCAnyHandler;
  /** Method-level lifecycle hooks. */
  beforeCall?: GRPCHook;
  afterCall?: GRPCAfterHook;
  onError?: GRPCErrorHook;
  /** Automatically end server/bidi streams when the handler completes. Default: true. */
  autoEnd?: boolean;
}

export interface GRPCUnaryRoute<
  Request = unknown,
  Response = unknown,
> extends GRPCBaseMethodRoute {
  type?: "unary";
  handler: GRPCUnaryHandler<Request, Response>;
}

export interface GRPCServerStreamRoute<
  Request = unknown,
  Response = unknown,
> extends GRPCBaseMethodRoute {
  type?: "serverStream";
  handler: GRPCServerStreamHandler<Request, Response>;
}

export interface GRPCClientStreamRoute<
  Request = unknown,
  Response = unknown,
> extends GRPCBaseMethodRoute {
  type?: "clientStream";
  handler: GRPCClientStreamHandler<Request, Response>;
}

export interface GRPCBidiStreamRoute<
  Request = unknown,
  Response = unknown,
> extends GRPCBaseMethodRoute {
  type?: "bidi";
  handler: GRPCBidiStreamHandler<Request, Response>;
}

export type GRPCMethodRoute =
  | GRPCUnaryRoute
  | GRPCServerStreamRoute
  | GRPCClientStreamRoute
  | GRPCBidiStreamRoute;

export type GRPCMethodConfig = GRPCAnyHandler | GRPCMethodRoute;

export interface GRPCServiceConfig extends GRPCServiceTarget {
  /** Service methods keyed by proto name or lower-camel JS name. */
  methods: Record<string, GRPCMethodConfig>;
  /** Per-service hooks. */
  hooks?: GRPCHooks;
  /** Extra data exposed as ctx.grpc.data for every method. */
  data?: unknown;
  /** Validate missing/unknown method handlers at startup. Default: true. */
  strictMethods?: boolean;
  /** Per-service server interceptors. Merged with listen-level interceptors. */
  interceptors?: GRPCServerInterceptor[];
}

export interface GRPCListenOptions {
  /** Full bind address. Takes precedence over host/port. Default: "0.0.0.0:50051". */
  address?: string;
  /** Bind host when address is not supplied. Default: "0.0.0.0". */
  host?: string;
  /** Bind port when address is not supplied. Default: 50051. */
  port?: number | string;
  /** Server credentials. Default: insecure. */
  credentials?: GRPCServerCredentialsInput;
  /** Options passed to new grpc.Server(). */
  serverOptions?: grpc.ServerOptions;
  /** Grace period for stop(). Default: 5000ms. */
  gracefulShutdownMs?: number;
  /** Optional callback after bind succeeds. */
  ready?: (info: GRPCServerHandle) => void;
  /** Enable gRPC health check service (grpc.health.v1.Health). Default: false. */
  health?: boolean | GRPCHealthOptions;
  /** Health checker instance to use. If not provided, one is created when health is enabled. */
  healthChecker?: GRPCHealthChecker;
  /** Server interceptors applied to all services on this server. */
  interceptors?: GRPCServerInterceptor[];
  /** Compression configuration for the server. */
  compression?: GRPCCompressionConfig;
  /** Deadline and metadata propagation settings. */
  propagation?: GRPCPropagationConfig;
  /** Enable gRPC server reflection. Default: false. */
  reflection?: boolean;
}

export interface GRPCRouterDefaults {
  loader?: protoLoader.Options;
  credentials?: GRPCServerCredentialsInput;
  serverOptions?: grpc.ServerOptions;
  listen?: Pick<
    GRPCListenOptions,
    "address" | "host" | "port" | "gracefulShutdownMs"
  >;
  hooks?: GRPCHooks;
}

export interface GRPCServerHandle {
  server: grpc.Server;
  address: string;
  host: string;
  port: number;
  services: string[];
  /** Health checker if health checks were enabled. */
  healthChecker?: GRPCHealthChecker;
  stop(gracefulShutdownMs?: number): Promise<void>;
  forceShutdown(): void;
}

export interface GRPCLoadedService {
  fullName: string;
  serviceDefinition: grpc.ServiceDefinition;
  clientConstructor: grpc.ServiceClientConstructor;
  packageDefinition: protoLoader.PackageDefinition;
  packageObject: grpc.GrpcObject;
}

interface GRPCRouteEntry {
  name: string;
  config: GRPCServiceConfig;
}

interface NormalizedMethodRoute {
  type: GRPCMethodKind;
  handler: GRPCAnyHandler;
  beforeCall?: GRPCHook;
  afterCall?: GRPCAfterHook;
  onError?: GRPCErrorHook;
  autoEnd: boolean;
}

type GRPCServerCallLike = {
  metadata: grpc.Metadata;
  getPeer(): string;
};

const DEFAULT_GRPC_HOST = "0.0.0.0";
const DEFAULT_GRPC_PORT = 50051;
const DEFAULT_GRACEFUL_SHUTDOWN_MS = 5000;

export const DEFAULT_GRPC_LOADER_OPTIONS: protoLoader.Options = {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
};

export function grpcUnary<Request = unknown, Response = unknown>(
  handler: GRPCUnaryHandler<Request, Response>,
  options: Omit<GRPCUnaryRoute<Request, Response>, "type" | "handler"> = {},
): GRPCUnaryRoute<Request, Response> {
  return { ...options, type: "unary", handler };
}

export function grpcServerStream<Request = unknown, Response = unknown>(
  handler: GRPCServerStreamHandler<Request, Response>,
  options: Omit<
    GRPCServerStreamRoute<Request, Response>,
    "type" | "handler"
  > = {},
): GRPCServerStreamRoute<Request, Response> {
  return { ...options, type: "serverStream", handler };
}

export function grpcClientStream<Request = unknown, Response = unknown>(
  handler: GRPCClientStreamHandler<Request, Response>,
  options: Omit<
    GRPCClientStreamRoute<Request, Response>,
    "type" | "handler"
  > = {},
): GRPCClientStreamRoute<Request, Response> {
  return { ...options, type: "clientStream", handler };
}

export function grpcBidi<Request = unknown, Response = unknown>(
  handler: GRPCBidiStreamHandler<Request, Response>,
  options: Omit<
    GRPCBidiStreamRoute<Request, Response>,
    "type" | "handler"
  > = {},
): GRPCBidiStreamRoute<Request, Response> {
  return { ...options, type: "bidi", handler };
}

export function createGRPCMetadata(init?: GRPCMetadataInit): grpc.Metadata {
  if (init instanceof grpc.Metadata) return init;

  const metadata = new grpc.Metadata();
  if (!init) return metadata;

  for (const [key, value] of Object.entries(init)) {
    if (Array.isArray(value)) {
      for (const item of value) metadata.add(key, item);
    } else {
      metadata.set(key, value);
    }
  }

  return metadata;
}

export function createGRPCError(
  code: grpc.status,
  details: string,
  metadata?: grpc.Metadata,
): grpc.ServiceError {
  const error = new Error(details) as grpc.ServiceError;
  error.code = code;
  error.details = details;
  error.metadata = metadata ?? new grpc.Metadata();
  return error;
}

export async function collectGRPCStream<T>(
  stream: AsyncIterable<T> | Iterable<T>,
): Promise<T[]> {
  const items: T[] = [];
  for await (const item of toAsyncIterable(stream)) {
    items.push(item);
  }
  return items;
}

export function writeGRPCMessage<T>(
  stream: {
    write(value: T): boolean;
    once(event: string, cb: () => void): unknown;
  },
  value: T,
): Promise<void> {
  if (stream.write(value)) return Promise.resolve();
  return new Promise((resolve) => {
    stream.once("drain", resolve);
  });
}

export function mergeGRPCLoaderOptions(
  ...options: Array<protoLoader.Options | undefined>
): protoLoader.Options {
  return Object.assign({}, DEFAULT_GRPC_LOADER_OPTIONS, ...options);
}

export function loadGRPCService(target: GRPCServiceTarget): GRPCLoadedService {
  const packageDefinition = protoLoader.loadSync(
    target.proto,
    mergeGRPCLoaderOptions(target.loader),
  );
  const packageObject = grpc.loadPackageDefinition(packageDefinition);
  const { value, fullName } = resolveNestedService(
    packageObject,
    target.package,
    target.service,
  );

  if (!value || typeof value !== "function" || !("service" in value)) {
    throw new Error(
      `[GRPCRouter] "${fullName}" is not a gRPC service. Check package/service names.`,
    );
  }

  return {
    fullName,
    serviceDefinition: value.service,
    clientConstructor: value,
    packageDefinition,
    packageObject,
  };
}

export function resolveGRPCServerCredentials(
  credentials?: GRPCServerCredentialsInput,
): grpc.ServerCredentials {
  if (!credentials || credentials === "insecure") {
    return grpc.ServerCredentials.createInsecure();
  }

  if (isGRPCServerCredentials(credentials)) {
    return credentials;
  }

  return grpc.ServerCredentials.createSsl(
    credentials.rootCerts ?? null,
    credentials.keyCertPairs,
    credentials.checkClientCertificate,
  );
}

export class GRPCRouter {
  public readonly name = "sinwan:grpc-router";

  private readonly routes = new Map<string, GRPCRouteEntry>();
  private readonly servers: GRPCServerHandle[] = [];
  private defaults: GRPCRouterDefaults;
  private propagationConfig?: GRPCPropagationConfig;

  constructor(defaults: GRPCRouterDefaults = {}) {
    this.defaults = defaults;
  }

  setDefaults(defaults: GRPCRouterDefaults): void {
    this.defaults = {
      ...this.defaults,
      ...defaults,
      loader: mergeGRPCLoaderOptions(this.defaults.loader, defaults.loader),
      listen: { ...this.defaults.listen, ...defaults.listen },
      hooks: { ...this.defaults.hooks, ...defaults.hooks },
      serverOptions: {
        ...this.defaults.serverOptions,
        ...defaults.serverOptions,
      },
    };
  }

  grpc(name: string, config: GRPCServiceConfig): void {
    if (!name || typeof name !== "string") {
      throw new TypeError(`[GRPCRouter.grpc] Name must be a non-empty string.`);
    }
    this.routes.set(name, { name, config });
  }

  hasRoutes(): boolean {
    return this.routes.size > 0;
  }

  async listen(
    runtime: Runtime,
    options?: GRPCListenOptions,
  ): Promise<GRPCServerHandle>;
  async listen(
    runtime: Runtime,
    name: string,
    options?: GRPCListenOptions,
  ): Promise<GRPCServerHandle>;
  async listen(
    runtime: Runtime,
    nameOrOptions?: string | GRPCListenOptions,
    maybeOptions?: GRPCListenOptions,
  ): Promise<GRPCServerHandle> {
    const selectedRoutes =
      typeof nameOrOptions === "string"
        ? [this.getRoute(nameOrOptions)]
        : Array.from(this.routes.values());
    const options =
      typeof nameOrOptions === "string" ? maybeOptions : nameOrOptions;

    if (selectedRoutes.length === 0) {
      throw new Error(`[GRPCRouter.listen] No gRPC services are registered.`);
    }

    const listenInterceptors = options?.interceptors;
    const allInterceptors = composeServerInterceptors(
      ...selectedRoutes.map((r) => r.config.interceptors),
      listenInterceptors,
    );

    const compressionOpts = resolveCompressionOptions(options?.compression);
    this.propagationConfig = options?.propagation;

    const server = new grpc.Server({
      ...this.defaults.serverOptions,
      ...compressionOpts,
      ...options?.serverOptions,
      ...(allInterceptors.length > 0 ? { interceptors: allInterceptors } : {}),
    });
    const serviceNames: string[] = [];

    for (const entry of selectedRoutes) {
      const loaded = loadGRPCService({
        proto: entry.config.proto,
        package: entry.config.package,
        service: entry.config.service,
        loader: mergeGRPCLoaderOptions(
          this.defaults.loader,
          entry.config.loader,
        ),
      });
      const implementation = this.createServiceImplementation(
        runtime,
        entry,
        loaded,
      );
      server.addService(loaded.serviceDefinition, implementation);
      serviceNames.push(loaded.fullName);
    }

    // Health check service
    let healthChecker: GRPCHealthChecker | undefined;
    const healthOpt = options?.health;
    if (healthOpt || options?.healthChecker) {
      healthChecker =
        options?.healthChecker ??
        new GRPCHealthChecker(typeof healthOpt === "object" ? healthOpt : {});
      const healthServiceDef = GRPCHealthChecker.loadServiceDefinition();
      server.addService(
        healthServiceDef,
        healthChecker.getServiceImplementation(),
      );
      for (const name of serviceNames) {
        healthChecker.setStatus(name, "SERVING");
      }
    }

    // Server reflection
    if (options?.reflection) {
      const protoPaths = selectedRoutes.map((r) => r.config.proto).flat();
      const reflectionService = createReflectionService(
        protoPaths,
        mergeGRPCLoaderOptions(this.defaults.loader),
      );
      addReflectionToServer(server, reflectionService);
    }

    const address = this.resolveAddress(options);
    const credentials = resolveGRPCServerCredentials(
      options?.credentials ?? this.defaults.credentials,
    );

    const boundPort = await new Promise<number>((resolve, reject) => {
      server.bindAsync(address, credentials, (error, port) => {
        if (error) {
          reject(
            new Error(
              `Failed to bind gRPC server on ${address}: ${error.message}`,
              { cause: error },
            ),
          );
          return;
        }
        resolve(port);
      });
    });

    const host =
      address.slice(0, address.lastIndexOf(":")) || DEFAULT_GRPC_HOST;
    const handle: GRPCServerHandle = {
      server,
      address,
      host,
      port: boundPort,
      services: serviceNames,
      healthChecker,
      stop: (gracefulShutdownMs?: number) => {
        healthChecker?.shutdown();
        return shutdownGRPCServer(
          server,
          gracefulShutdownMs ??
            options?.gracefulShutdownMs ??
            this.defaults.listen?.gracefulShutdownMs ??
            DEFAULT_GRACEFUL_SHUTDOWN_MS,
        );
      },
      forceShutdown: () => {
        healthChecker?.shutdown();
        server.forceShutdown();
      },
    };

    this.servers.push(handle);
    options?.ready?.(handle);
    return handle;
  }

  async stop(gracefulShutdownMs?: number): Promise<void> {
    const servers = this.servers.splice(0, this.servers.length);
    await Promise.all(
      servers.map((server) =>
        server.stop(
          gracefulShutdownMs ??
            this.defaults.listen?.gracefulShutdownMs ??
            DEFAULT_GRACEFUL_SHUTDOWN_MS,
        ),
      ),
    );
  }

  private getRoute(name: string): GRPCRouteEntry {
    const route = this.routes.get(name);
    if (!route) {
      throw new Error(`gRPC route "${name}" is not registered.`);
    }
    return route;
  }

  private resolveAddress(options?: GRPCListenOptions): string {
    if (options?.address) return options.address;
    if (this.defaults.listen?.address) return this.defaults.listen.address;

    const host =
      options?.host ?? this.defaults.listen?.host ?? DEFAULT_GRPC_HOST;
    const port =
      options?.port ?? this.defaults.listen?.port ?? DEFAULT_GRPC_PORT;
    return `${host}:${port}`;
  }

  private createServiceImplementation(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
  ): grpc.UntypedServiceImplementation {
    const serviceDefinition = loaded.serviceDefinition;
    const serviceMethods = Object.keys(serviceDefinition);
    const usedConfigKeys = new Set<string>();
    const implementation: grpc.UntypedServiceImplementation = {};

    for (const methodName of serviceMethods) {
      const definition = serviceDefinition[methodName];
      if (!definition) continue;

      const resolved = this.resolveMethodConfig(
        entry,
        methodName,
        definition,
        usedConfigKeys,
      );

      if (!resolved) {
        implementation[methodName] = this.createUnimplementedHandler(
          loaded.fullName,
          methodName,
          getMethodKind(definition),
        );
        continue;
      }

      implementation[methodName] = this.createMethodHandler(
        runtime,
        entry,
        loaded,
        methodName,
        definition,
        resolved,
      );
    }

    if (entry.config.strictMethods !== false) {
      const unknownMethods = Object.keys(entry.config.methods).filter(
        (method) => !usedConfigKeys.has(method),
      );
      if (unknownMethods.length > 0) {
        throw new Error(
          `[GRPCRouter] Unknown method(s) for ${loaded.fullName}: ${unknownMethods.join(
            ", ",
          )}. Available: ${serviceMethods.join(", ")}`,
        );
      }
    }

    return implementation;
  }

  private resolveMethodConfig(
    entry: GRPCRouteEntry,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    usedConfigKeys: Set<string>,
  ): NormalizedMethodRoute | null {
    const methods = entry.config.methods;
    const candidates = getMethodCandidates(methodName, definition);

    for (const candidate of candidates) {
      const config = methods[candidate];
      if (!config) continue;

      usedConfigKeys.add(candidate);
      return normalizeMethodRoute(entry.name, methodName, definition, config);
    }

    if (entry.config.strictMethods === false) return null;

    throw new Error(
      `[GRPCRouter] Missing handler for "${methodName}" in gRPC route "${entry.name}".`,
    );
  }

  private createMethodHandler(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
  ): grpc.UntypedHandleCall {
    if (route.type === "unary") {
      return (
        call: grpc.ServerUnaryCall<unknown, unknown>,
        callback: grpc.sendUnaryData<unknown>,
      ) => {
        this.handleUnary(
          runtime,
          entry,
          loaded,
          methodName,
          definition,
          route,
          call,
        )
          .then((response) => callback(null, response ?? {}))
          .catch((error) => callback(toGRPCError(error), null));
      };
    }

    if (route.type === "serverStream") {
      return (call: grpc.ServerWritableStream<unknown, unknown>) => {
        this.handleServerStream(
          runtime,
          entry,
          loaded,
          methodName,
          definition,
          route,
          call,
        ).catch((error) => call.destroy(toGRPCError(error)));
      };
    }

    if (route.type === "clientStream") {
      return (
        call: grpc.ServerReadableStream<unknown, unknown>,
        callback: grpc.sendUnaryData<unknown>,
      ) => {
        this.handleClientStream(
          runtime,
          entry,
          loaded,
          methodName,
          definition,
          route,
          call,
        )
          .then((response) => callback(null, response ?? {}))
          .catch((error) => callback(toGRPCError(error), null));
      };
    }

    return (call: grpc.ServerDuplexStream<unknown, unknown>) => {
      this.handleBidi(
        runtime,
        entry,
        loaded,
        methodName,
        definition,
        route,
        call,
      ).catch((error) => call.destroy(toGRPCError(error)));
    };
  }

  private async handleUnary(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
    call: grpc.ServerUnaryCall<unknown, unknown>,
  ): Promise<unknown> {
    return this.runWithContext(
      runtime,
      entry,
      loaded,
      methodName,
      definition,
      route,
      call,
      call.request,
      async (ctx) =>
        (route.handler as GRPCUnaryHandler)(ctx, call.request, call),
    );
  }

  private async handleServerStream(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
    call: grpc.ServerWritableStream<unknown, unknown>,
  ): Promise<void> {
    await this.runWithContext(
      runtime,
      entry,
      loaded,
      methodName,
      definition,
      route,
      call,
      call.request,
      async (ctx) => {
        const result = await (route.handler as GRPCServerStreamHandler)(
          ctx,
          call.request,
          call,
        );
        if (isIterableLike(result)) {
          await writeIterableToStream(call, result);
          call.end();
          return;
        }
        if (route.autoEnd && !isWritableEnded(call)) call.end();
      },
    );
  }

  private async handleClientStream(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
    call: grpc.ServerReadableStream<unknown, unknown>,
  ): Promise<unknown> {
    return this.runWithContext(
      runtime,
      entry,
      loaded,
      methodName,
      definition,
      route,
      call,
      undefined,
      async (ctx) => (route.handler as GRPCClientStreamHandler)(ctx, call),
    );
  }

  private async handleBidi(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
    call: grpc.ServerDuplexStream<unknown, unknown>,
  ): Promise<void> {
    await this.runWithContext(
      runtime,
      entry,
      loaded,
      methodName,
      definition,
      route,
      call,
      undefined,
      async (ctx) => {
        const result = await (route.handler as GRPCBidiStreamHandler)(
          ctx,
          call,
        );
        if (isIterableLike(result)) {
          await writeIterableToStream(call, result);
          call.end();
          return;
        }
        if (route.autoEnd && !isWritableEnded(call)) call.end();
      },
    );
  }

  private async runWithContext<T>(
    runtime: Runtime,
    entry: GRPCRouteEntry,
    loaded: GRPCLoadedService,
    methodName: string,
    definition: grpc.MethodDefinition<unknown, unknown>,
    route: NormalizedMethodRoute,
    call: GRPCServerCallLike,
    request: unknown,
    runHandler: (ctx: Context) => Promise<T> | T,
  ): Promise<T> {
    const ctx = runtime.acquireContext();
    const info: GRPCCallInfo = {
      name: entry.name,
      package: entry.config.package,
      service: loaded.fullName,
      method: methodName,
      path: definition.path,
      kind: route.type,
      request,
      metadata: call.metadata,
      peer: getPeer(call),
      data: entry.config.data ?? null,
    };

    ctx.setGRPC({
      name: entry.name,
      package: entry.config.package,
      service: loaded.fullName,
      method: methodName,
      path: definition.path,
      kind: route.type,
      request,
      call,
      metadata: call.metadata,
      data: entry.config.data ?? null,
    });

    const startedAt = performance.now();

    try {
      await this.emitCallStart(runtime, ctx, info);

      // A grpc:call listener may have rejected the call by setting a response
      // or calling ctx.stop(). Check both before proceeding to the hooks.
      if (ctx.hasResponded()) {
        throw responseToGRPCError(ctx);
      }
      if (ctx.isStopped()) {
        throw createGRPCError(
          grpc.status.PERMISSION_DENIED,
          "gRPC call stopped by a grpc:call listener.",
        );
      }

      await this.runBeforeHooks(ctx, info, entry, route);

      const runHandlerWithPropagation = (): Promise<T> => {
        if (this.propagationConfig) {
          const deadline = extractDeadlineFromMetadata(call.metadata);
          return Promise.resolve(
            runWithPropagationContext(
              { deadline, metadata: call.metadata },
              () => runHandler(ctx),
            ),
          );
        }
        return Promise.resolve(runHandler(ctx));
      };

      const result = await runHandlerWithPropagation();

      const durationMs = performance.now() - startedAt;
      await this.runAfterHooks(ctx, info, entry, route, durationMs);
      await this.emitCallFinish(runtime, ctx, { ...info, durationMs });
      return result;
    } catch (error) {
      throw await this.handleCallError(runtime, ctx, error, info, entry, route);
    } finally {
      ctx.dispose();
      runtime.releaseContext(ctx);
    }
  }

  private async emitCallStart(
    runtime: Runtime,
    ctx: Context,
    info: GRPCCallInfo,
  ): Promise<void> {
    if (!runtime.bus.hasListeners("grpc:call")) return;
    const result = await runtime.bus.emitAsync("grpc:call", ctx, info, {
      source: "grpc-router",
    });
    if (result === "STOP") ctx.stop();
  }

  private async runBeforeHooks(
    ctx: Context,
    info: GRPCCallInfo,
    entry: GRPCRouteEntry,
    route: NormalizedMethodRoute,
  ): Promise<void> {
    await this.defaults.hooks?.beforeCall?.(ctx, info);
    await entry.config.hooks?.beforeCall?.(ctx, info);
    await route.beforeCall?.(ctx, info);
  }

  private async runAfterHooks(
    ctx: Context,
    info: GRPCCallInfo,
    entry: GRPCRouteEntry,
    route: NormalizedMethodRoute,
    durationMs: number,
  ): Promise<void> {
    const payload = { ...info, durationMs };
    await route.afterCall?.(ctx, payload);
    await entry.config.hooks?.afterCall?.(ctx, payload);
    await this.defaults.hooks?.afterCall?.(ctx, payload);
  }

  private async emitCallFinish(
    runtime: Runtime,
    ctx: Context,
    info: GRPCCallInfo & { durationMs: number },
  ): Promise<void> {
    if (!runtime.bus.hasListeners("grpc:finish")) return;
    await runtime.bus.emitAsync("grpc:finish", ctx, info, {
      source: "grpc-router",
    });
  }

  private async handleCallError(
    runtime: Runtime,
    ctx: Context,
    error: unknown,
    info: GRPCCallInfo,
    entry: GRPCRouteEntry,
    route: NormalizedMethodRoute,
  ): Promise<grpc.ServiceError> {
    await runtime.errorNormalizer.report(error, ctx);

    if (runtime.bus.hasListeners("grpc:error")) {
      try {
        await runtime.bus.emitAsync(
          "grpc:error",
          ctx,
          { ...info, error },
          {
            source: "grpc-router",
          },
        );
      } catch {}
    }

    let override: unknown;
    try {
      override =
        (await route.onError?.(ctx, error, info)) ??
        (await entry.config.hooks?.onError?.(ctx, error, info)) ??
        (await this.defaults.hooks?.onError?.(ctx, error, info));
    } catch (hookError) {
      console.error("[sinwan:grpc] Unhandled error hook failure:", hookError);
    }

    return toGRPCError(override ?? error);
  }

  private createUnimplementedHandler(
    serviceName: string,
    methodName: string,
    kind: GRPCMethodKind,
  ): grpc.UntypedHandleCall {
    const error = createGRPCError(
      grpc.status.UNIMPLEMENTED,
      `Method ${serviceName}.${methodName} is not implemented.`,
    );

    if (kind === "unary" || kind === "clientStream") {
      return (_call: unknown, callback: grpc.sendUnaryData<unknown>) =>
        callback(error, null);
    }

    return (call: { destroy(error: unknown): void }) => call.destroy(error);
  }
}

function normalizeMethodRoute(
  routeName: string,
  methodName: string,
  definition: grpc.MethodDefinition<unknown, unknown>,
  config: GRPCMethodConfig,
): NormalizedMethodRoute {
  const inferredType = getMethodKind(definition);
  const route =
    typeof config === "function"
      ? ({ handler: config } as GRPCBaseMethodRoute)
      : config;
  const type = route.type ?? inferredType;

  if (type !== inferredType) {
    throw new Error(
      `[GRPCRouter] Method "${methodName}" in route "${routeName}" is "${inferredType}" in the proto, but was configured as "${type}".`,
    );
  }

  return {
    type,
    handler: route.handler,
    beforeCall: route.beforeCall,
    afterCall: route.afterCall,
    onError: route.onError,
    autoEnd: route.autoEnd !== false,
  };
}

function getMethodKind(
  definition: Pick<
    grpc.MethodDefinition<unknown, unknown>,
    "requestStream" | "responseStream"
  >,
): GRPCMethodKind {
  if (definition.requestStream && definition.responseStream) return "bidi";
  if (definition.requestStream) return "clientStream";
  if (definition.responseStream) return "serverStream";
  return "unary";
}

function getMethodCandidates(
  methodName: string,
  definition: grpc.MethodDefinition<unknown, unknown>,
): string[] {
  const candidates = [
    methodName,
    definition.originalName,
    lowerFirst(methodName),
    definition.originalName ? lowerFirst(definition.originalName) : undefined,
  ];
  return Array.from(new Set(candidates.filter(Boolean) as string[]));
}

function lowerFirst(value: string): string {
  if (value.length === 0) return value;
  return value.charAt(0).toLowerCase() + value.slice(1);
}

function resolveNestedService(
  packageObject: grpc.GrpcObject,
  packageName: string | undefined,
  serviceName: string,
): { value: grpc.ServiceClientConstructor | undefined; fullName: string } {
  const parts =
    packageName && !serviceName.includes(".")
      ? [...packageName.split("."), serviceName]
      : serviceName.split(".");
  let current: unknown = packageObject;

  for (const part of parts) {
    current = (current as Record<string, unknown> | undefined)?.[part];
    if (!current) {
      return { value: undefined, fullName: parts.join(".") };
    }
  }

  return {
    value: current as grpc.ServiceClientConstructor | undefined,
    fullName: parts.join("."),
  };
}

function isGRPCServerCredentials(
  value: unknown,
): value is grpc.ServerCredentials {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { _isSecure?: unknown })._isSecure === "function"
  );
}

function responseToGRPCError(ctx: Context): grpc.ServiceError {
  const body = ctx.body as
    | { message?: unknown; error?: unknown }
    | string
    | null;
  const message =
    typeof body === "string"
      ? body
      : typeof body?.message === "string"
        ? body.message
        : typeof body?.error === "string"
          ? body.error
          : "Sinwan middleware produced an HTTP response for a gRPC call.";

  return createGRPCError(httpStatusToGRPCStatus(ctx.statusCode), message);
}

function httpStatusToGRPCStatus(statusCode: number): grpc.status {
  if (statusCode === 400) return grpc.status.INVALID_ARGUMENT;
  if (statusCode === 401) return grpc.status.UNAUTHENTICATED;
  if (statusCode === 403) return grpc.status.PERMISSION_DENIED;
  if (statusCode === 404) return grpc.status.NOT_FOUND;
  if (statusCode === 409) return grpc.status.ABORTED;
  if (statusCode === 429) return grpc.status.RESOURCE_EXHAUSTED;
  if (statusCode === 499) return grpc.status.CANCELLED;
  if (statusCode === 501) return grpc.status.UNIMPLEMENTED;
  if (statusCode === 503) return grpc.status.UNAVAILABLE;
  if (statusCode === 504) return grpc.status.DEADLINE_EXCEEDED;
  return grpc.status.INTERNAL;
}

function toGRPCError(error: unknown): grpc.ServiceError {
  if (isServiceError(error)) return error;

  if (typeof error === "object" && error !== null) {
    const maybe = error as {
      details?: unknown;
      message?: unknown;
      code?: unknown;
      metadata?: unknown;
    };
    const details =
      typeof maybe.details === "string"
        ? maybe.details
        : typeof maybe.message === "string"
          ? maybe.message
          : "gRPC call failed.";
    const code =
      typeof maybe.code === "number" ? maybe.code : grpc.status.INTERNAL;
    const metadata =
      maybe.metadata instanceof grpc.Metadata ? maybe.metadata : undefined;
    return createGRPCError(code, details, metadata);
  }

  return createGRPCError(grpc.status.INTERNAL, String(error));
}

function isServiceError(error: unknown): error is grpc.ServiceError {
  return (
    error instanceof Error &&
    typeof (error as { code?: unknown }).code === "number" &&
    typeof (error as { details?: unknown }).details === "string"
  );
}

function getPeer(call: GRPCServerCallLike): string | undefined {
  try {
    return call.getPeer();
  } catch {
    return undefined;
  }
}

function isIterableLike<T>(
  value: unknown,
): value is Iterable<T> | AsyncIterable<T> {
  return (
    typeof value === "object" &&
    value !== null &&
    (Symbol.iterator in value || Symbol.asyncIterator in value)
  );
}

async function* toAsyncIterable<T>(
  iterable: Iterable<T> | AsyncIterable<T>,
): AsyncIterable<T> {
  if (Symbol.asyncIterator in iterable) {
    yield* iterable;
    return;
  }

  for (const item of iterable) {
    yield item;
  }
}

async function writeIterableToStream<T>(
  stream: {
    write(value: T): boolean;
    once(event: string, cb: () => void): unknown;
  },
  iterable: Iterable<T> | AsyncIterable<T>,
): Promise<void> {
  for await (const item of toAsyncIterable(iterable)) {
    await writeGRPCMessage(stream, item);
  }
}

function isWritableEnded(stream: unknown): boolean {
  const writable = stream as {
    writableEnded?: boolean;
    destroyed?: boolean;
    closed?: boolean;
  };
  return Boolean(
    writable.writableEnded || writable.destroyed || writable.closed,
  );
}

async function shutdownGRPCServer(
  server: grpc.Server,
  gracefulShutdownMs: number,
): Promise<void> {
  await new Promise<void>((resolve) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      server.forceShutdown();
      resolve();
    }, gracefulShutdownMs);

    server.tryShutdown(() => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    });
  });
}
