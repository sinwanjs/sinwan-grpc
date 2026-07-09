/**
 * SinwanJS Core Runtime - GRPCClient
 *
 * A small typed client helper for services loaded with @grpc/proto-loader.
 */

import * as grpc from "@grpc/grpc-js";
import {
  createGRPCMetadata,
  loadGRPCService,
  mergeGRPCLoaderOptions,
  type GRPCMetadataInit,
  type GRPCProtoPath,
} from "./server";
import {
  type GRPCClientInterceptor,
  composeClientInterceptors,
} from "./interceptors";
import {
  makeAbortablePromise,
  attachAbortSignal,
  type GRPCAbortablePromise,
} from "./cancellation";
import {
  type GRPCCompressionConfig,
  resolveCompressionOptions,
} from "./compression";
import {
  type GRPCPropagationConfig,
  createPropagationInterceptor,
} from "./propagation";

export type GRPCClientCredentialsInput =
  | grpc.ChannelCredentials
  | "insecure"
  | {
      rootCerts?: Buffer | null;
      privateKey?: Buffer | null;
      certChain?: Buffer | null;
      verifyOptions?: grpc.VerifyOptions;
    };

export interface GRPCReconnectOptions {
  /** Enable automatic reconnection at the channel level. Default: true. */
  enabled?: boolean;
  /** Initial backoff in ms before the first reconnect attempt. Default: 1000. */
  initialBackoffMs?: number;
  /** Maximum backoff in ms between reconnect attempts. Default: 30000. */
  maxBackoffMs?: number;
  /** Minimum time in ms between reconnect attempts. Default: 1000. */
  minBackoffMs?: number;
}

export type GRPCConnectionState =
  | "idle"
  | "connecting"
  | "ready"
  | "transient-failure"
  | "shutdown";

export type GRPCConnectionListener = (state: GRPCConnectionState) => void;

export interface GRPCClientConfig {
  /** .proto file path or paths. */
  proto: GRPCProtoPath;
  /** Proto package name, e.g. "users.v1". Optional if service is fully qualified. */
  package?: string;
  /** Service name, e.g. "UserService" or "users.v1.UserService". */
  service: string;
  /** Host:port target, e.g. "localhost:50051". */
  address: string;
  /** Client credentials. Default: insecure. */
  credentials?: GRPCClientCredentialsInput;
  /** Proto-loader options. Merged with Sinwan defaults. */
  loader?: import("@grpc/proto-loader").Options;
  /** grpc-js client options. */
  options?: grpc.ClientOptions;
  /** Automatic reconnection settings. */
  reconnect?: GRPCReconnectOptions;
  /** Called whenever the channel connectivity state changes. */
  onConnectionStateChange?: GRPCConnectionListener;
  /** Client interceptors applied to all calls. */
  interceptors?: GRPCClientInterceptor[];
  /** Compression configuration for the client. */
  compression?: GRPCCompressionConfig;
  /** Deadline and metadata propagation settings. */
  propagation?: GRPCPropagationConfig;
}

export interface GRPCCallOptions {
  metadata?: GRPCMetadataInit;
  options?: grpc.CallOptions;
  /** Per-call client interceptors. Merged with client-level interceptors. */
  interceptors?: GRPCClientInterceptor[];
  /** AbortSignal that cancels the call when fired. */
  signal?: AbortSignal;
}

export interface GRPCClientStreamCall<Request = unknown, Response = unknown> {
  stream: grpc.ClientWritableStream<Request>;
  response: Promise<Response>;
}

export class GRPCClient<
  ServiceShape extends Record<string, (...args: unknown[]) => unknown> = Record<
    string,
    (...args: unknown[]) => unknown
  >,
> {
  public readonly client: grpc.Client & ServiceShape;
  public readonly serviceName: string;
  public readonly address: string;
  private readonly serviceDefinition: grpc.ServiceDefinition;
  private readonly reconnectOptions: GRPCReconnectOptions;
  private readonly connectionListener?: GRPCConnectionListener;
  private readonly clientInterceptors: GRPCClientInterceptor[];
  private connectionMonitorActive = false;

  constructor(config: GRPCClientConfig) {
    const loaded = loadGRPCService({
      proto: config.proto,
      package: config.package,
      service: config.service,
      loader: mergeGRPCLoaderOptions(config.loader),
    });

    this.serviceName = loaded.fullName;
    this.address = config.address;
    this.serviceDefinition = loaded.serviceDefinition;
    this.reconnectOptions = resolveReconnectOptions(config.reconnect);
    this.connectionListener = config.onConnectionStateChange;
    this.clientInterceptors = config.interceptors ?? [];

    // Add propagation interceptor if propagation is configured
    if (config.propagation) {
      this.clientInterceptors = [
        ...this.clientInterceptors,
        createPropagationInterceptor(config.propagation),
      ];
    }

    const mergedOptions = mergeClientOptions(
      config.options,
      this.reconnectOptions,
      this.clientInterceptors,
      config.compression,
    );

    this.client = new loaded.clientConstructor(
      config.address,
      resolveGRPCClientCredentials(config.credentials),
      mergedOptions,
    ) as grpc.Client & ServiceShape;

    if (this.connectionListener) {
      this.startConnectionMonitor();
    }
  }

  static create<
    ServiceShape extends Record<string, (...args: unknown[]) => unknown> =
      Record<string, (...args: unknown[]) => unknown>,
  >(config: GRPCClientConfig): GRPCClient<ServiceShape> {
    return new GRPCClient<ServiceShape>(config);
  }

  unary<Request = unknown, Response = unknown>(
    method: string,
    request: Request,
    callOptions: GRPCCallOptions = {},
  ): GRPCAbortablePromise<Response> {
    const methodName = this.resolveMethodName(method, "unary");
    const fn = this.getMethod(methodName);
    const options = this.resolveCallOptions(callOptions);

    let callRef: grpc.ClientUnaryCall | undefined;
    const promise = new Promise<Response>((resolve, reject) => {
      const callback = (error: grpc.ServiceError | null, value?: Response) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(value as Response);
      };

      callRef = invokeUnary(fn, request, { ...callOptions, options }, callback);
      attachAbortSignal(callOptions.signal, callRef);
    });

    return makeAbortablePromise(promise, {
      cancel: () => callRef?.cancel(),
    });
  }

  serverStream<Request = unknown, Response = unknown>(
    method: string,
    request: Request,
    callOptions: GRPCCallOptions = {},
  ): grpc.ClientReadableStream<Response> {
    const methodName = this.resolveMethodName(method, "serverStream");
    const fn = this.getMethod(methodName);
    const metadata = createGRPCMetadata(callOptions.metadata);
    const options = this.resolveCallOptions(callOptions);

    const stream = options
      ? (fn(request, metadata, options) as grpc.ClientReadableStream<Response>)
      : (fn(request, metadata) as grpc.ClientReadableStream<Response>);
    attachAbortSignal(callOptions.signal, stream);
    return stream;
  }

  clientStream<Request = unknown, Response = unknown>(
    method: string,
    callOptions: GRPCCallOptions = {},
  ): GRPCClientStreamCall<Request, Response> {
    const methodName = this.resolveMethodName(method, "clientStream");
    const fn = this.getMethod(methodName);
    const options = this.resolveCallOptions(callOptions);

    let stream!: grpc.ClientWritableStream<Request>;
    const response = new Promise<Response>((resolve, reject) => {
      const callback = (error: grpc.ServiceError | null, value?: Response) => {
        if (error) {
          reject(error);
          return;
        }
        resolve(value as Response);
      };

      const metadata = createGRPCMetadata(callOptions.metadata);
      stream = options
        ? (fn(
            metadata,
            options,
            callback,
          ) as grpc.ClientWritableStream<Request>)
        : (fn(metadata, callback) as grpc.ClientWritableStream<Request>);
      attachAbortSignal(callOptions.signal, stream);
    });

    return { stream, response };
  }

  bidi<Request = unknown, Response = unknown>(
    method: string,
    callOptions: GRPCCallOptions = {},
  ): grpc.ClientDuplexStream<Request, Response> {
    const methodName = this.resolveMethodName(method, "bidi");
    const fn = this.getMethod(methodName);
    const metadata = createGRPCMetadata(callOptions.metadata);
    const options = this.resolveCallOptions(callOptions);

    const stream = options
      ? (fn(metadata, options) as grpc.ClientDuplexStream<Request, Response>)
      : (fn(metadata) as grpc.ClientDuplexStream<Request, Response>);
    attachAbortSignal(callOptions.signal, stream);
    return stream;
  }

  waitForReady(deadline: Date | number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.client.waitForReady(deadline, (error?: Error) => {
        if (error) reject(error);
        else resolve();
      });
    });
  }

  /** Current channel connectivity state. */
  getConnectionState(): GRPCConnectionState {
    return toConnectionState(
      this.client.getChannel().getConnectivityState(true),
    );
  }

  /** Force the channel to reconnect immediately. */
  reconnect(): void {
    const channel = this.client.getChannel();
    channel.getConnectivityState(true);
  }

  /** Start polling for connection state changes and notify the listener. */
  startConnectionMonitor(): void {
    if (this.connectionMonitorActive || !this.connectionListener) return;
    this.connectionMonitorActive = true;
    this.pollConnectionState();
  }

  /** Stop polling for connection state changes. */
  stopConnectionMonitor(): void {
    this.connectionMonitorActive = false;
  }

  private pollConnectionState(): void {
    if (!this.connectionMonitorActive || !this.connectionListener) return;

    const channel = this.client.getChannel();
    const currentState = channel.getConnectivityState(true);

    this.connectionListener(toConnectionState(currentState));

    channel.watchConnectivityState(currentState, Infinity, (error) => {
      if (error || !this.connectionMonitorActive) return;
      this.pollConnectionState();
    });
  }

  close(): void {
    this.stopConnectionMonitor();
    this.client.close();
  }

  private getMethod(methodName: string): (...args: unknown[]) => unknown {
    const fn = (this.client as Record<string, unknown>)[methodName];
    if (typeof fn !== "function") {
      throw new Error(
        `[GRPCClient] Method "${methodName}" is not available on ${this.serviceName}.`,
      );
    }
    return fn.bind(this.client);
  }

  private resolveCallOptions(
    callOptions: GRPCCallOptions,
  ): grpc.CallOptions | undefined {
    const perCallInterceptors = composeClientInterceptors(
      this.clientInterceptors,
      callOptions.interceptors,
    );
    if (!callOptions.options && perCallInterceptors.length === 0)
      return undefined;
    return {
      ...callOptions.options,
      ...(perCallInterceptors.length > 0
        ? { interceptors: perCallInterceptors }
        : {}),
    };
  }

  private resolveMethodName(
    method: string,
    expectedKind: GRPCClientMethodKind,
  ): string {
    const candidates = getClientMethodCandidates(method);

    for (const candidate of candidates) {
      const definition = this.serviceDefinition[candidate];
      if (!definition) continue;

      const actualKind = getClientMethodKind(definition);
      if (actualKind !== expectedKind) {
        throw new Error(
          `[GRPCClient] Method "${candidate}" is "${actualKind}", not "${expectedKind}".`,
        );
      }

      return candidate;
    }

    throw new Error(
      `[GRPCClient] Unknown method "${method}" on ${this.serviceName}. Available: ${Object.keys(
        this.serviceDefinition,
      ).join(", ")}`,
    );
  }
}

export function createGRPCClient<
  ServiceShape extends Record<string, (...args: unknown[]) => unknown> = Record<
    string,
    (...args: unknown[]) => unknown
  >,
>(config: GRPCClientConfig): GRPCClient<ServiceShape> {
  return new GRPCClient<ServiceShape>(config);
}

export function resolveGRPCClientCredentials(
  credentials?: GRPCClientCredentialsInput,
): grpc.ChannelCredentials {
  if (!credentials || credentials === "insecure") {
    return grpc.credentials.createInsecure();
  }

  if (isGRPCChannelCredentials(credentials)) {
    return credentials;
  }

  return grpc.credentials.createSsl(
    credentials.rootCerts ?? null,
    credentials.privateKey ?? null,
    credentials.certChain ?? null,
    credentials.verifyOptions,
  );
}

type GRPCClientMethodKind = "unary" | "serverStream" | "clientStream" | "bidi";

function invokeUnary<Request, Response>(
  fn: (...args: unknown[]) => unknown,
  request: Request,
  callOptions: GRPCCallOptions,
  callback: grpc.requestCallback<Response>,
): grpc.ClientUnaryCall {
  const metadata = createGRPCMetadata(callOptions.metadata);

  return callOptions.options
    ? (fn(
        request,
        metadata,
        callOptions.options,
        callback,
      ) as grpc.ClientUnaryCall)
    : (fn(request, metadata, callback) as grpc.ClientUnaryCall);
}

function getClientMethodKind(
  definition: Pick<
    grpc.MethodDefinition<unknown, unknown>,
    "requestStream" | "responseStream"
  >,
): GRPCClientMethodKind {
  if (definition.requestStream && definition.responseStream) return "bidi";
  if (definition.requestStream) return "clientStream";
  if (definition.responseStream) return "serverStream";
  return "unary";
}

function getClientMethodCandidates(method: string): string[] {
  return Array.from(new Set([method, lowerFirst(method)]));
}

function lowerFirst(value: string): string {
  if (value.length === 0) return value;
  return value.charAt(0).toLowerCase() + value.slice(1);
}

function isGRPCChannelCredentials(
  value: unknown,
): value is grpc.ChannelCredentials {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { _isSecure?: unknown })._isSecure === "function"
  );
}

// ─── Reconnect helpers ────────────────────────────────────────

const DEFAULT_RECONNECT: Required<GRPCReconnectOptions> = {
  enabled: true,
  initialBackoffMs: 1000,
  maxBackoffMs: 30000,
  minBackoffMs: 1000,
};

function resolveReconnectOptions(
  options?: GRPCReconnectOptions,
): GRPCReconnectOptions {
  if (!options) return { ...DEFAULT_RECONNECT };
  return {
    enabled: options.enabled ?? DEFAULT_RECONNECT.enabled,
    initialBackoffMs:
      options.initialBackoffMs ?? DEFAULT_RECONNECT.initialBackoffMs,
    maxBackoffMs: options.maxBackoffMs ?? DEFAULT_RECONNECT.maxBackoffMs,
    minBackoffMs: options.minBackoffMs ?? DEFAULT_RECONNECT.minBackoffMs,
  };
}

function mergeClientOptions(
  base: grpc.ClientOptions | undefined,
  reconnect: GRPCReconnectOptions,
  interceptors: GRPCClientInterceptor[] = [],
  compression?: GRPCCompressionConfig,
): grpc.ClientOptions {
  const reconnectChannelOptions: Record<string, unknown> = {};

  if (reconnect.enabled !== false) {
    reconnectChannelOptions["grpc.initial_reconnect_backoff_ms"] =
      reconnect.initialBackoffMs;
    reconnectChannelOptions["grpc.max_reconnect_backoff_ms"] =
      reconnect.maxBackoffMs;
    reconnectChannelOptions["grpc.min_reconnect_backoff_ms"] =
      reconnect.minBackoffMs;
  }

  const compressionOpts = resolveCompressionOptions(compression);

  return {
    ...base,
    ...compressionOpts,
    ...reconnectChannelOptions,
    ...(interceptors.length > 0
      ? {
          interceptors: composeClientInterceptors(
            base?.interceptors,
            interceptors,
          ),
        }
      : {}),
  };
}

function toConnectionState(state: grpc.connectivityState): GRPCConnectionState {
  switch (state) {
    case grpc.connectivityState.IDLE:
      return "idle";
    case grpc.connectivityState.CONNECTING:
      return "connecting";
    case grpc.connectivityState.READY:
      return "ready";
    case grpc.connectivityState.TRANSIENT_FAILURE:
      return "transient-failure";
    case grpc.connectivityState.SHUTDOWN:
      return "shutdown";
    default:
      return "idle";
  }
}
