/**
 * SinwanJS gRPC — Interceptors
 *
 * First-class interceptor support for both server and client sides.
 * grpc-js already supports interceptors via ServerOptions.interceptors
 * and ClientOptions.interceptors — this module provides typed helpers,
 * composition utilities, and integration points in GRPCRouter / GRPCClient.
 */

import * as grpc from "@grpc/grpc-js";

export type GRPCServerInterceptor = grpc.ServerInterceptor;
export type GRPCClientInterceptor = grpc.Interceptor;

/**
 * Compose multiple interceptor arrays into a single array.
 * Earlier arrays take precedence (run first).
 */
export function composeServerInterceptors(
  ...arrays: Array<GRPCServerInterceptor[] | undefined>
): GRPCServerInterceptor[] {
  return arrays.filter(Boolean).flat() as GRPCServerInterceptor[];
}

/**
 * Compose multiple client interceptor arrays into a single array.
 * Earlier arrays take precedence (run first).
 */
export function composeClientInterceptors(
  ...arrays: Array<GRPCClientInterceptor[] | undefined>
): GRPCClientInterceptor[] {
  return arrays.filter(Boolean).flat() as GRPCClientInterceptor[];
}

/**
 * Create a simple logging interceptor that records method name and duration.
 * Useful for debugging and as an example.
 */
export function createLoggingInterceptor(
  log: (method: string, durationMs: number) => void,
): GRPCServerInterceptor {
  return (methodDescriptor, call) => {
    const start = performance.now();
    const listener = {
      onReceiveMetadata: (
        metadata: grpc.Metadata,
        next: (metadata: grpc.Metadata) => void,
      ) => {
        next(metadata);
      },
      onReceiveMessage: (
        message: unknown,
        next: (message: unknown) => void,
      ) => {
        next(message);
      },
      onReceiveHalfClose: (next: () => void) => {
        next();
      },
      onCancel: () => {},
    };

    const responder = {
      start: (next: (listener?: grpc.ServerListener) => void) => {
        next(listener);
      },
      sendMetadata: (
        metadata: grpc.Metadata,
        next: (metadata: grpc.Metadata) => void,
      ) => {
        next(metadata);
      },
      sendMessage: (message: unknown, next: (message: unknown) => void) => {
        next(message);
      },
      sendStatus: (
        status: {
          code: number;
          details: string;
          metadata?: grpc.Metadata | null;
        },
        next: (status: {
          code: number;
          details: string;
          metadata?: grpc.Metadata | null;
        }) => void,
      ) => {
        const durationMs = performance.now() - start;
        log(methodDescriptor.path, durationMs);
        next(status);
      },
    };

    return new grpc.ServerInterceptingCall(call, responder);
  };
}

/**
 * Create a metadata-injecting server interceptor.
 * Adds the provided key-value pairs to incoming metadata.
 */
export function createMetadataInjectionInterceptor(
  entries: Record<string, string>,
): GRPCServerInterceptor {
  return (_methodDescriptor, call) => {
    const listener = {
      onReceiveMetadata: (
        metadata: grpc.Metadata,
        next: (metadata: grpc.Metadata) => void,
      ) => {
        for (const [key, value] of Object.entries(entries)) {
          metadata.set(key, value);
        }
        next(metadata);
      },
      onReceiveMessage: (
        message: unknown,
        next: (message: unknown) => void,
      ) => {
        next(message);
      },
      onReceiveHalfClose: (next: () => void) => {
        next();
      },
      onCancel: () => {},
    };

    return new grpc.ServerInterceptingCall(call, {
      start: (next: (listener?: grpc.ServerListener) => void) => {
        next(listener);
      },
    });
  };
}
