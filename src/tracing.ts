/**
 * SinwanJS gRPC — OpenTelemetry Tracing
 *
 * Provides server and client interceptors that create and propagate
 * OpenTelemetry spans for gRPC calls.
 */

import * as grpc from "@grpc/grpc-js";
import * as api from "@opentelemetry/api";

export interface GRPCTracingConfig {
  /** Tracer name. Default: "sinwan-grpc". */
  tracerName?: string;
  /** Span name prefix. Default: "grpc". */
  spanPrefix?: string;
  /** Additional attributes to set on every span. */
  attributes?: Record<string, string>;
}

const DEFAULT_TRACER_NAME = "sinwan-grpc";
const DEFAULT_SPAN_PREFIX = "grpc";

/**
 * Get the tracer from the global OpenTelemetry API.
 */
function getTracer(name: string): api.Tracer {
  return api.trace.getTracer(name);
}

/**
 * Create a server interceptor that traces incoming gRPC calls.
 */
export function createTracingServerInterceptor(
  config: GRPCTracingConfig = {},
): grpc.ServerInterceptor {
  const tracerName = config.tracerName ?? DEFAULT_TRACER_NAME;
  const spanPrefix = config.spanPrefix ?? DEFAULT_SPAN_PREFIX;
  const extraAttrs = config.attributes ?? {};

  return (methodDescriptor, call) => {
    const tracer = getTracer(tracerName);
    const spanName = `${spanPrefix}.server/${methodDescriptor.path}`;

    const span = tracer.startSpan(spanName, {
      kind: api.SpanKind.SERVER,
      attributes: {
        "rpc.system": "grpc",
        "rpc.method": methodDescriptor.path,
        ...extraAttrs,
      },
    });

    const responder = {
      start: (next: (listener?: grpc.ServerListener) => void) => {
        next({
          onReceiveMetadata: (
            metadata: grpc.Metadata,
            next: (metadata: grpc.Metadata) => void,
          ) => {
            // Extract trace context from metadata
            const context = api.propagation.extract(api.context.active(), {
              get: (key: string) => metadata.get(key).map(String),
              keys: () => Object.keys(metadata.getMap()),
            });
            api.context.with(context, () => {
              next(metadata);
            });
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
          onCancel: () => {
            span.setStatus({
              code: api.SpanStatusCode.ERROR,
              message: "cancelled",
            });
            span.end();
          },
        });
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
        if (status.code === grpc.status.OK) {
          span.setStatus({ code: api.SpanStatusCode.OK });
        } else {
          span.setStatus({
            code: api.SpanStatusCode.ERROR,
            message: status.details,
          });
          span.setAttribute("rpc.grpc.status_code", status.code);
        }
        span.end();
        next(status);
      },
    };

    return new grpc.ServerInterceptingCall(call, responder);
  };
}

/**
 * Create a client interceptor that traces outgoing gRPC calls
 * and injects trace context into metadata.
 */
export function createTracingClientInterceptor(
  config: GRPCTracingConfig = {},
): grpc.Interceptor {
  const tracerName = config.tracerName ?? DEFAULT_TRACER_NAME;
  const spanPrefix = config.spanPrefix ?? DEFAULT_SPAN_PREFIX;
  const extraAttrs = config.attributes ?? {};

  return (options, nextCall) => {
    const tracer = getTracer(tracerName);
    const methodPath =
      (options as unknown as { method_definition?: { path?: string } })
        .method_definition?.path ?? "unknown";
    const spanName = `${spanPrefix}.client/${methodPath}`;

    const span = tracer.startSpan(spanName, {
      kind: api.SpanKind.CLIENT,
      attributes: {
        "rpc.system": "grpc",
        "rpc.method": methodPath,
        ...extraAttrs,
      },
    });

    return new grpc.InterceptingCall(nextCall(options), {
      start: (
        metadata: grpc.Metadata,
        listener: grpc.Listener,
        next: (metadata: grpc.Metadata, listener: grpc.Listener) => void,
      ) => {
        // Inject trace context into metadata
        api.context.with(api.trace.setSpan(api.context.active(), span), () => {
          api.propagation.inject(api.context.active(), {
            set: (key: string, value: string) => metadata.set(key, value),
          });
        });

        // Wrap listener to intercept onReceiveStatus
        const wrappedListener: grpc.Listener = {
          onReceiveMetadata: listener.onReceiveMetadata
            ? (md: grpc.Metadata, next: (md: grpc.Metadata) => void) => next(md)
            : undefined,
          onReceiveMessage: listener.onReceiveMessage
            ? (msg: unknown, next: (msg: unknown) => void) => next(msg)
            : undefined,
          onReceiveStatus: (
            status: grpc.StatusObject,
            next: (status: grpc.StatusObject) => void,
          ) => {
            if (status.code === grpc.status.OK) {
              span.setStatus({ code: api.SpanStatusCode.OK });
            } else {
              span.setStatus({
                code: api.SpanStatusCode.ERROR,
                message: status.details,
              });
              span.setAttribute("rpc.grpc.status_code", status.code);
            }
            span.end();
            next(status);
          },
        };

        next(metadata, wrappedListener);
      },
    });
  };
}
