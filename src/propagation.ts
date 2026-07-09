/**
 * SinwanJS gRPC — Deadline & Metadata Propagation
 *
 * Provides automatic propagation of deadline and metadata from
 * incoming gRPC calls to outgoing gRPC calls made within the same
 * request context. Uses AsyncLocalStorage to carry context across
 * async boundaries.
 */

import * as grpc from "@grpc/grpc-js";
import { AsyncLocalStorage } from "node:async_hooks";

export interface GRPCPropagationContext {
  /** Absolute deadline timestamp (ms since epoch). */
  deadline?: number;
  /** Incoming metadata to propagate. */
  metadata?: grpc.Metadata;
}

export interface GRPCPropagationConfig {
  /** Enable deadline propagation. Default: false. */
  deadline?: boolean;
  /**
   * Metadata keys to propagate. `true` propagates all keys.
   * A string array propagates only the listed keys.
   */
  metadata?: string[] | true;
}

const propagationStorage = new AsyncLocalStorage<GRPCPropagationContext>();

/**
 * Run a function within a propagation context.
 * Used by the server to establish the context for downstream calls.
 */
export function runWithPropagationContext<T>(
  context: GRPCPropagationContext,
  fn: () => T,
): T {
  return propagationStorage.run(context, fn);
}

/**
 * Get the current propagation context, if any.
 */
export function getPropagationContext(): GRPCPropagationContext | undefined {
  return propagationStorage.getStore();
}

/**
 * Create a client interceptor that propagates deadline and metadata
 * from the current AsyncLocalStorage context to outgoing calls.
 */
export function createPropagationInterceptor(
  config: GRPCPropagationConfig,
): grpc.Interceptor {
  return (options, nextCall) => {
    const ctx = propagationStorage.getStore();

    if (!ctx) {
      return new grpc.InterceptingCall(nextCall(options));
    }

    // Determine propagated deadline
    let deadline: number | undefined;
    if (config.deadline && ctx.deadline) {
      const remaining = ctx.deadline - Date.now();
      if (remaining > 0) {
        deadline = ctx.deadline;
      }
    }

    // Collect metadata keys to propagate
    const propagateEntries: Array<[string, string | Buffer]> = [];
    if (config.metadata && ctx.metadata) {
      if (config.metadata === true) {
        for (const [key, value] of Object.entries(ctx.metadata.getMap())) {
          if (Array.isArray(value)) {
            for (const v of value)
              propagateEntries.push([key, v as string | Buffer]);
          } else if (value !== undefined) {
            propagateEntries.push([key, value as string | Buffer]);
          }
        }
      } else {
        for (const key of config.metadata) {
          const values = ctx.metadata.get(key);
          for (const value of values) {
            propagateEntries.push([key, value as string | Buffer]);
          }
        }
      }
    }

    const callOptions = deadline ? { ...options, deadline } : options;

    return new grpc.InterceptingCall(nextCall(callOptions), {
      start: (
        metadata: grpc.Metadata,
        listener: grpc.Listener,
        next: (metadata: grpc.Metadata, listener: grpc.Listener) => void,
      ) => {
        for (const [key, value] of propagateEntries) {
          metadata.add(key, value);
        }
        next(metadata, listener);
      },
    });
  };
}

/**
 * Extract the deadline from a gRPC call's metadata.
 * The `grpc-timeout` header encodes the timeout in a special format.
 */
export function extractDeadlineFromMetadata(
  metadata: grpc.Metadata,
): number | undefined {
  const timeout = metadata.get("grpc-timeout");
  if (timeout.length === 0) return undefined;

  const value = timeout[0];
  if (typeof value !== "string") return undefined;

  // Format: <value><unit> where unit is one of: H, M, S, m, u, n
  const match = /^(\d+)([HMSmun])$/.exec(value);
  if (!match) return undefined;

  const num = parseInt(match[1]!, 10);
  const unit = match[2] as string;

  const multipliers: Record<string, number> = {
    H: 3_600_000,
    M: 60_000,
    S: 1_000,
    m: 1,
    u: 0.001,
    n: 0.000_001,
  };

  const timeoutMs = num * (multipliers[unit] ?? 0);
  return Date.now() + timeoutMs;
}
