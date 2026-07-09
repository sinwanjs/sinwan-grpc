/**
 * SinwanJS gRPC — Load Balancing Policies
 *
 * Provides typed configuration for gRPC client-side load balancing.
 * Maps to grpc-js channel options (grpc.lb_policy_name).
 */

export type GRPCLoadBalancingPolicy = "pick_first" | "round_robin" | "grpclb";

export interface GRPCLoadBalancingConfig {
  /** Load balancing policy. Default: "pick_first". */
  policy?: GRPCLoadBalancingPolicy;
  /**
   * For "grpclb" policy, the fallback policy to use when no gRPC LB server is available.
   * Default: "round_robin".
   */
  fallbackPolicy?: GRPCLoadBalancingPolicy;
}

/**
 * Convert a GRPCLoadBalancingConfig into grpc-js channel options.
 */
export function resolveLoadBalancingOptions(
  config?: GRPCLoadBalancingConfig,
): Record<string, unknown> {
  if (!config || !config.policy) return {};

  const options: Record<string, unknown> = {
    "grpc.lb_policy_name": config.policy,
  };

  if (config.fallbackPolicy) {
    options["grpc.grpclb_fallback_timeout_ms"] = 10_000;
  }

  return options;
}

/**
 * Format a comma-separated list of addresses for use with round_robin/grpclb.
 * e.g. ["localhost:50051", "localhost:50052"] -> "dns:///localhost:50051,localhost:50052"
 */
export function formatLoadBalancingTarget(
  addresses: string[],
  policy: GRPCLoadBalancingPolicy = "round_robin",
): string {
  if (policy === "pick_first" || addresses.length === 1) {
    return addresses[0]!;
  }

  // For round_robin and grpclb, use dns:/// prefix with comma-separated addresses
  return `dns:///${addresses.join(",")}`;
}
