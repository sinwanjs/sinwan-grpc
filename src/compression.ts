/**
 * SinwanJS gRPC — Compression
 *
 * First-class compression configuration for both server and client.
 * Maps to grpc-js channel/server options (grpc.default_compression_algorithm).
 */

import * as grpc from "@grpc/grpc-js";

export type GRPCCompressionAlgorithm = "identity" | "deflate" | "gzip";

export interface GRPCCompressionConfig {
  /** Compression algorithm. Default: "identity" (no compression). */
  algorithm?: GRPCCompressionAlgorithm;
  /**
   * Minimum message size (bytes) before compression is applied.
   * Maps to grpc.default_compression_level via min size heuristic.
   * Only effective when algorithm is not "identity".
   */
  minSize?: number;
}

const ALGORITHM_MAP: Record<GRPCCompressionAlgorithm, number> = {
  identity: grpc.compressionAlgorithms.identity,
  deflate: grpc.compressionAlgorithms.deflate,
  gzip: grpc.compressionAlgorithms.gzip,
};

/**
 * Convert a GRPCCompressionConfig into grpc-js server/channel options.
 */
export function resolveCompressionOptions(
  config?: GRPCCompressionConfig,
): Record<string, unknown> {
  if (!config || !config.algorithm || config.algorithm === "identity") {
    return {};
  }

  const options: Record<string, unknown> = {
    "grpc.default_compression_algorithm": ALGORITHM_MAP[config.algorithm],
  };

  if (config.minSize !== undefined) {
    options["grpc.min_message_size_to_compress"] = config.minSize;
  }

  return options;
}

/**
 * Get the numeric CompressionAlgorithms value for a named algorithm.
 */
export function getCompressionAlgorithmValue(
  algorithm: GRPCCompressionAlgorithm,
): number {
  return ALGORITHM_MAP[algorithm];
}
