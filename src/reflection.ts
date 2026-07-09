/**
 * SinwanJS gRPC — Server Reflection
 *
 * Integrates @grpc/reflection to provide gRPC server reflection
 * (grpc.reflection.v1alpha.ServerReflection), enabling clients like
 * grpcurl to discover services and proto definitions at runtime.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { ReflectionService } from "@grpc/reflection";

export interface GRPCReflectionOptions {
  /** Reflection protocol version. Default: "v1alpha". */
  version?: "v1" | "v1alpha";
}

/**
 * Create a ReflectionService from one or more proto file paths.
 * Loads the proto definitions and wraps them in a ReflectionService.
 */
export function createReflectionService(
  protoPaths: string | string[],
  loaderOptions?: protoLoader.Options,
  options?: GRPCReflectionOptions,
): ReflectionService {
  const paths = Array.isArray(protoPaths) ? protoPaths : [protoPaths];
  const packageDefinition = protoLoader.loadSync(paths, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    ...loaderOptions,
  });

  return new ReflectionService(packageDefinition);
}

/**
 * Add reflection service to a grpc server.
 */
export function addReflectionToServer(
  server: Pick<grpc.Server, "addService">,
  reflectionService: ReflectionService,
): void {
  reflectionService.addToServer(server);
}
