/**
 * SinwanJS gRPC — Health Check Service (grpc.health.v1.Health)
 *
 * Implements the standard gRPC health check protocol so that
 * load balancers, service meshes, and orchestrators can probe
 * the health of individual gRPC services or the overall server.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HEALTH_PROTO = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "health.proto",
);

export type HealthServingStatus =
  | "UNKNOWN"
  | "SERVING"
  | "NOT_SERVING"
  | "SERVICE_UNKNOWN";

const STATUS_MAP: Record<HealthServingStatus, number> = {
  UNKNOWN: 0,
  SERVING: 1,
  NOT_SERVING: 2,
  SERVICE_UNKNOWN: 3,
};

const STATUS_NAME_MAP: Record<HealthServingStatus, string> = {
  UNKNOWN: "UNKNOWN",
  SERVING: "SERVING",
  NOT_SERVING: "NOT_SERVING",
  SERVICE_UNKNOWN: "SERVICE_UNKNOWN",
};

const STATUS_REVERSE_MAP: Record<number, HealthServingStatus> = {
  0: "UNKNOWN",
  1: "SERVING",
  2: "NOT_SERVING",
  3: "SERVICE_UNKNOWN",
};

export interface GRPCHealthOptions {
  /** Initial status for the overall server (service name ""). Default: SERVING. */
  initialStatus?: HealthServingStatus;
}

export class GRPCHealthChecker {
  private readonly statuses = new Map<string, HealthServingStatus>();
  private readonly watchers = new Map<
    string,
    Set<(status: HealthServingStatus) => void>
  >();
  private shutDown = false;

  constructor(options: GRPCHealthOptions = {}) {
    this.statuses.set("", options.initialStatus ?? "SERVING");
  }

  setStatus(service: string, status: HealthServingStatus): void {
    this.statuses.set(service, status);
    this.notifyWatchers(service, status);
  }

  getStatus(service: string): HealthServingStatus {
    if (this.shutDown) return "NOT_SERVING";
    return this.statuses.get(service) ?? "SERVICE_UNKNOWN";
  }

  clearStatus(service: string): void {
    this.statuses.delete(service);
    this.notifyWatchers(service, "SERVICE_UNKNOWN");
  }

  shutdown(): void {
    this.shutDown = true;
    for (const service of this.statuses.keys()) {
      this.notifyWatchers(service, "NOT_SERVING");
    }
  }

  /** Returns the grpc-js service implementation for the Health service. */
  getServiceImplementation(): grpc.UntypedServiceImplementation {
    const self = this;
    return {
      Check: (
        call: grpc.ServerUnaryCall<{ service: string }, { status: number }>,
        callback: grpc.sendUnaryData<{ status: number }>,
      ) => {
        const service = call.request.service ?? "";
        const status = self.getStatus(service);
        if (status === "SERVICE_UNKNOWN") {
          callback(
            {
              code: grpc.status.NOT_FOUND,
              details: `Service "${service}" is not registered.`,
              name: "NOT_FOUND",
              message: `Service "${service}" is not registered.`,
            } as grpc.ServiceError,
            null,
          );
          return;
        }
        callback(null, {
          status: STATUS_NAME_MAP[status] as unknown as number,
        });
      },

      Watch: (
        call: grpc.ServerWritableStream<
          { service: string },
          { status: number }
        >,
      ) => {
        const service = call.request.service ?? "";
        const sendStatus = (status: HealthServingStatus): void => {
          if (call.destroyed) return;
          call.write({ status: STATUS_NAME_MAP[status] as unknown as number });
        };

        let watchers = self.watchers.get(service);
        if (!watchers) {
          watchers = new Set();
          self.watchers.set(service, watchers);
        }
        watchers.add(sendStatus);

        sendStatus(self.getStatus(service));

        call.on("cancelled", () => {
          const set = self.watchers.get(service);
          if (set) {
            set.delete(sendStatus);
            if (set.size === 0) self.watchers.delete(service);
          }
        });
      },
    };
  }

  /** Loads the Health service definition from the bundled proto. */
  static loadServiceDefinition(): grpc.ServiceDefinition {
    const packageDefinition = protoLoader.loadSync(HEALTH_PROTO, {
      keepCase: false,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    });
    const packageObject = grpc.loadPackageDefinition(packageDefinition);
    const grpcPkg = (packageObject as unknown as Record<string, unknown>)[
      "grpc"
    ];
    const healthPkg = (grpcPkg as Record<string, unknown> | undefined)?.[
      "health"
    ];
    const v1Pkg = (healthPkg as Record<string, unknown> | undefined)?.["v1"];
    const Health = (
      v1Pkg as Record<string, { service: grpc.ServiceDefinition }> | undefined
    )?.["Health"];
    if (!Health) {
      throw new Error(
        "[sinwan-grpc] Failed to load grpc.health.v1.Health service definition.",
      );
    }
    return Health.service;
  }

  private notifyWatchers(service: string, status: HealthServingStatus): void {
    const watchers = this.watchers.get(service);
    if (watchers) {
      for (const watcher of watchers) {
        watcher(status);
      }
    }
  }
}

export function healthStatusToNumber(status: HealthServingStatus): number {
  return STATUS_MAP[status] ?? 0;
}

export function numberToHealthStatus(value: number): HealthServingStatus {
  return STATUS_REVERSE_MAP[value] ?? "UNKNOWN";
}
