import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  type GRPCServiceConfig,
  type GRPCListenOptions,
  type GRPCMethodConfig,
} from "../src/server";
import {
  GRPCHealthChecker,
  healthStatusToNumber,
  numberToHealthStatus,
  type HealthServingStatus,
} from "../src/health";
import { createTestRuntime } from "./helpers";
import type { Runtime } from "sinwan-engine";

const PROTO_PATH = new URL("./test.proto", import.meta.url).pathname;

function makeServiceConfig(): GRPCServiceConfig {
  const methods: Record<string, GRPCMethodConfig> = {
    SayHello: grpcUnary((_ctx, req: { name: string }) => ({
      message: `Hello, ${req.name}!`,
    })) as GRPCMethodConfig,
    StreamGreetings: grpcServerStream(function* () {
      yield { message: "hi" };
    }) as GRPCMethodConfig,
    CollectGreetings: grpcClientStream(() => ({
      count: 0,
    })) as GRPCMethodConfig,
    Chat: grpcBidi(function* () {
      yield { message: "ok" };
    }) as GRPCMethodConfig,
  };
  return {
    proto: PROTO_PATH,
    package: "test.v1",
    service: "TestService",
    methods,
  };
}

// ─── Unit tests: GRPCHealthChecker ─────────────────────────

describe("GRPCHealthChecker", () => {
  let checker: GRPCHealthChecker;

  beforeEach(() => {
    checker = new GRPCHealthChecker();
  });

  test("default overall status is SERVING", () => {
    expect(checker.getStatus("")).toBe("SERVING");
  });

  test("setStatus and getStatus", () => {
    checker.setStatus("my.service", "SERVING");
    expect(checker.getStatus("my.service")).toBe("SERVING");

    checker.setStatus("my.service", "NOT_SERVING");
    expect(checker.getStatus("my.service")).toBe("NOT_SERVING");
  });

  test("getStatus returns SERVICE_UNKNOWN for unregistered service", () => {
    expect(checker.getStatus("unknown.service")).toBe("SERVICE_UNKNOWN");
  });

  test("clearStatus removes the status", () => {
    checker.setStatus("my.service", "SERVING");
    checker.clearStatus("my.service");
    expect(checker.getStatus("my.service")).toBe("SERVICE_UNKNOWN");
  });

  test("shutdown sets all statuses to NOT_SERVING", () => {
    checker.setStatus("my.service", "SERVING");
    checker.shutdown();
    expect(checker.getStatus("")).toBe("NOT_SERVING");
    expect(checker.getStatus("my.service")).toBe("NOT_SERVING");
  });

  test("initialStatus option", () => {
    const c = new GRPCHealthChecker({ initialStatus: "NOT_SERVING" });
    expect(c.getStatus("")).toBe("NOT_SERVING");
  });

  test("getServiceImplementation has Check and Watch", () => {
    const impl = checker.getServiceImplementation();
    expect(typeof impl.Check).toBe("function");
    expect(typeof impl.Watch).toBe("function");
  });

  test("loadServiceDefinition returns a valid service definition", () => {
    const def = GRPCHealthChecker.loadServiceDefinition();
    expect(def).toBeDefined();
    expect(def.Check).toBeDefined();
    expect(def.Watch).toBeDefined();
  });
});

// ─── Unit tests: status conversion helpers ─────────────────

describe("healthStatusToNumber", () => {
  test("converts known statuses", () => {
    expect(healthStatusToNumber("UNKNOWN")).toBe(0);
    expect(healthStatusToNumber("SERVING")).toBe(1);
    expect(healthStatusToNumber("NOT_SERVING")).toBe(2);
    expect(healthStatusToNumber("SERVICE_UNKNOWN")).toBe(3);
  });
});

describe("numberToHealthStatus", () => {
  test("converts known numbers", () => {
    expect(numberToHealthStatus(0)).toBe("UNKNOWN");
    expect(numberToHealthStatus(1)).toBe("SERVING");
    expect(numberToHealthStatus(2)).toBe("NOT_SERVING");
    expect(numberToHealthStatus(3)).toBe("SERVICE_UNKNOWN");
  });

  test("defaults to UNKNOWN for unknown numbers", () => {
    expect(numberToHealthStatus(99)).toBe("UNKNOWN");
  });
});

// ─── Integration tests: health check via gRPC ──────────────

describe("health check integration", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let handle:
    | {
        port: number;
        stop: () => Promise<void>;
        healthChecker?: GRPCHealthChecker;
      }
    | undefined;

  beforeEach(() => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
    router.grpc("test", makeServiceConfig());
  });

  afterEach(async () => {
    if (handle) await handle.stop();
    await router.stop();
  });

  test("Check returns SERVING for overall server", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    const healthDef = GRPCHealthChecker.loadServiceDefinition();
    const client = new (grpc.makeGenericClientConstructor(
      healthDef,
      "Health",
      {},
    ))(
      `localhost:${handle.port}`,
      grpc.credentials.createInsecure(),
    ) as unknown as grpc.Client & {
      Check: (
        req: { service: string },
        cb: (
          err: grpc.ServiceError | null,
          resp: { status: number | string },
        ) => void,
      ) => void;
    };

    const response = await new Promise<{ status: number | string }>(
      (resolve, reject) => {
        client.Check({ service: "" }, (err, resp) => {
          if (err) reject(err);
          else resolve(resp);
        });
      },
    );

    expect(String(response.status)).toBe("SERVING");
    client.close();
  });

  test("Check returns SERVING for registered service", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    const healthDef = GRPCHealthChecker.loadServiceDefinition();
    const client = new (grpc.makeGenericClientConstructor(
      healthDef,
      "Health",
      {},
    ))(
      `localhost:${handle.port}`,
      grpc.credentials.createInsecure(),
    ) as unknown as grpc.Client & {
      Check: (
        req: { service: string },
        cb: (
          err: grpc.ServiceError | null,
          resp: { status: number | string },
        ) => void,
      ) => void;
    };

    const response = await new Promise<{ status: number | string }>(
      (resolve, reject) => {
        client.Check({ service: "test.v1.TestService" }, (err, resp) => {
          if (err) reject(err);
          else resolve(resp);
        });
      },
    );

    expect(String(response.status)).toBe("SERVING");
    client.close();
  });

  test("Check returns NOT_FOUND for unknown service", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    const healthDef = GRPCHealthChecker.loadServiceDefinition();
    const client = new (grpc.makeGenericClientConstructor(
      healthDef,
      "Health",
      {},
    ))(
      `localhost:${handle.port}`,
      grpc.credentials.createInsecure(),
    ) as unknown as grpc.Client & {
      Check: (
        req: { service: string },
        cb: (err: grpc.ServiceError | null, resp: { status: number }) => void,
      ) => void;
    };

    const error = await new Promise<grpc.ServiceError | null>((resolve) => {
      client.Check({ service: "nonexistent" }, (err) => {
        resolve(err);
      });
    });

    expect(error).not.toBeNull();
    expect(error!.code).toBe(grpc.status.NOT_FOUND);
    client.close();
  });

  test("healthChecker is exposed on handle", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    expect(handle.healthChecker).toBeInstanceOf(GRPCHealthChecker);
  });

  test("healthChecker.shutdown is called on stop", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    const checker = handle.healthChecker!;
    expect(checker.getStatus("")).toBe("SERVING");

    await handle.stop();
    handle = undefined;

    expect(checker.getStatus("")).toBe("NOT_SERVING");
  });

  test("custom healthChecker can be provided", async () => {
    const customChecker = new GRPCHealthChecker({
      initialStatus: "NOT_SERVING",
    });

    handle = await router.listen(runtime, {
      port: 0,
      health: true,
      healthChecker: customChecker,
    } as GRPCListenOptions);

    expect(handle.healthChecker).toBe(customChecker);
    expect(customChecker.getStatus("")).toBe("NOT_SERVING");
  });

  test("Watch streams status updates", async () => {
    handle = await router.listen(runtime, {
      port: 0,
      health: true,
    } as GRPCListenOptions);

    const healthDef = GRPCHealthChecker.loadServiceDefinition();
    const client = new (grpc.makeGenericClientConstructor(
      healthDef,
      "Health",
      {},
    ))(
      `localhost:${handle.port}`,
      grpc.credentials.createInsecure(),
    ) as unknown as grpc.Client & {
      Watch: (req: {
        service: string;
      }) => grpc.ClientReadableStream<{ status: number | string }>;
    };

    const stream = client.Watch({ service: "" });

    const firstStatus = await new Promise<{ status: number | string }>(
      (resolve, reject) => {
        stream.on("data", (resp: { status: number | string }) => {
          resolve(resp);
        });
        stream.on("error", reject);
        setTimeout(() => reject(new Error("Watch timeout")), 3000);
      },
    );

    expect(String(firstStatus.status)).toBe("SERVING");

    // Trigger a status change
    handle.healthChecker!.setStatus("", "NOT_SERVING");

    const secondStatus = await new Promise<{ status: number | string }>(
      (resolve, reject) => {
        stream.on("data", (resp: { status: number | string }) => {
          resolve(resp);
        });
        stream.on("error", reject);
        setTimeout(() => reject(new Error("Watch timeout")), 3000);
      },
    );

    expect(String(secondStatus.status)).toBe("NOT_SERVING");

    stream.cancel();
    client.close();
  });
});
