import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
import {
  sinwanGRPC,
  registerSinwanGRPC,
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  type GRPCServiceConfig,
  type GRPCListenOptions,
  type GRPCMethodConfig,
} from "../src/index";
import { Sinwan } from "sinwan-engine";

const PROTO_PATH = new URL("./test.proto", import.meta.url).pathname;

// ─── sinwanGRPC module ─────────────────────────────────────

describe("sinwanGRPC module", () => {
  test("has correct name", () => {
    expect(sinwanGRPC.name).toBe("sinwan-grpc");
  });

  test("has a register function", () => {
    expect(typeof sinwanGRPC.register).toBe("function");
  });
});

// ─── registerSinwanGRPC ────────────────────────────────────

describe("registerSinwanGRPC", () => {
  test("registers the provider (idempotent)", () => {
    // Should not throw even if called multiple times
    registerSinwanGRPC();
    registerSinwanGRPC();
  });
});

// ─── SinwanGRPCProvider (via Sinwan integration) ───────────

describe("SinwanGRPCProvider via Sinwan", () => {
  let app: Sinwan;

  beforeEach(() => {
    // Reset provider by registering ours
    registerSinwanGRPC();
  });

  afterEach(async () => {
    if (app) {
      await app.stop();
    }
  });

  test("app.grpc() registers a service", async () => {
    app = await Sinwan.create();
    app.register(sinwanGRPC);

    const config: GRPCServiceConfig = {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: {
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
      },
    };

    // The module augmentation should make grpc() available
    expect(() =>
      (
        app as unknown as {
          grpc: (name: string, config: GRPCServiceConfig) => void;
        }
      ).grpc("test", config),
    ).not.toThrow();
  });

  test("app.listenGRPC() starts a gRPC server", async () => {
    app = await Sinwan.create();
    app.register(sinwanGRPC);

    const config: GRPCServiceConfig = {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: {
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
      },
    };

    (
      app as unknown as {
        grpc: (name: string, config: GRPCServiceConfig) => void;
      }
    ).grpc("test", config);

    const handle = await (
      app as unknown as {
        listenGRPC: (
          options?: GRPCListenOptions,
        ) => Promise<{
          port: number;
          services: string[];
          stop: () => Promise<void>;
        }>;
      }
    ).listenGRPC({ port: 0 } as GRPCListenOptions);

    expect(handle.port).toBeGreaterThan(0);
    expect(handle.services).toContain("test.v1.TestService");
    await handle.stop();
  });

  test("app.listenGRPC() with named service", async () => {
    app = await Sinwan.create();
    app.register(sinwanGRPC);

    const config: GRPCServiceConfig = {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: {
        SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
        StreamGreetings: grpcServerStream(function* () {
          yield { message: "hi" };
        }) as GRPCMethodConfig,
        CollectGreetings: grpcClientStream(() => ({
          count: 0,
        })) as GRPCMethodConfig,
        Chat: grpcBidi(function* () {
          yield { message: "ok" };
        }) as GRPCMethodConfig,
      },
    };

    (
      app as unknown as {
        grpc: (name: string, config: GRPCServiceConfig) => void;
      }
    ).grpc("myservice", config);

    const handle = await (
      app as unknown as {
        listenGRPC: (
          name: string,
          options?: GRPCListenOptions,
        ) => Promise<{
          port: number;
          services: string[];
          stop: () => Promise<void>;
        }>;
      }
    ).listenGRPC("myservice", { port: 0 } as GRPCListenOptions);

    expect(handle.port).toBeGreaterThan(0);
    await handle.stop();
  });
});

// ─── Re-exports ────────────────────────────────────────────

describe("re-exports", () => {
  test("GRPCRouter is exported", () => {
    expect(GRPCRouter).toBeDefined();
  });

  test("grpcUnary is exported", () => {
    expect(typeof grpcUnary).toBe("function");
  });

  test("sinwanGRPC is exported", () => {
    expect(sinwanGRPC).toBeDefined();
  });

  test("registerSinwanGRPC is exported", () => {
    expect(typeof registerSinwanGRPC).toBe("function");
  });
});
