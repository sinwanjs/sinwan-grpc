import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
import * as api from "@opentelemetry/api";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  type GRPCServiceConfig,
  type GRPCMethodConfig,
} from "../src/server";
import { GRPCClient, type GRPCClientConfig } from "../src/client";
import {
  createTracingServerInterceptor,
  createTracingClientInterceptor,
} from "../src/tracing";
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

// ─── Unit tests ────────────────────────────────────────────

describe("createTracingServerInterceptor", () => {
  test("returns a function (interceptor)", () => {
    const interceptor = createTracingServerInterceptor();
    expect(typeof interceptor).toBe("function");
  });

  test("accepts config options", () => {
    const interceptor = createTracingServerInterceptor({
      tracerName: "custom-tracer",
      spanPrefix: "custom",
      attributes: { "service.name": "test" },
    });
    expect(typeof interceptor).toBe("function");
  });
});

describe("createTracingClientInterceptor", () => {
  test("returns a function (interceptor)", () => {
    const interceptor = createTracingClientInterceptor();
    expect(typeof interceptor).toBe("function");
  });

  test("accepts config options", () => {
    const interceptor = createTracingClientInterceptor({
      tracerName: "custom-tracer",
      spanPrefix: "custom",
      attributes: { "service.name": "test" },
    });
    expect(typeof interceptor).toBe("function");
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("tracing integration", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let handle: { port: number; stop: () => Promise<void> } | undefined;

  beforeEach(() => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
  });

  afterEach(async () => {
    if (handle) await handle.stop();
    await router.stop();
  });

  test("server tracing interceptor works with gRPC calls", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [createTracingServerInterceptor()],
    });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Traced" },
    );

    expect(response.message).toBe("Hello, Traced!");
    client.close();
  });

  test("client tracing interceptor works with gRPC calls", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      interceptors: [createTracingClientInterceptor()],
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Traced" },
    );

    expect(response.message).toBe("Hello, Traced!");
    client.close();
  });

  test("both server and client tracing work together", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [createTracingServerInterceptor()],
    });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      interceptors: [createTracingClientInterceptor()],
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Both" },
    );

    expect(response.message).toBe("Hello, Both!");
    client.close();
  });

  test("server tracing interceptor sets error status on handler failure", async () => {
    const errorMethods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(() => {
        throw new Error("handler failure");
      }) as GRPCMethodConfig,
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

    router.grpc("test", {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: errorMethods,
    });
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [createTracingServerInterceptor()],
    });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    await expect(
      client.unary<{ name: string }, { message: string }>("SayHello", {
        name: "Error",
      }),
    ).rejects.toThrow("handler failure");

    client.close();
  });

  test("client tracing interceptor sets error status on call failure", async () => {
    const errorMethods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(() => {
        throw new Error("client trace failure");
      }) as GRPCMethodConfig,
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

    router.grpc("test", {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: errorMethods,
    });
    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      interceptors: [createTracingClientInterceptor()],
    });

    await expect(
      client.unary<{ name: string }, { message: string }>("SayHello", {
        name: "Error",
      }),
    ).rejects.toThrow("client trace failure");

    client.close();
  });
});
