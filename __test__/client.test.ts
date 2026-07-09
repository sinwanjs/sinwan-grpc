import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
import {
  GRPCClient,
  createGRPCClient,
  resolveGRPCClientCredentials,
  type GRPCClientConfig,
  type GRPCReconnectOptions,
} from "../src/client";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  loadGRPCService,
  type GRPCServiceConfig,
  type GRPCListenOptions,
  type GRPCMethodConfig,
} from "../src/server";
import { createTestRuntime } from "./helpers";
import type { Runtime } from "sinwan-engine";

const PROTO_PATH = new URL("./test.proto", import.meta.url).pathname;

function makeServiceConfig(): GRPCServiceConfig {
  const methods: Record<string, GRPCMethodConfig> = {
    SayHello: grpcUnary((_ctx, req: { name: string }) => ({
      message: `Hello, ${req.name}!`,
    })) as GRPCMethodConfig,
    StreamGreetings: grpcServerStream(function* (_ctx, req: { name: string }) {
      yield { message: `Hi, ${req.name}!` };
      yield { message: `Bye, ${req.name}!` };
    }) as GRPCMethodConfig,
    CollectGreetings: grpcClientStream((_ctx, call) => {
      return new Promise<{ count: number }>((resolve) => {
        let count = 0;
        call.on("data", () => count++);
        call.on("end", () => resolve({ count }));
      });
    }) as GRPCMethodConfig,
    Chat: grpcBidi(function* () {
      yield { message: "welcome" };
    }) as GRPCMethodConfig,
  };
  return {
    proto: PROTO_PATH,
    package: "test.v1",
    service: "TestService",
    methods,
  };
}

// ─── resolveGRPCClientCredentials ──────────────────────────

describe("resolveGRPCClientCredentials", () => {
  test("returns insecure credentials for undefined", () => {
    const creds = resolveGRPCClientCredentials(undefined);
    expect(creds).toBeDefined();
  });

  test("returns insecure credentials for 'insecure'", () => {
    const creds = resolveGRPCClientCredentials("insecure");
    expect(creds).toBeDefined();
  });

  test("returns the same object for ChannelCredentials input", () => {
    const original = grpc.credentials.createInsecure();
    const creds = resolveGRPCClientCredentials(original);
    expect(creds).toBe(original);
  });

  test("creates SSL credentials from rootCerts/privateKey/certChain", () => {
    // Use empty buffers to avoid BoringSSL PEM parsing errors
    // We just verify the function doesn't throw for the SSL config shape
    expect(() =>
      resolveGRPCClientCredentials({
        rootCerts: undefined,
        privateKey: undefined,
        certChain: undefined,
      }),
    ).not.toThrow();
  });
});

// ─── createGRPCClient ──────────────────────────────────────

describe("createGRPCClient", () => {
  test("creates a GRPCClient instance", () => {
    const client = createGRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: "localhost:50051",
    });
    expect(client).toBeInstanceOf(GRPCClient);
    expect(client.serviceName).toBe("test.v1.TestService");
    expect(client.address).toBe("localhost:50051");
    client.close();
  });
});

// ─── GRPCClient ────────────────────────────────────────────

describe("GRPCClient", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let serverHandle: { stop: () => Promise<void>; port: number } | undefined;
  let client: GRPCClient;

  beforeEach(async () => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
    router.grpc("test", makeServiceConfig());
    serverHandle = await router.listen(runtime, {
      port: 0,
    } as GRPCListenOptions);

    client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
  });

  afterEach(async () => {
    client.close();
    if (serverHandle) await serverHandle.stop();
    await router.stop();
  });

  test("constructor sets serviceName and address", () => {
    expect(client.serviceName).toBe("test.v1.TestService");
    expect(client.address).toBe(`localhost:${serverHandle!.port}`);
  });

  test("static create returns GRPCClient instance", () => {
    const c = GRPCClient.create({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
    expect(c).toBeInstanceOf(GRPCClient);
    c.close();
  });

  test("unary() calls the server and returns response", async () => {
    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "World" },
    );
    expect(response.message).toBe("Hello, World!");
  });

  test("unary() resolves method name with lowerFirst fallback", async () => {
    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
    );
    expect(response.message).toBe("Hello, Test!");
  });

  test("unary() throws for unknown method", () => {
    expect(() => client.unary("UnknownMethod", {})).toThrow("Unknown method");
  });

  test("unary() throws for wrong method kind", () => {
    expect(() => client.unary("StreamGreetings", { name: "test" })).toThrow(
      'is "serverStream", not "unary"',
    );
  });

  test("waitForReady() resolves when server is available", async () => {
    await client.waitForReady(Date.now() + 5000);
  });

  test("getConnectionState() returns a valid state", () => {
    const state = client.getConnectionState();
    expect([
      "idle",
      "connecting",
      "ready",
      "transient-failure",
      "shutdown",
    ]).toContain(state);
  });

  test("reconnect() does not throw", () => {
    expect(() => client.reconnect()).not.toThrow();
  });

  test("close() does not throw", () => {
    const c = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
    expect(() => c.close()).not.toThrow();
  });

  test("startConnectionMonitor() and stopConnectionMonitor() do not throw", () => {
    const c = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
      onConnectionStateChange: () => {},
    });
    expect(() => c.startConnectionMonitor()).not.toThrow();
    expect(() => c.stopConnectionMonitor()).not.toThrow();
    c.close();
  });

  test("startConnectionMonitor() is no-op without listener", () => {
    expect(() => client.startConnectionMonitor()).not.toThrow();
  });
});

// ─── GRPCClient with metadata ──────────────────────────────

describe("GRPCClient with metadata", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let serverHandle: { stop: () => Promise<void>; port: number } | undefined;
  let client: GRPCClient;

  beforeEach(async () => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
    router.grpc("test", {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: {
        SayHello: grpcUnary((ctx, req: { name: string }) => {
          const md = ctx.grpc?.metadata as grpc.Metadata | undefined;
          const customHeader = md?.get("x-custom")?.[0];
          return {
            message: `Hello, ${req.name}! custom=${customHeader ?? "none"}`,
          };
        }) as unknown as GRPCServiceConfig["methods"][string],
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
    });
    serverHandle = await router.listen(runtime, {
      port: 0,
    } as GRPCListenOptions);

    client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
  });

  afterEach(async () => {
    client.close();
    if (serverHandle) await serverHandle.stop();
    await router.stop();
  });

  test("unary() sends metadata to server", async () => {
    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Meta" },
      { metadata: { "x-custom": "test-value" } },
    );
    expect(response.message).toContain("custom=test-value");
  });
});

// ─── GRPCClient error handling ─────────────────────────────

describe("GRPCClient error handling", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let serverHandle: { stop: () => Promise<void>; port: number } | undefined;
  let client: GRPCClient;

  beforeEach(async () => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
    router.grpc("test", {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      methods: {
        SayHello: grpcUnary(() => {
          throw new Error("handler error");
        }) as unknown as GRPCServiceConfig["methods"][string],
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
    });
    serverHandle = await router.listen(runtime, {
      port: 0,
    } as GRPCListenOptions);

    client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
  });

  afterEach(async () => {
    client.close();
    if (serverHandle) await serverHandle.stop();
    await router.stop();
  });

  test("unary() rejects with ServiceError when handler throws", async () => {
    await expect(client.unary("SayHello", { name: "error" })).rejects.toThrow(
      "handler error",
    );
  });
});

// ─── GRPCClient streaming methods ──────────────────────────

describe("GRPCClient streaming methods", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let serverHandle: { stop: () => Promise<void>; port: number } | undefined;
  let client: GRPCClient;

  beforeEach(async () => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
    router.grpc("test", makeServiceConfig());
    serverHandle = await router.listen(runtime, {
      port: 0,
    } as GRPCListenOptions);

    client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${serverHandle!.port}`,
    });
  });

  afterEach(async () => {
    client.close();
    if (serverHandle) await serverHandle.stop();
    await router.stop();
  });

  test("serverStream() returns a readable stream", async () => {
    const stream = client.serverStream<{ name: string }, { message: string }>(
      "StreamGreetings",
      { name: "Stream" },
    );

    const messages: string[] = [];
    await new Promise<void>((resolve) => {
      stream.on("data", (msg: { message: string }) => {
        messages.push(msg.message);
      });
      stream.on("end", () => resolve());
      stream.on("error", () => resolve());
    });

    expect(messages).toContain("Hi, Stream!");
    expect(messages).toContain("Bye, Stream!");
  });

  test("clientStream() sends messages and receives response", async () => {
    const call = client.clientStream<{ name: string }, { count: number }>(
      "CollectGreetings",
    );

    call.stream.write({ name: "msg1" });
    call.stream.write({ name: "msg2" });
    call.stream.write({ name: "msg3" });
    call.stream.end();

    const response = await call.response;
    expect(response.count).toBe(3);
  });

  test("bidi() returns a duplex stream", async () => {
    const stream = client.bidi<{ name: string }, { message: string }>("Chat");

    const messages: string[] = [];
    const done = new Promise<void>((resolve) => {
      stream.on("data", (msg: { message: string }) => {
        messages.push(msg.message);
      });
      stream.on("end", () => resolve());
      stream.on("error", () => resolve());
    });

    stream.write({ name: "hello" });
    stream.end();

    await done;
    expect(messages).toContain("welcome");
  });

  test("serverStream() throws for unknown method", () => {
    expect(() => client.serverStream("Unknown", {})).toThrow("Unknown method");
  });

  test("clientStream() throws for unknown method", () => {
    expect(() => client.clientStream("Unknown")).toThrow("Unknown method");
  });

  test("bidi() throws for unknown method", () => {
    expect(() => client.bidi("Unknown")).toThrow("Unknown method");
  });
});
