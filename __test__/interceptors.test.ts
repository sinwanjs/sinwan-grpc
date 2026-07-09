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
import { GRPCClient, type GRPCClientConfig } from "../src/client";
import {
  composeServerInterceptors,
  composeClientInterceptors,
  createLoggingInterceptor,
  createMetadataInjectionInterceptor,
  type GRPCServerInterceptor,
  type GRPCClientInterceptor,
} from "../src/interceptors";
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

// ─── Unit tests: composition helpers ───────────────────────

describe("composeServerInterceptors", () => {
  test("merges multiple arrays", () => {
    const a: GRPCServerInterceptor[] = [
      (() => ({})) as unknown as GRPCServerInterceptor,
    ];
    const b: GRPCServerInterceptor[] = [
      (() => ({})) as unknown as GRPCServerInterceptor,
    ];
    const result = composeServerInterceptors(a, b);
    expect(result.length).toBe(2);
  });

  test("filters undefined", () => {
    const a: GRPCServerInterceptor[] = [
      (() => ({})) as unknown as GRPCServerInterceptor,
    ];
    const result = composeServerInterceptors(a, undefined, undefined);
    expect(result.length).toBe(1);
  });

  test("returns empty for all undefined", () => {
    expect(composeServerInterceptors(undefined, undefined)).toEqual([]);
  });
});

describe("composeClientInterceptors", () => {
  test("merges multiple arrays", () => {
    const a: GRPCClientInterceptor[] = [
      (() => ({})) as unknown as GRPCClientInterceptor,
    ];
    const b: GRPCClientInterceptor[] = [
      (() => ({})) as unknown as GRPCClientInterceptor,
    ];
    const result = composeClientInterceptors(a, b);
    expect(result.length).toBe(2);
  });
});

// ─── Unit tests: interceptor factories ─────────────────────

describe("createLoggingInterceptor", () => {
  test("returns a function (interceptor)", () => {
    const interceptor = createLoggingInterceptor(() => {});
    expect(typeof interceptor).toBe("function");
  });
});

describe("createMetadataInjectionInterceptor", () => {
  test("returns a function (interceptor)", () => {
    const interceptor = createMetadataInjectionInterceptor({
      "x-test": "value",
    });
    expect(typeof interceptor).toBe("function");
  });
});

// ─── Integration tests: server interceptors ────────────────

describe("server interceptors integration", () => {
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

  test("interceptors are passed to server and run", async () => {
    let intercepted = false;
    const interceptor: GRPCServerInterceptor = (_methodDescriptor, call) => {
      intercepted = true;
      return new grpc.ServerInterceptingCall(call, {});
    };

    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [interceptor],
    } as GRPCListenOptions);

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
    );

    expect(intercepted).toBe(true);
    expect(response.message).toBe("Hello, Test!");
    client.close();
  });

  test("createLoggingInterceptor logs method and duration", async () => {
    let loggedMethod: string | undefined;
    let loggedDuration: number | undefined;

    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [
        createLoggingInterceptor((method, durationMs) => {
          loggedMethod = method;
          loggedDuration = durationMs;
        }),
      ],
    } as GRPCListenOptions);

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    await client.unary<{ name: string }, { message: string }>("SayHello", {
      name: "Log",
    });

    expect(loggedMethod).toContain("SayHello");
    expect(typeof loggedDuration).toBe("number");
    client.close();
  });

  test("createMetadataInjectionInterceptor injects metadata", async () => {
    let receivedValue: string | undefined;

    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary((ctx, req: { name: string }) => {
        const md = ctx.grpc?.metadata as grpc.Metadata | undefined;
        receivedValue = md?.get("x-injected")?.[0] as string | undefined;
        return { message: `Hello, ${req.name}!` };
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
      methods,
    });
    handle = await router.listen(runtime, {
      port: 0,
      interceptors: [
        createMetadataInjectionInterceptor({ "x-injected": "test-value" }),
      ],
    } as GRPCListenOptions);

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    await client.unary<{ name: string }, { message: string }>("SayHello", {
      name: "Inject",
    });

    expect(receivedValue).toBe("test-value");
    client.close();
  });
});

// ─── Integration tests: client interceptors ────────────────

describe("client interceptors integration", () => {
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

  test("client interceptors are applied to calls", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    let intercepted = false;
    const interceptor: GRPCClientInterceptor = (options, nextCall) => {
      intercepted = true;
      return new grpc.InterceptingCall(nextCall(options), {
        start: (metadata, listener, next) => {
          next(metadata, listener);
        },
      });
    };

    const clientConfig: GRPCClientConfig = {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      interceptors: [interceptor],
    };

    const client = new GRPCClient(clientConfig);
    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
    );

    expect(intercepted).toBe(true);
    expect(response.message).toBe("Hello, Test!");
    client.close();
  });

  test("per-call interceptors are applied", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    let intercepted = false;
    const interceptor: GRPCClientInterceptor = (options, nextCall) => {
      intercepted = true;
      return new grpc.InterceptingCall(nextCall(options), {
        start: (metadata, listener, next) => {
          next(metadata, listener);
        },
      });
    };

    const clientConfig: GRPCClientConfig = {
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    };

    const client = new GRPCClient(clientConfig);
    await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
      { interceptors: [interceptor] },
    );

    expect(intercepted).toBe(true);
    client.close();
  });
});
