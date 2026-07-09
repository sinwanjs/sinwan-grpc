import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
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
  runWithPropagationContext,
  getPropagationContext,
  createPropagationInterceptor,
  extractDeadlineFromMetadata,
} from "../src/propagation";
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

describe("runWithPropagationContext / getPropagationContext", () => {
  test("sets and gets context within run", () => {
    const ctx = { deadline: 12345, metadata: new grpc.Metadata() };
    runWithPropagationContext(ctx, () => {
      expect(getPropagationContext()).toBe(ctx);
    });
  });

  test("returns undefined outside of run", () => {
    expect(getPropagationContext()).toBeUndefined();
  });
});

describe("extractDeadlineFromMetadata", () => {
  test("returns undefined for no grpc-timeout", () => {
    const metadata = new grpc.Metadata();
    expect(extractDeadlineFromMetadata(metadata)).toBeUndefined();
  });

  test("parses seconds timeout", () => {
    const metadata = new grpc.Metadata();
    metadata.set("grpc-timeout", "5S");
    const deadline = extractDeadlineFromMetadata(metadata);
    expect(deadline).toBeDefined();
    expect(deadline! - Date.now()).toBeGreaterThan(4000);
    expect(deadline! - Date.now()).toBeLessThan(6000);
  });

  test("parses milliseconds timeout", () => {
    const metadata = new grpc.Metadata();
    metadata.set("grpc-timeout", "100m");
    const deadline = extractDeadlineFromMetadata(metadata);
    expect(deadline).toBeDefined();
    expect(deadline! - Date.now()).toBeGreaterThan(90);
    expect(deadline! - Date.now()).toBeLessThan(120);
  });

  test("returns undefined for invalid format", () => {
    const metadata = new grpc.Metadata();
    metadata.set("grpc-timeout", "invalid");
    expect(extractDeadlineFromMetadata(metadata)).toBeUndefined();
  });
});

describe("createPropagationInterceptor", () => {
  test("returns a function (interceptor)", () => {
    const interceptor = createPropagationInterceptor({ deadline: true });
    expect(typeof interceptor).toBe("function");
  });

  test("passes through when no propagation context", () => {
    const interceptor = createPropagationInterceptor({ deadline: true });
    let intercepted = false;
    const options: grpc.CallOptions = {};
    const nextCall = () => {
      intercepted = true;
      return {
        start: () => {},
        sendMessage: () => {},
        halfClose: () => {},
        getPeer: () => "peer",
      };
    };
    const call = interceptor(
      options as unknown as grpc.InterceptorOptions,
      nextCall as unknown as (
        options: grpc.InterceptorOptions,
      ) => grpc.InterceptingCall,
    );
    expect(intercepted).toBe(true);
  });

  test("propagates deadline and metadata within context", () => {
    const md = new grpc.Metadata();
    md.set("x-trace-id", "trace-123");
    const ctx = { deadline: Date.now() + 10_000, metadata: md };

    let capturedMetadata: grpc.Metadata | undefined;

    runWithPropagationContext(ctx, () => {
      const interceptor = createPropagationInterceptor({
        deadline: true,
        metadata: ["x-trace-id"],
      });
      const options: grpc.CallOptions = {};
      const nextCall = () => ({
        start: (metadata: grpc.Metadata, _listener: grpc.Listener) => {
          capturedMetadata = metadata;
        },
        sendMessage: () => {},
        halfClose: () => {},
        getPeer: () => "peer",
      });
      const call = interceptor(
        options as unknown as grpc.InterceptorOptions,
        nextCall as unknown as (
          options: grpc.InterceptorOptions,
        ) => grpc.InterceptingCall,
      );
      call.start(new grpc.Metadata(), { onReceiveStatus: () => {} });
    });

    expect(capturedMetadata).toBeDefined();
    expect(capturedMetadata!.get("x-trace-id")).toEqual(["trace-123"]);
  });

  test("propagates all metadata when metadata is true", () => {
    const md = new grpc.Metadata();
    md.set("x-trace-id", "trace-456");
    md.set("x-custom", "custom-val");
    const ctx = { metadata: md };

    let capturedMetadata: grpc.Metadata | undefined;

    runWithPropagationContext(ctx, () => {
      const interceptor = createPropagationInterceptor({
        metadata: true,
      });
      const options: grpc.CallOptions = {};
      const nextCall = () => ({
        start: (metadata: grpc.Metadata, _listener: grpc.Listener) => {
          capturedMetadata = metadata;
        },
        sendMessage: () => {},
        halfClose: () => {},
        getPeer: () => "peer",
      });
      const call = interceptor(
        options as unknown as grpc.InterceptorOptions,
        nextCall as unknown as (
          options: grpc.InterceptorOptions,
        ) => grpc.InterceptingCall,
      );
      call.start(new grpc.Metadata(), { onReceiveStatus: () => {} });
    });

    expect(capturedMetadata).toBeDefined();
    expect(capturedMetadata!.get("x-trace-id")).toEqual(["trace-456"]);
    expect(capturedMetadata!.get("x-custom")).toEqual(["custom-val"]);
  });

  test("does not propagate deadline when remaining time is negative", () => {
    const md = new grpc.Metadata();
    md.set("x-key", "val");
    const ctx = { deadline: Date.now() - 1000, metadata: md };

    let startCalled = false;

    runWithPropagationContext(ctx, () => {
      const interceptor = createPropagationInterceptor({
        deadline: true,
        metadata: ["x-key"],
      });
      const options: grpc.CallOptions = {};
      const nextCall = () => ({
        start: (_metadata: grpc.Metadata, _listener: grpc.Listener) => {
          startCalled = true;
        },
        sendMessage: () => {},
        halfClose: () => {},
        getPeer: () => "peer",
      });
      const call = interceptor(
        options as unknown as grpc.InterceptorOptions,
        nextCall as unknown as (
          options: grpc.InterceptorOptions,
        ) => grpc.InterceptingCall,
      );
      call.start(new grpc.Metadata(), { onReceiveStatus: () => {} });
    });

    expect(startCalled).toBe(true);
  });

  test("propagates metadata with multiple values for same key", () => {
    const md = new grpc.Metadata();
    md.add("x-multi", "val1");
    md.add("x-multi", "val2");
    // Override getMap to return array values (simulates alternative metadata implementations)
    const originalGetMap = md.getMap.bind(md);
    md.getMap = () =>
      ({ "x-multi": ["val1", "val2"] }) as unknown as ReturnType<
        typeof originalGetMap
      >;
    const ctx = { metadata: md };

    let capturedMetadata: grpc.Metadata | undefined;

    runWithPropagationContext(ctx, () => {
      const interceptor = createPropagationInterceptor({
        metadata: true,
      });
      const options: grpc.CallOptions = {};
      const nextCall = () => ({
        start: (metadata: grpc.Metadata, _listener: grpc.Listener) => {
          capturedMetadata = metadata;
        },
        sendMessage: () => {},
        halfClose: () => {},
        getPeer: () => "peer",
      });
      const call = interceptor(
        options as unknown as grpc.InterceptorOptions,
        nextCall as unknown as (
          options: grpc.InterceptorOptions,
        ) => grpc.InterceptingCall,
      );
      call.start(new grpc.Metadata(), { onReceiveStatus: () => {} });
    });

    expect(capturedMetadata).toBeDefined();
    expect(capturedMetadata!.get("x-multi")).toEqual(["val1", "val2"]);
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("propagation integration", () => {
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

  test("metadata is propagated from server to client call", async () => {
    // Server receives metadata and makes a downstream call that should
    // receive the propagated metadata.
    let receivedMetadata: string | undefined;

    // First server: echoes back metadata it receives
    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary((_ctx, req: { name: string }) => {
        const ctx = getPropagationContext();
        if (ctx?.metadata) {
          const traceId = ctx.metadata.get("x-trace-id");
          if (traceId.length > 0) receivedMetadata = traceId[0] as string;
        }
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
      propagation: { metadata: ["x-trace-id"] },
    });

    // Client sends metadata
    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const metadata = new grpc.Metadata();
    metadata.set("x-trace-id", "test-trace-123");

    await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Propagated" },
      { metadata },
    );

    expect(receivedMetadata).toBe("test-trace-123");
    client.close();
  });

  test("propagation works with true (all metadata)", async () => {
    let receivedValues: string[] = [];

    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(() => {
        const ctx = getPropagationContext();
        if (ctx?.metadata) {
          receivedValues.push(
            (ctx.metadata.get("x-trace-id")[0] ?? "") as string,
          );
          receivedValues.push(
            (ctx.metadata.get("x-custom")[0] ?? "") as string,
          );
        }
        return { message: "ok" };
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
      propagation: { metadata: true },
    });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const metadata = new grpc.Metadata();
    metadata.set("x-trace-id", "trace-456");
    metadata.set("x-custom", "custom-789");

    await client.unary("SayHello", { name: "Test" }, { metadata });

    expect(receivedValues).toContain("trace-456");
    expect(receivedValues).toContain("custom-789");
    client.close();
  });

  test("no propagation when not configured", async () => {
    let contextExists = false;

    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(() => {
        contextExists = getPropagationContext() !== undefined;
        return { message: "ok" };
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

    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    await client.unary("SayHello", { name: "Test" });

    expect(contextExists).toBe(false);
    client.close();
  });
});
