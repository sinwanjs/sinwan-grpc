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
  makeAbortablePromise,
  attachAbortSignal,
  isCallCancelled,
  type GRPCAbortablePromise,
} from "../src/cancellation";
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

describe("makeAbortablePromise", () => {
  test("adds cancel() to a promise", () => {
    const p = Promise.resolve(42);
    const abortable = makeAbortablePromise(p, { cancel: () => {} });
    expect(typeof abortable.cancel).toBe("function");
  });

  test("cancel() calls the provided cancel function", () => {
    let cancelled = false;
    const p = Promise.resolve(42);
    const abortable = makeAbortablePromise(p, {
      cancel: () => {
        cancelled = true;
      },
    });
    abortable.cancel();
    expect(cancelled).toBe(true);
  });
});

describe("isCallCancelled", () => {
  test("returns false for non-cancelled call", () => {
    expect(isCallCancelled({ cancelled: false })).toBe(false);
  });

  test("returns true for cancelled call", () => {
    expect(isCallCancelled({ cancelled: true })).toBe(true);
  });

  test("returns true for destroyed call", () => {
    expect(isCallCancelled({ destroyed: true })).toBe(true);
  });

  test("returns false for call without cancelled/destroyed", () => {
    expect(isCallCancelled({})).toBe(false);
  });
});

describe("attachAbortSignal", () => {
  test("does nothing without signal", () => {
    const call = {
      cancel: () => {
        throw new Error("should not be called");
      },
      on: () => {},
    };
    attachAbortSignal(undefined, call);
  });

  test("cancels immediately if signal already aborted", () => {
    const controller = new AbortController();
    controller.abort();
    let cancelled = false;
    const call = {
      cancel: () => {
        cancelled = true;
      },
      on: () => {},
    };
    attachAbortSignal(controller.signal, call);
    expect(cancelled).toBe(true);
  });

  test("cancels when signal fires after delay", async () => {
    const controller = new AbortController();
    let cancelled = false;
    const call = {
      cancel: () => {
        cancelled = true;
      },
      on: () => {},
    };
    attachAbortSignal(controller.signal, call);

    expect(cancelled).toBe(false);

    controller.abort();
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(cancelled).toBe(true);
  });

  test("removes abort listener when call is cancelled by other means", () => {
    const controller = new AbortController();
    let cancelled = false;
    let cancelledListener: (() => void) | undefined;
    const call = {
      cancel: () => {
        cancelled = true;
      },
      on: (_event: string, listener: () => void) => {
        cancelledListener = listener;
      },
    };
    attachAbortSignal(controller.signal, call);

    // Simulate call cancelled by other means
    expect(cancelledListener).toBeDefined();
    cancelledListener!();

    // Now aborting the signal should NOT call cancel again
    cancelled = false;
    controller.abort();
    expect(cancelled).toBe(false);
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("cancellation integration", () => {
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

  test("unary() returns abortable promise with cancel()", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const promise = client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
    );

    expect(typeof (promise as GRPCAbortablePromise<unknown>).cancel).toBe(
      "function",
    );

    const response = await promise;
    expect(response.message).toBe("Hello, Test!");
    client.close();
  });

  test("unary() cancel() rejects with CANCELLED", async () => {
    // Use a handler that delays so we can cancel before it completes
    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(async () => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        return { message: "delayed" };
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

    const promise = client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
    );

    promise.cancel();

    try {
      await promise;
      expect(false).toBe(true); // should not reach
    } catch (error) {
      const serviceError = error as grpc.ServiceError;
      expect(serviceError.code).toBe(grpc.status.CANCELLED);
    }

    client.close();
  });

  test("AbortSignal cancels unary call", async () => {
    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(async () => {
        await new Promise((resolve) => setTimeout(resolve, 500));
        return { message: "delayed" };
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

    const controller = new AbortController();

    const promise = client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Test" },
      { signal: controller.signal },
    );

    controller.abort();

    try {
      await promise;
      expect(false).toBe(true);
    } catch (error) {
      const serviceError = error as grpc.ServiceError;
      expect(serviceError.code).toBe(grpc.status.CANCELLED);
    }

    client.close();
  });

  test("serverStream() supports cancel via stream.cancel()", async () => {
    const methods: Record<string, GRPCMethodConfig> = {
      SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
      StreamGreetings: grpcServerStream(async function* () {
        yield { message: "first" };
        await new Promise((resolve) => setTimeout(resolve, 200));
        yield { message: "second" };
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

    const stream = client.serverStream("StreamGreetings", {});
    const messages: string[] = [];

    await new Promise<void>((resolve) => {
      stream.on("data", (msg: { message: string }) => {
        messages.push(msg.message);
        stream.cancel();
      });
      stream.on("end", () => resolve());
      stream.on("error", () => resolve());
    });

    expect(messages.length).toBe(1);
    expect(messages[0]).toBe("first");
    client.close();
  });
});
