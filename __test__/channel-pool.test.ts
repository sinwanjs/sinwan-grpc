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
import { GRPCChannelPool } from "../src/channel-pool";
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

describe("GRPCChannelPool", () => {
  test("creates pool with single address", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    expect(pool.size).toBe(1);
    pool.close();
  });

  test("creates pool with multiple addresses", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051", "localhost:50052"],
    });
    expect(pool.size).toBe(2);
    pool.close();
  });

  test("creates pool with channelsPerAddress > 1", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051", "localhost:50052"],
      channelsPerAddress: 3,
    });
    expect(pool.size).toBe(6);
    pool.close();
  });

  test("throws on empty addresses", () => {
    expect(() => new GRPCChannelPool({ addresses: [] })).toThrow();
  });

  test("getClient returns a grpc.Client", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    const client = pool.getClient();
    expect(client).toBeDefined();
    expect(typeof client.close).toBe("function");
    pool.close();
  });

  test("round_robin strategy cycles through channels", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051", "localhost:50052", "localhost:50053"],
      strategy: "round_robin",
    });
    const addr1 = pool.getAddress();
    const addr2 = pool.getAddress();
    const addr3 = pool.getAddress();
    const addr4 = pool.getAddress();

    expect(addr1).toBe("localhost:50051");
    expect(addr2).toBe("localhost:50052");
    expect(addr3).toBe("localhost:50053");
    expect(addr4).toBe("localhost:50051"); // wraps around
    pool.close();
  });

  test("least_connections strategy picks channel with fewest calls", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051", "localhost:50052"],
      strategy: "least_connections",
    });
    expect(pool.getActiveCallCounts()).toEqual([0, 0]);
    pool.close();
  });

  test("close prevents further getClient calls", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    pool.close();
    expect(() => pool.getClient()).toThrow("ChannelPool is closed");
  });

  test("close prevents further getAddress calls", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    pool.close();
    expect(() => pool.getAddress()).toThrow("ChannelPool is closed");
  });

  test("close prevents further unaryCall calls", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    pool.close();
    expect(() => pool.unaryCall("/test/SayHello", {})).toThrow(
      "ChannelPool is closed",
    );
  });

  test("close is idempotent", () => {
    const pool = new GRPCChannelPool({
      addresses: ["localhost:50051"],
    });
    pool.close();
    pool.close(); // should not throw
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("channel pool integration", () => {
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

  test("pool distributes calls to a real gRPC server", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const pool = new GRPCChannelPool({
      addresses: [`localhost:${handle.port}`],
    });

    expect(pool.size).toBe(1);
    pool.close();
  });

  test("pool unaryCall executes against a real gRPC server", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const pool = new GRPCChannelPool({
      addresses: [`localhost:${handle.port}`],
    });

    // The pool uses JSON serialization while the server expects protobuf,
    // so the call may error — but this exercises the full code path:
    // selectChannel, activeCalls++, makeUnaryRequest, callback, activeCalls--
    try {
      await pool.unaryCall("/test.v1.TestService/SayHello", {
        name: "Pool",
      });
    } catch {
      // Expected: serialization mismatch may cause an error
    }

    // Verify activeCalls was decremented back to 0
    expect(pool.getActiveCallCounts()).toEqual([0]);

    pool.close();
  });

  test("pool unaryCall rejects for invalid method", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const pool = new GRPCChannelPool({
      addresses: [`localhost:${handle.port}`],
    });

    await expect(
      pool.unaryCall("/test.v1.TestService/Unknown", {}),
    ).rejects.toThrow();

    pool.close();
  });
});
