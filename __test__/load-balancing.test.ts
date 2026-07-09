import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  type GRPCServiceConfig,
  type GRPCMethodConfig,
} from "../src/server";
import { GRPCClient } from "../src/client";
import {
  resolveLoadBalancingOptions,
  formatLoadBalancingTarget,
  type GRPCLoadBalancingConfig,
} from "../src/load-balancing";
import { createTestRuntime } from "./helpers";
import type { Runtime } from "sinwan-engine";

const PROTO_PATH = new URL("./test.proto", import.meta.url).pathname;

function makeServiceConfig(): GRPCServiceConfig {
  const methods: Record<string, GRPCMethodConfig> = {
    SayHello: grpcUnary((_ctx, req: { name: string }) => ({
      message: `Hello, ${req.name}!`,
    })) as GRPCMethodConfig,
    StreamGreetings: grpcServerStream(function* () { yield { message: "hi" }; }) as GRPCMethodConfig,
    CollectGreetings: grpcClientStream(() => ({ count: 0 })) as GRPCMethodConfig,
    Chat: grpcBidi(function* () { yield { message: "ok" }; }) as GRPCMethodConfig,
  };
  return {
    proto: PROTO_PATH,
    package: "test.v1",
    service: "TestService",
    methods,
  };
}

// ─── Unit tests ────────────────────────────────────────────

describe("resolveLoadBalancingOptions", () => {
  test("returns empty for undefined config", () => {
    expect(resolveLoadBalancingOptions(undefined)).toEqual({});
  });

  test("returns empty for config without policy", () => {
    expect(resolveLoadBalancingOptions({})).toEqual({});
  });

  test("returns pick_first option", () => {
    const opts = resolveLoadBalancingOptions({ policy: "pick_first" });
    expect(opts["grpc.lb_policy_name"]).toBe("pick_first");
  });

  test("returns round_robin option", () => {
    const opts = resolveLoadBalancingOptions({ policy: "round_robin" });
    expect(opts["grpc.lb_policy_name"]).toBe("round_robin");
  });

  test("returns grpclb option with fallback timeout", () => {
    const opts = resolveLoadBalancingOptions({
      policy: "grpclb",
      fallbackPolicy: "round_robin",
    });
    expect(opts["grpc.lb_policy_name"]).toBe("grpclb");
    expect(opts["grpc.grpclb_fallback_timeout_ms"]).toBe(10_000);
  });
});

describe("formatLoadBalancingTarget", () => {
  test("returns single address as-is for pick_first", () => {
    expect(formatLoadBalancingTarget(["localhost:50051"], "pick_first")).toBe(
      "localhost:50051",
    );
  });

  test("returns single address as-is for single address", () => {
    expect(formatLoadBalancingTarget(["localhost:50051"], "round_robin")).toBe(
      "localhost:50051",
    );
  });

  test("formats multiple addresses with dns:/// prefix for round_robin", () => {
    const result = formatLoadBalancingTarget(
      ["localhost:50051", "localhost:50052"],
      "round_robin",
    );
    expect(result).toBe("dns:///localhost:50051,localhost:50052");
  });

  test("formats multiple addresses for grpclb", () => {
    const result = formatLoadBalancingTarget(
      ["localhost:50051", "localhost:50052", "localhost:50053"],
      "grpclb",
    );
    expect(result).toBe("dns:///localhost:50051,localhost:50052,localhost:50053");
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("load balancing integration", () => {
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

  test("client with pick_first policy works", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      options: resolveLoadBalancingOptions({ policy: "pick_first" }) as never,
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "LB" },
    );

    expect(response.message).toBe("Hello, LB!");
    client.close();
  });
});
