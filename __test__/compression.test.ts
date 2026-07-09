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
  resolveCompressionOptions,
  getCompressionAlgorithmValue,
  type GRPCCompressionConfig,
} from "../src/compression";
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

// ─── Unit tests: resolveCompressionOptions ─────────────────

describe("resolveCompressionOptions", () => {
  test("returns empty for undefined config", () => {
    expect(resolveCompressionOptions(undefined)).toEqual({});
  });

  test("returns empty for identity algorithm", () => {
    expect(resolveCompressionOptions({ algorithm: "identity" })).toEqual({});
  });

  test("returns gzip option", () => {
    const opts = resolveCompressionOptions({ algorithm: "gzip" });
    expect(opts["grpc.default_compression_algorithm"]).toBe(
      grpc.compressionAlgorithms.gzip,
    );
  });

  test("returns deflate option", () => {
    const opts = resolveCompressionOptions({ algorithm: "deflate" });
    expect(opts["grpc.default_compression_algorithm"]).toBe(
      grpc.compressionAlgorithms.deflate,
    );
  });

  test("includes minSize when provided", () => {
    const opts = resolveCompressionOptions({ algorithm: "gzip", minSize: 1024 });
    expect(opts["grpc.min_message_size_to_compress"]).toBe(1024);
  });

  test("does not include minSize for identity", () => {
    const opts = resolveCompressionOptions({ algorithm: "identity", minSize: 1024 });
    expect(opts["grpc.min_message_size_to_compress"]).toBeUndefined();
  });
});

// ─── Unit tests: getCompressionAlgorithmValue ──────────────

describe("getCompressionAlgorithmValue", () => {
  test("returns correct values", () => {
    expect(getCompressionAlgorithmValue("identity")).toBe(
      grpc.compressionAlgorithms.identity,
    );
    expect(getCompressionAlgorithmValue("deflate")).toBe(
      grpc.compressionAlgorithms.deflate,
    );
    expect(getCompressionAlgorithmValue("gzip")).toBe(
      grpc.compressionAlgorithms.gzip,
    );
  });
});

// ─── Integration tests ─────────────────────────────────────

describe("compression integration", () => {
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

  test("server with gzip compression works", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      compression: { algorithm: "gzip" },
    } as GRPCListenOptions);

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
      compression: { algorithm: "gzip" },
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Compressed" },
    );

    expect(response.message).toBe("Hello, Compressed!");
    client.close();
  });

  test("server with identity (no compression) works", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, {
      port: 0,
      compression: { algorithm: "identity" },
    } as GRPCListenOptions);

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "Plain" },
    );

    expect(response.message).toBe("Hello, Plain!");
    client.close();
  });

  test("server without compression config works", async () => {
    router.grpc("test", makeServiceConfig());
    handle = await router.listen(runtime, { port: 0 });

    const client = new GRPCClient({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
      address: `localhost:${handle.port}`,
    });

    const response = await client.unary<{ name: string }, { message: string }>(
      "SayHello",
      { name: "NoComp" },
    );

    expect(response.message).toBe("Hello, NoComp!");
    client.close();
  });
});
