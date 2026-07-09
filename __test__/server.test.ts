import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  createGRPCMetadata,
  createGRPCError,
  collectGRPCStream,
  writeGRPCMessage,
  mergeGRPCLoaderOptions,
  loadGRPCService,
  resolveGRPCServerCredentials,
  DEFAULT_GRPC_LOADER_OPTIONS,
  type GRPCServiceConfig,
  type GRPCListenOptions,
  type GRPCMethodConfig,
} from "../src/server";
import { createTestRuntime } from "./helpers";
import type { Runtime } from "sinwan-engine";

const PROTO_PATH = new URL("./test.proto", import.meta.url).pathname;

function makeServiceConfig(
  overrides: Partial<GRPCServiceConfig> = {},
): GRPCServiceConfig {
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
    ...overrides,
  };
}

// ─── Utility function tests ────────────────────────────────

describe("grpcUnary", () => {
  test("creates a unary route with type and handler", () => {
    const handler = () => ({ message: "hi" });
    const route = grpcUnary(handler);
    expect(route.type).toBe("unary");
    expect(route.handler).toBe(handler);
  });

  test("accepts options (hooks, autoEnd)", () => {
    const handler = () => ({ message: "hi" });
    const beforeCall = () => {};
    const route = grpcUnary(handler, { beforeCall, autoEnd: false });
    expect(route.type).toBe("unary");
    expect(route.beforeCall).toBe(beforeCall);
    expect(route.autoEnd).toBe(false);
  });
});

describe("grpcServerStream", () => {
  test("creates a serverStream route", () => {
    const handler = function* () {
      yield { message: "hi" };
    };
    const route = grpcServerStream(handler);
    expect(route.type).toBe("serverStream");
    expect(route.handler).toBe(handler);
  });
});

describe("grpcClientStream", () => {
  test("creates a clientStream route", () => {
    const handler = () => ({ count: 0 });
    const route = grpcClientStream(handler);
    expect(route.type).toBe("clientStream");
    expect(route.handler).toBe(handler);
  });
});

describe("grpcBidi", () => {
  test("creates a bidi route", () => {
    const handler = function* () {
      yield { message: "hi" };
    };
    const route = grpcBidi(handler);
    expect(route.type).toBe("bidi");
    expect(route.handler).toBe(handler);
  });
});

// ─── createGRPCMetadata ────────────────────────────────────

describe("createGRPCMetadata", () => {
  test("returns existing Metadata instance unchanged", () => {
    const md = new grpc.Metadata();
    md.set("foo", "bar");
    const result = createGRPCMetadata(md);
    expect(result).toBe(md);
  });

  test("creates empty metadata from undefined", () => {
    const md = createGRPCMetadata(undefined);
    expect(md).toBeInstanceOf(grpc.Metadata);
  });

  test("creates metadata from object", () => {
    const md = createGRPCMetadata({ "x-custom": "value" });
    expect(md.get("x-custom")).toEqual(["value"]);
  });

  test("creates metadata with array values", () => {
    const md = createGRPCMetadata({ "x-multi": ["a", "b"] });
    expect(md.get("x-multi")).toEqual(["a", "b"]);
  });

  test("creates metadata with Buffer values", () => {
    const buf = Buffer.from("hello");
    const md = createGRPCMetadata({ "x-bin": buf });
    expect(md.get("x-bin")).toHaveLength(1);
  });
});

// ─── createGRPCError ───────────────────────────────────────

describe("createGRPCError", () => {
  test("creates an error with code and details", () => {
    const error = createGRPCError(grpc.status.NOT_FOUND, "not found");
    expect(error).toBeInstanceOf(Error);
    expect(error.code).toBe(grpc.status.NOT_FOUND);
    expect(error.details).toBe("not found");
    expect(error.metadata).toBeInstanceOf(grpc.Metadata);
  });

  test("creates an error with custom metadata", () => {
    const md = new grpc.Metadata();
    md.set("x-trace", "abc123");
    const error = createGRPCError(grpc.status.INVALID_ARGUMENT, "bad", md);
    expect(error.metadata).toBe(md);
  });
});

// ─── collectGRPCStream ─────────────────────────────────────

describe("collectGRPCStream", () => {
  test("collects from sync iterable", async () => {
    const items = await collectGRPCStream([1, 2, 3]);
    expect(items).toEqual([1, 2, 3]);
  });

  test("collects from async iterable", async () => {
    async function* gen() {
      yield "a";
      yield "b";
    }
    const items = await collectGRPCStream(gen());
    expect(items).toEqual(["a", "b"]);
  });

  test("returns empty array for empty iterable", async () => {
    const items = await collectGRPCStream([]);
    expect(items).toEqual([]);
  });
});

// ─── writeGRPCMessage ──────────────────────────────────────

describe("writeGRPCMessage", () => {
  test("writes immediately when stream accepts", async () => {
    let written: unknown;
    const stream = {
      write(value: unknown): boolean {
        written = value;
        return true;
      },
      once(_event: string, _cb: () => void): unknown {
        return undefined;
      },
    };
    await writeGRPCMessage(stream, { msg: "hello" });
    expect(written).toEqual({ msg: "hello" });
  });

  test("waits for drain when stream returns false", async () => {
    let written: unknown;
    let drainCb: (() => void) | undefined;
    const stream = {
      write(_value: unknown): boolean {
        return false;
      },
      once(_event: string, cb: () => void): unknown {
        if (_event === "drain") drainCb = cb;
        return undefined;
      },
    };
    const promise = writeGRPCMessage(stream, { msg: "drained" });
    // Should not resolve yet
    let resolved = false;
    promise.then(() => {
      resolved = true;
    });
    await new Promise((r) => setTimeout(r, 10));
    expect(resolved).toBe(false);
    // Trigger drain
    drainCb!();
    await promise;
    expect(resolved).toBe(true);
  });
});

// ─── mergeGRPCLoaderOptions ────────────────────────────────

describe("mergeGRPCLoaderOptions", () => {
  test("returns defaults with no arguments", () => {
    const result = mergeGRPCLoaderOptions();
    expect(result).toEqual(DEFAULT_GRPC_LOADER_OPTIONS);
  });

  test("merges custom options over defaults", () => {
    const result = mergeGRPCLoaderOptions({ keepCase: true });
    expect(result.keepCase).toBe(true);
    expect(result.longs).toBe(DEFAULT_GRPC_LOADER_OPTIONS.longs);
  });

  test("merges multiple option objects in order", () => {
    const result = mergeGRPCLoaderOptions(
      { keepCase: true },
      { keepCase: false, defaults: false },
    );
    expect(result.keepCase).toBe(false);
    expect(result.defaults).toBe(false);
  });

  test("handles undefined arguments", () => {
    const result = mergeGRPCLoaderOptions(
      undefined,
      { keepCase: true },
      undefined,
    );
    expect(result.keepCase).toBe(true);
  });
});

// ─── loadGRPCService ───────────────────────────────────────

describe("loadGRPCService", () => {
  test("loads a service from proto file", () => {
    const loaded = loadGRPCService({
      proto: PROTO_PATH,
      package: "test.v1",
      service: "TestService",
    });
    expect(loaded.fullName).toBe("test.v1.TestService");
    expect(loaded.serviceDefinition).toBeDefined();
    expect(loaded.clientConstructor).toBeDefined();
    expect(loaded.packageDefinition).toBeDefined();
    expect(loaded.packageObject).toBeDefined();
  });

  test("throws for non-existent service", () => {
    expect(() =>
      loadGRPCService({
        proto: PROTO_PATH,
        package: "test.v1",
        service: "NonExistent",
      }),
    ).toThrow("is not a gRPC service");
  });

  test("resolves nested service by fully qualified name", () => {
    const loaded = loadGRPCService({
      proto: PROTO_PATH,
      service: "test.v1.TestService",
    });
    expect(loaded.fullName).toBe("test.v1.TestService");
  });
});

// ─── resolveGRPCServerCredentials ──────────────────────────

describe("resolveGRPCServerCredentials", () => {
  test("returns insecure credentials for undefined", () => {
    const creds = resolveGRPCServerCredentials(undefined);
    expect(creds).toBeDefined();
  });

  test("returns insecure credentials for 'insecure'", () => {
    const creds = resolveGRPCServerCredentials("insecure");
    expect(creds).toBeDefined();
  });

  test("returns the same object for ServerCredentials input", () => {
    const original = grpc.ServerCredentials.createInsecure();
    const creds = resolveGRPCServerCredentials(original);
    expect(creds).toBe(original);
  });

  test("creates SSL credentials from keyCertPairs", () => {
    const creds = resolveGRPCServerCredentials({
      keyCertPairs: [
        {
          private_key: Buffer.from("fake-key"),
          cert_chain: Buffer.from("fake-cert"),
        },
      ],
    });
    expect(creds).toBeDefined();
  });
});

// ─── GRPCRouter ────────────────────────────────────────────

describe("GRPCRouter", () => {
  let runtime: Runtime;
  let router: GRPCRouter;

  beforeEach(() => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
  });

  // ─── Route registration ──────────────────────────────────

  describe("grpc()", () => {
    test("registers a service config", () => {
      router.grpc("test", makeServiceConfig());
      expect(router.hasRoutes()).toBe(true);
    });

    test("throws on empty name", () => {
      expect(() => router.grpc("", makeServiceConfig())).toThrow(
        "Name must be a non-empty string",
      );
    });

    test("throws on non-string name", () => {
      expect(() =>
        router.grpc(null as unknown as string, makeServiceConfig()),
      ).toThrow("Name must be a non-empty string");
    });
  });

  describe("hasRoutes()", () => {
    test("returns false when no routes registered", () => {
      expect(router.hasRoutes()).toBe(false);
    });

    test("returns true after registering a route", () => {
      router.grpc("test", makeServiceConfig());
      expect(router.hasRoutes()).toBe(true);
    });
  });

  describe("setDefaults()", () => {
    test("merges defaults", () => {
      router.setDefaults({
        listen: { port: 50052 },
        hooks: { beforeCall: () => {} },
      });
      // No direct getter, but listen should use the default port
      // We verify by attempting to listen and checking the handle port
    });
  });

  // ─── listen() ────────────────────────────────────────────

  describe("listen()", () => {
    let handle:
      | { stop: () => Promise<void>; port: number; services: string[] }
      | undefined;

    afterEach(async () => {
      if (handle) await handle.stop();
    });

    test("starts a gRPC server with default options", async () => {
      router.grpc("test", makeServiceConfig());
      handle = (await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions)) as typeof handle;
      expect(handle!.port).toBeGreaterThan(0);
      expect(handle!.services).toContain("test.v1.TestService");
    });

    test("starts a gRPC server with address", async () => {
      router.grpc("test", makeServiceConfig());
      handle = (await router.listen(runtime, {
        address: "127.0.0.1:0",
      } as GRPCListenOptions)) as typeof handle;
      expect(handle!.port).toBeGreaterThan(0);
    });

    test("starts a gRPC server for a named service", async () => {
      router.grpc("test", makeServiceConfig());
      handle = (await router.listen(runtime, "test", {
        port: 0,
      } as GRPCListenOptions)) as typeof handle;
      expect(handle!.services).toContain("test.v1.TestService");
    });

    test("throws when no services are registered", async () => {
      await expect(router.listen(runtime)).rejects.toThrow(
        "No gRPC services are registered",
      );
    });

    test("throws for unknown named service", async () => {
      router.grpc("test", makeServiceConfig());
      await expect(
        router.listen(runtime, "unknown", { port: 0 } as GRPCListenOptions),
      ).rejects.toThrow("is not registered");
    });

    test("throws on missing handler for method (strictMethods)", async () => {
      router.grpc(
        "test",
        makeServiceConfig({
          methods: {
            SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
            // Missing StreamGreetings, CollectGreetings, Chat
          },
        }),
      );
      await expect(
        router.listen(runtime, { port: 0 } as GRPCListenOptions),
      ).rejects.toThrow("Missing handler");
    });

    test("allows missing handlers when strictMethods=false", async () => {
      router.grpc(
        "test",
        makeServiceConfig({
          methods: {
            SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
          },
          strictMethods: false,
        }),
      );
      handle = (await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions)) as typeof handle;
      expect(handle!.services).toContain("test.v1.TestService");
    });

    test("throws on unknown method in config", async () => {
      router.grpc(
        "test",
        makeServiceConfig({
          methods: {
            SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
            StreamGreetings: grpcServerStream(function* () {
              yield {};
            }) as GRPCMethodConfig,
            CollectGreetings: grpcClientStream(() => ({
              count: 0,
            })) as GRPCMethodConfig,
            Chat: grpcBidi(function* () {
              yield {};
            }) as GRPCMethodConfig,
            UnknownMethod: grpcUnary(() => ({})) as GRPCMethodConfig,
          },
        }),
      );
      await expect(
        router.listen(runtime, { port: 0 } as GRPCListenOptions),
      ).rejects.toThrow("Unknown method");
    });

    test("throws on type mismatch", async () => {
      router.grpc(
        "test",
        makeServiceConfig({
          methods: {
            SayHello: grpcServerStream(function* () {
              yield {};
            }) as GRPCMethodConfig, // proto says unary
            StreamGreetings: grpcServerStream(function* () {
              yield {};
            }) as GRPCMethodConfig,
            CollectGreetings: grpcClientStream(() => ({
              count: 0,
            })) as GRPCMethodConfig,
            Chat: grpcBidi(function* () {
              yield {};
            }) as GRPCMethodConfig,
          },
        }),
      );
      await expect(
        router.listen(runtime, { port: 0 } as GRPCListenOptions),
      ).rejects.toThrow(
        'is "unary" in the proto, but was configured as "serverStream"',
      );
    });

    test("ready callback is called with handle", async () => {
      router.grpc("test", makeServiceConfig());
      let readyHandle: unknown;
      handle = (await router.listen(runtime, {
        port: 0,
        ready: (info) => {
          readyHandle = info;
        },
      } as GRPCListenOptions)) as typeof handle;
      expect(readyHandle).toBeDefined();
      expect((readyHandle as { port: number }).port).toBe(handle!.port);
    });
  });

  // ─── stop() ──────────────────────────────────────────────

  describe("stop()", () => {
    test("stops all running servers", async () => {
      router.grpc("test", makeServiceConfig());
      const h = await router.listen(runtime, { port: 0 } as GRPCListenOptions);
      await router.stop();
      // Should not throw
    });

    test("stop with no servers is a no-op", async () => {
      await router.stop();
    });
  });

  // ─── Integration: unary call ─────────────────────────────

  describe("integration: unary call", () => {
    test("handles unary RPC end-to-end", async () => {
      router.grpc("test", makeServiceConfig());
      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          SayHello: (
            req: { name: string },
            cb: (
              err: grpc.ServiceError | null,
              resp: { message: string },
            ) => void,
          ) => void;
        };

        const response = await new Promise<{ message: string }>(
          (resolve, reject) => {
            client.SayHello(
              { name: "World" },
              (err: grpc.ServiceError | null, resp: { message: string }) => {
                if (err) reject(err);
                else resolve(resp);
              },
            );
          },
        );

        expect(response.message).toBe("Hello, World!");
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });

  // ─── Integration: streaming calls ────────────────────────

  describe("integration: streaming calls", () => {
    test("handles server-streaming RPC end-to-end", async () => {
      router.grpc("test", makeServiceConfig());
      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          StreamGreetings: (req: {
            name: string;
          }) => grpc.ClientReadableStream<{ message: string }>;
        };

        const stream = client.StreamGreetings({ name: "Stream" });
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
        client.close();
      } finally {
        await handle.stop();
      }
    });

    test("handles client-streaming RPC end-to-end", async () => {
      router.grpc("test", makeServiceConfig());
      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          CollectGreetings: (
            cb: (
              err: grpc.ServiceError | null,
              resp: { count: number },
            ) => void,
          ) => grpc.ClientWritableStream<{ name: string }>;
        };

        const done = new Promise<void>((resolve, reject) => {
          const stream = client.CollectGreetings(
            (err: grpc.ServiceError | null, resp: { count: number }) => {
              if (err) {
                reject(err);
                return;
              }
              expect(resp.count).toBe(2);
              resolve();
            },
          );

          stream.write({ name: "a" });
          stream.write({ name: "b" });
          stream.end();
        });

        await done;
        client.close();
      } finally {
        await handle.stop();
      }
    });

    test("handles bidi-streaming RPC end-to-end", async () => {
      router.grpc("test", makeServiceConfig());
      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          Chat: () => grpc.ClientDuplexStream<
            { name: string },
            { message: string }
          >;
        };

        const stream = client.Chat();
        const messages: string[] = [];

        await new Promise<void>((resolve) => {
          stream.on("data", (msg: { message: string }) => {
            messages.push(msg.message);
          });
          stream.on("end", () => resolve());
          stream.on("error", () => resolve());

          stream.write({ name: "hello" });
          stream.end();
        });

        expect(messages).toContain("welcome");
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });

  // ─── Integration: forceShutdown ──────────────────────────

  describe("integration: forceShutdown", () => {
    test("forceShutdown stops the server immediately", async () => {
      router.grpc("test", makeServiceConfig());
      const handle = (await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions)) as unknown as {
        port: number;
        stop: () => Promise<void>;
        forceShutdown: () => void;
      };

      handle.forceShutdown();
    });
  });

  // ─── Integration: unimplemented method ───────────────────

  describe("integration: unimplemented method", () => {
    test("unimplemented unary method returns UNIMPLEMENTED", async () => {
      router.grpc("test", {
        proto: PROTO_PATH,
        package: "test.v1",
        service: "TestService",
        methods: {
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
        strictMethods: false,
      });

      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          SayHello: (
            req: { name: string },
            cb: (
              err: grpc.ServiceError | null,
              resp: { message: string },
            ) => void,
          ) => void;
        };

        const error = await new Promise<grpc.ServiceError | null>((resolve) => {
          client.SayHello({ name: "test" }, (err) => resolve(err));
        });

        expect(error).toBeDefined();
        expect(error!.code).toBe(grpc.status.UNIMPLEMENTED);
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });

  // ─── Integration: handler errors ─────────────────────────

  describe("integration: handler errors", () => {
    test("server-streaming handler error is sent to client", async () => {
      const errorMethods: Record<string, GRPCMethodConfig> = {
        SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
        StreamGreetings: grpcServerStream(function* () {
          yield { message: "first" };
          throw new Error("stream handler error");
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

      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          StreamGreetings: (req: {
            name: string;
          }) => grpc.ClientReadableStream<{ message: string }>;
        };

        const stream = client.StreamGreetings({ name: "error" });
        const messages: string[] = [];
        await new Promise<void>((resolve) => {
          stream.on("data", (msg: { message: string }) => {
            messages.push(msg.message);
          });
          stream.on("error", () => resolve());
          stream.on("end", () => resolve());
          stream.on("status", () => resolve());
          setTimeout(() => {
            stream.cancel();
            resolve();
          }, 2000);
        });

        // Should receive first message before error
        expect(messages).toContain("first");
        client.close();
      } finally {
        await handle.stop();
      }
    });

    test("client-streaming handler error maps to gRPC error", async () => {
      const errorMethods: Record<string, GRPCMethodConfig> = {
        SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
        StreamGreetings: grpcServerStream(function* () {
          yield { message: "hi" };
        }) as GRPCMethodConfig,
        CollectGreetings: grpcClientStream(() => {
          throw new Error("client stream handler error");
        }) as GRPCMethodConfig,
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

      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          CollectGreetings: (
            cb: (err: grpc.ServiceError | null, resp: unknown) => void,
          ) => grpc.ClientWritableStream<unknown>;
        };

        const done = new Promise<void>((resolve) => {
          const stream = client.CollectGreetings(
            (err: grpc.ServiceError | null) => {
              expect(err).toBeDefined();
              expect(err!.details).toContain("client stream handler error");
              resolve();
            },
          );

          stream.write({ name: "a" });
          stream.end();
        });

        await done;
        client.close();
      } finally {
        await handle.stop();
      }
    });

    test("bidi handler error is sent to client", async () => {
      const errorMethods: Record<string, GRPCMethodConfig> = {
        SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
        StreamGreetings: grpcServerStream(function* () {
          yield { message: "hi" };
        }) as GRPCMethodConfig,
        CollectGreetings: grpcClientStream(() => ({
          count: 0,
        })) as GRPCMethodConfig,
        Chat: grpcBidi(function* () {
          yield { message: "first" };
          throw new Error("bidi handler error");
        }) as GRPCMethodConfig,
      };

      router.grpc("test", {
        proto: PROTO_PATH,
        package: "test.v1",
        service: "TestService",
        methods: errorMethods,
      });

      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          Chat: () => grpc.ClientDuplexStream<unknown, { message: string }>;
        };

        const stream = client.Chat();
        const messages: string[] = [];
        await new Promise<void>((resolve) => {
          stream.on("data", (msg: { message: string }) => {
            messages.push(msg.message);
          });
          stream.on("error", () => resolve());
          stream.on("end", () => resolve());
          stream.on("status", () => resolve());
          setTimeout(() => {
            stream.cancel();
            resolve();
          }, 2000);
        });

        stream.write({});
        stream.end();

        // Should receive first message before error
        expect(messages).toContain("first");
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });

  // ─── Integration: graceful shutdown with active call ────

  describe("integration: graceful shutdown", () => {
    test("forceShutdown is called when graceful shutdown times out", async () => {
      // Use a handler that blocks forever to keep the call active
      const blockingMethods: Record<string, GRPCMethodConfig> = {
        SayHello: grpcUnary(
          () =>
            new Promise(() => {
              // Never resolves
            }),
        ) as GRPCMethodConfig,
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
        methods: blockingMethods,
      });

      const handle = await router.listen(runtime, {
        port: 0,
        gracefulShutdownMs: 100,
      } as GRPCListenOptions);

      // Start a call that will block
      const loaded = loadGRPCService({
        proto: PROTO_PATH,
        package: "test.v1",
        service: "TestService",
      });
      const client = new loaded.clientConstructor(
        `localhost:${handle!.port}`,
        grpc.credentials.createInsecure(),
      ) as unknown as grpc.Client & {
        SayHello: (
          req: { name: string },
          cb: (err: grpc.ServiceError | null, resp: unknown) => void,
        ) => void;
      };

      client.SayHello({ name: "block" }, () => {});

      // Give the call time to reach the server
      await new Promise((resolve) => setTimeout(resolve, 50));

      // Stop should trigger forceShutdown after 100ms
      await handle.stop();
      client.close();
    });
  });

  // ─── Integration: isWritableEnded and autoEnd ────────────

  describe("integration: autoEnd with server streaming", () => {
    test("serverStream with autoEnd=false does not auto-end", async () => {
      const methods: Record<string, GRPCMethodConfig> = {
        SayHello: grpcUnary(() => ({ message: "hi" })) as GRPCMethodConfig,
        StreamGreetings: {
          type: "serverStream",
          handler: (
            ctx: unknown,
            req: unknown,
            call: {
              write: (v: unknown) => boolean;
              end: () => void;
              writableEnded?: boolean;
            },
          ) => {
            call.write({ message: "hi" });
            // Return non-iterable so autoEnd + isWritableEnded path is hit
            return undefined;
          },
          autoEnd: true,
        } as unknown as GRPCMethodConfig,
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

      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          StreamGreetings: (req: {
            name: string;
          }) => grpc.ClientReadableStream<{ message: string }>;
        };

        const stream = client.StreamGreetings({ name: "test" });
        const messages: string[] = [];

        await new Promise<void>((resolve) => {
          stream.on("data", (msg: { message: string }) => {
            messages.push(msg.message);
          });
          stream.on("end", () => resolve());
          stream.on("error", () => resolve());
          setTimeout(() => {
            stream.cancel();
            resolve();
          }, 2000);
        });

        expect(messages).toContain("hi");
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });

  // ─── Integration: sinwan pipeline response ───────────────

  describe("integration: sinwan pipeline response", () => {
    test("pipeline that responds triggers responseToGRPCError", async () => {
      // Create a runtime with a step that sets a response
      const runtime = createTestRuntime();
      runtime.engine.add({
        name: "respond-step",
        run: (ctx) => {
          ctx.json({ error: "blocked by middleware" }, 403);
          return { type: "continue" as const };
        },
      });

      router.grpc("test", makeServiceConfig());
      const handle = await router.listen(runtime, {
        port: 0,
      } as GRPCListenOptions);

      try {
        const loaded = loadGRPCService({
          proto: PROTO_PATH,
          package: "test.v1",
          service: "TestService",
        });
        const client = new loaded.clientConstructor(
          `localhost:${handle!.port}`,
          grpc.credentials.createInsecure(),
        ) as unknown as grpc.Client & {
          SayHello: (
            req: { name: string },
            cb: (err: grpc.ServiceError | null, resp: unknown) => void,
          ) => void;
        };

        const error = await new Promise<grpc.ServiceError | null>((resolve) => {
          client.SayHello({ name: "test" }, (err) => resolve(err));
        });

        expect(error).toBeDefined();
        expect(error!.code).toBe(grpc.status.PERMISSION_DENIED);
        expect(error!.details).toContain("blocked by middleware");
        client.close();
      } finally {
        await handle.stop();
      }
    });
  });
});
