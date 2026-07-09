import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import * as http from "node:http";
import * as net from "node:net";
import {
  GRPCRouter,
  grpcUnary,
  grpcServerStream,
  grpcClientStream,
  grpcBidi,
  type GRPCServiceConfig,
  type GRPCMethodConfig,
} from "../src/server";
import { startGRPCWebProxy } from "../src/grpc-web";
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

describe("grpc-web proxy", () => {
  let runtime: Runtime;
  let router: GRPCRouter;
  let grpcHandle: { port: number; stop: () => Promise<void> } | undefined;
  let proxyHandle: { port: number; close: () => Promise<void> } | undefined;

  beforeEach(() => {
    runtime = createTestRuntime();
    router = new GRPCRouter();
  });

  afterEach(async () => {
    if (proxyHandle) await proxyHandle.close();
    if (grpcHandle) await grpcHandle.stop();
    await router.stop();
  });

  test("proxy starts and listens on a port", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    expect(proxyHandle.port).toBeGreaterThan(0);
  });

  test("proxy responds to OPTIONS with CORS headers", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    const response = await new Promise<{
      status: number;
      headers: http.IncomingHttpHeaders;
      body: string;
    }>((resolve, reject) => {
      const req = http.request(
        {
          hostname: "localhost",
          port: proxyHandle!.port,
          method: "OPTIONS",
          path: "/test.v1.TestService/SayHello",
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () =>
            resolve({
              status: res.statusCode ?? 0,
              headers: res.headers,
              body,
            }),
          );
        },
      );
      req.on("error", reject);
      req.end();
    });

    expect(response.status).toBe(204);
    expect(response.headers["access-control-allow-origin"]).toBe("*");
  });

  test("proxy rejects non-POST methods", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    const response = await new Promise<{ status: number }>(
      (resolve, reject) => {
        const req = http.request(
          {
            hostname: "localhost",
            port: proxyHandle!.port,
            method: "GET",
            path: "/test.v1.TestService/SayHello",
          },
          (res) => {
            res.resume();
            res.on("end", () => resolve({ status: res.statusCode ?? 0 }));
          },
        );
        req.on("error", reject);
        req.end();
      },
    );

    expect(response.status).toBe(405);
  });

  test("proxy rejects non-grpc-web content types", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    const response = await new Promise<{ status: number }>(
      (resolve, reject) => {
        const req = http.request(
          {
            hostname: "localhost",
            port: proxyHandle!.port,
            method: "POST",
            path: "/test.v1.TestService/SayHello",
            headers: { "content-type": "application/json" },
          },
          (res) => {
            res.resume();
            res.on("end", () => resolve({ status: res.statusCode ?? 0 }));
          },
        );
        req.on("error", reject);
        req.end();
      },
    );

    expect(response.status).toBe(415);
  });

  test("proxy forwards grpc-web request to gRPC server", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    // Create a minimal grpc-web request frame:
    // [1 byte flag=0][4 byte length][protobuf message]
    // The test.proto SayHello expects a HelloRequest with field 1 = name (string)
    // Protobuf encoding: field 1, wire type 2 (length-delimited): 0x0a, length, bytes
    const name = "WebTest";
    const nameBytes = Buffer.from(name, "utf-8");
    const protoMessage = Buffer.alloc(2 + nameBytes.length);
    protoMessage[0] = 0x0a; // field 1, wire type 2
    protoMessage[1] = nameBytes.length;
    nameBytes.copy(protoMessage, 2);

    // grpc-web frame
    const grpcWebBody = Buffer.alloc(5 + protoMessage.length);
    grpcWebBody.writeUInt8(0, 0); // data frame flag
    grpcWebBody.writeUInt32BE(protoMessage.length, 1);
    protoMessage.copy(grpcWebBody, 5);

    const response = await new Promise<{
      status: number;
      headers: http.IncomingHttpHeaders;
      body: Buffer;
    }>((resolve, reject) => {
      const req = http.request(
        {
          hostname: "localhost",
          port: proxyHandle!.port,
          method: "POST",
          path: "/test.v1.TestService/SayHello",
          headers: {
            "content-type": "application/grpc-web",
            "x-grpc-web": "1",
          },
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk: Buffer) => chunks.push(chunk));
          res.on("end", () => {
            resolve({
              status: res.statusCode ?? 0,
              headers: res.headers,
              body: Buffer.concat(chunks),
            });
          });
        },
      );
      req.on("error", reject);
      req.write(grpcWebBody);
      req.end();
    });

    expect(response.status).toBe(200);
    expect(response.headers["content-type"]).toContain("grpc-web");

    // Parse grpc-web response: first 5 bytes are frame header
    const flag = response.body.readUInt8(0);
    const length = response.body.readUInt32BE(1);
    expect(flag).toBe(0); // data frame
    expect(length).toBeGreaterThan(0);

    // The data frame contains the raw gRPC response which itself has a 5-byte frame header
    const grpcFrame = response.body.subarray(5, 5 + length);
    const grpcFlag = grpcFrame.readUInt8(0);
    const grpcLength = grpcFrame.readUInt32BE(1);
    expect(grpcFlag).toBe(0); // data frame within gRPC
    expect(grpcLength).toBeGreaterThan(0);

    // The protobuf message inside the gRPC frame
    const responseData = grpcFrame.subarray(5, 5 + grpcLength);
    // Field 1 (message), wire type 2: 0x0a
    expect(responseData[0]).toBe(0x0a);

    // Check that the response contains "Hello, WebTest!"
    const responseText = responseData.toString("utf-8");
    expect(responseText).toContain("Hello, WebTest!");
  });

  test("proxy returns 502 when target is unreachable", async () => {
    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: "127.0.0.1:65535",
    });

    const response = await new Promise<{ status: number; body: string }>(
      (resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error("timeout")), 3000);
        const req = http.request(
          {
            hostname: "localhost",
            port: proxyHandle!.port,
            method: "POST",
            path: "/test.v1.TestService/SayHello",
            headers: {
              "content-type": "application/grpc-web",
              "x-grpc-web": "1",
            },
          },
          (res) => {
            let body = "";
            res.on("data", (chunk) => (body += chunk));
            res.on("end", () => {
              clearTimeout(timeout);
              resolve({ status: res.statusCode ?? 0, body });
            });
          },
        );
        req.on("error", () => {
          clearTimeout(timeout);
          reject(new Error("request error"));
        });
        req.write(Buffer.alloc(5, 0));
        req.end();
      },
    );

    expect(response.status).toBe(502);
  });

  test("proxy handles client disconnect gracefully", async () => {
    router.grpc("test", makeServiceConfig());
    grpcHandle = await router.listen(runtime, { port: 0 });

    proxyHandle = await startGRPCWebProxy({
      port: 0,
      target: `localhost:${grpcHandle.port}`,
    });

    // Use raw socket to send partial data and abort
    // Bun.serve handles this internally — the proxy should not crash
    await new Promise<void>((resolve) => {
      const socket = new net.Socket();
      socket.connect(proxyHandle!.port, "localhost", () => {
        socket.write(
          "POST /test.v1.TestService/SayHello HTTP/1.1\r\n" +
            "Host: localhost\r\n" +
            "Content-Type: application/grpc-web\r\n" +
            "x-grpc-web: 1\r\n" +
            "Content-Length: 100\r\n" +
            "\r\n",
        );
        socket.write(Buffer.alloc(5, 0));
        setTimeout(() => socket.destroy(), 50);
      });
      socket.on("data", () => {
        socket.destroy();
        resolve();
      });
      socket.on("error", () => resolve());
      setTimeout(resolve, 2000);
    });

    // Verify the proxy is still running by making a normal request
    const response = await new Promise<{ status: number }>(
      (resolve, reject) => {
        const req = http.request(
          {
            hostname: "localhost",
            port: proxyHandle!.port,
            method: "OPTIONS",
            path: "/test.v1.TestService/SayHello",
          },
          (res) => {
            res.resume();
            res.on("end", () => resolve({ status: res.statusCode ?? 0 }));
          },
        );
        req.on("error", reject);
        req.end();
      },
    );

    expect(response.status).toBe(204);
  });
});
