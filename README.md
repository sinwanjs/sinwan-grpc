<div align="left">
  <table border="0" width="100%" align="center">
    <tr>
      <td width="150" align="left">
        <img src="https://avatars.githubusercontent.com/u/252437356?s=400&v=4" alt="Sinwan Logo" width="150" />
      </td>
      <td align="left">
        <h1>Sinwan gRPC</h1>
        <p>Production-ready gRPC support for Sinwan Engine — typed services, health checks, interceptors, cancellation, compression, propagation, reflection, grpc-web proxy, OpenTelemetry tracing, load balancing, TLS rotation, and channel pooling.</p>
        <p>
          <a href="https://github.com/sinwanjs/sinwan-grpc/stargazers"><img src="https://img.shields.io/github/stars/sinwanjs/sinwan-grpc.svg?color=ffce3b&label=stars&logo=github" alt="GitHub stars" /></a>
          <a href="https://www.npmjs.com/package/sinwan-grpc"><img src="https://img.shields.io/npm/dm/sinwan-grpc?color=42b883&label=downloads&logo=npm" alt="NPM Downloads" /></a>
          <a href="./LICENSE"><img src="https://img.shields.io/npm/l/sinwan-grpc?color=35495e&label=license" alt="License" /></a>
        </p>
      </td>
    </tr>
  </table>
</div>

<br clear="both" />

`sinwan-grpc` implements the `GRPCProvider` interface from `sinwan-engine` using [`@grpc/grpc-js`](https://www.npmjs.com/package/@grpc/grpc-js) and [`@grpc/proto-loader`](https://www.npmjs.com/package/@grpc/proto-loader). It provides a typed gRPC router, client, and a full suite of production features — all built on Bun.

## Install

```sh
bun add sinwan-engine sinwan-grpc
```

> **Requires [Bun](https://bun.sh) runtime and [sinwan-engine](https://www.npmjs.com/package/sinwan-engine) v1.0.0+.**

## Quick Start

```ts
import { Sinwan } from "sinwan-engine";
import { sinwanGRPC } from "sinwan-grpc";

const app = await Sinwan.create();

app.register(sinwanGRPC);

app.grpc("greeter", {
  proto: "./proto/greeter.proto",
  package: "hello.v1",
  service: "Greeter",
  methods: {
    SayHello: (ctx, request: { name: string }) => ({
      message: `Hello, ${request.name}!`,
    }),
  },
});

await app.listenGRPC({ port: 50051 });
```

## Features

- **Typed gRPC services** — Define unary, server-streaming, client-streaming, and bidi handlers with full TypeScript inference
- **Health checks** — gRPC Health Checking Protocol (`grpc.health.v1.Health`) with per-service status management
- **Interceptors** — Composable server and client interceptors for cross-cutting concerns
- **Cancellation** — `AbortSignal` integration for client streaming calls and server-side cancellation detection
- **Compression** — Configurable gzip/deflate compression via grpc-js channel and server options
- **Deadline & metadata propagation** — `AsyncLocalStorage`-based context propagation with `grpc-timeout` header parsing
- **Server reflection** — gRPC Server Reflection API (`grpc.reflection.v1alpha`) for service discovery
- **grpc-web proxy** — HTTP/1.1 proxy translating between grpc-web format and native gRPC for browser clients
- **OpenTelemetry tracing** — Server and client interceptors creating and propagating OTel spans
- **Load balancing** — `pick_first`, `round_robin`, and `grpclb` policy configuration with multi-address targeting
- **TLS certificate rotation** — File-watching utilities that reload credentials without restart
- **Channel pooling** — Multi-channel pool with round-robin and least-connections selection strategies

## Development

```sh
bun test          # Run all tests (214 tests across 14 files)
bun run typecheck # TypeScript type checking
bun run build     # Build distributable
```

## Author

Mohammed Ben Cheikh

## License

MIT — see [LICENSE](./LICENSE).
