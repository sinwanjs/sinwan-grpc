# Changelog

All notable changes to **sinwan-grpc** are documented in this file. The format follows [Keep a Changelog](https://keepachangelog.com/) and sinwan-grpc adheres to [Semantic Versioning](https://semver.org/).

## [0.1.1] — 2026-08-03 — Thin Adapter (StepEngine Removed)

Removes the `StepEngine` dependency so the gRPC router is a thin adapter over `@grpc/grpc-js` that drives Sinwan's `EventBus` directly. This aligns `sinwan-grpc` with `sinwan-engine` v1.1.0, where `StepEngine` was removed in favor of router-driven event flows.

### Changed — Core

- **`GRPCRouter`** (`src/server.ts`) — RPCs now run through the `grpc:call` `EventBus` event instead of `runtime.engine.run()`. The `runSinwanPipeline` private method was removed; response/stop checks now run inline after `emitCallStart`
- **`GRPCHooks.beforeCall`** JSDoc updated — now documented as running after the `grpc:call` bus event (previously "after the Sinwan StepEngine")
- **Module header comment** (`src/server.ts`) — updated from "runs each RPC through Sinwan's StepEngine and EventBus" to "runs each RPC through Sinwan's EventBus (grpc:call / grpc:finish / grpc:error)"

### Changed — Behavior

- A `grpc:call` listener that calls `ctx.stop()` now produces `grpc.status.PERMISSION_DENIED` with the message "gRPC call stopped by a grpc:call listener." (previously "gRPC call stopped by Sinwan pipeline.")
- A `grpc:call` listener that sets a response still triggers `responseToGRPCError`, preserving the 0.1.0 rejection contract

### Changed — Tests

- `__test__/helpers.ts` — `createTestRuntime()` no longer constructs a `StepEngine`; it now wires an `HTTPRouter` into the `Runtime` (matching `sinwan-engine` v1.1.0's `Runtime` shape). Re-exports drop `StepEngine`
- `__test__/server.test.ts` — the "sinwan pipeline response" integration suite was renamed to "grpc:call listener response" and rewritten to register a `grpc:call` bus listener instead of adding a step. Added a new test verifying that `ctx.stop()` in a `grpc:call` listener yields `PERMISSION_DENIED`

### Changed — Package

- `package.json` — bumped to `0.1.1`; metadata fields (`author`, `licenseFilename`, `bugs`, `homepage`, `repository`, `license`) moved to the top of the file, ahead of `keywords`, for consistency with other Sinwan packages

### Compatibility

- Requires `sinwan-engine` >= 1.1.0 (where `StepEngine` is no longer exported and `Runtime` accepts `httpRouter` instead of `engine`)
- No public API breakage for consumers who did not call `runtime.engine.run()` directly

### Coverage

- `bun test` passes with 215/215 tests across 14 files
- `src/server.ts` line coverage remains 100%

## [0.1.0] — 2026-07-09 — Initial Release

Production-ready gRPC support for Sinwan Engine, built on `@grpc/grpc-js` and `@grpc/proto-loader`. Implements the `GRPCProvider` interface from `sinwan-engine` with a typed gRPC router, client, and 11 feature modules.

### Added — Core

- **`GRPCRouter`** (`src/server.ts`) — Typed gRPC router with service registration, `listen()`, graceful shutdown, per-service and listen-level configuration, hooks (`before`/`after`/`error`), and proto-loader integration
- **`GRPCClient`** (`src/client.ts`) — Typed gRPC client supporting unary, server-streaming, client-streaming, and bidi calls with reconnection, connection state events, and call options
- **`sinwanGRPC` module** (`src/index.ts`) — `SinwanModule` that registers the gRPC provider with `sinwan-engine`, enabling `app.grpc()`, `app.listenGRPC()`, and `app.connectGRPC()`

### Added — Feature Modules

- **Health checks** (`src/health.ts`) — `GRPCHealthChecker` implementing `grpc.health.v1.Health` protocol with per-service status management, `Watch` streaming, and `Shutdown` support. Enabled via `listenGRPC({ health: true })`
- **Interceptors** (`src/interceptors.ts`) — `GRPCServerInterceptor` and `GRPCClientInterceptor` types, `composeServerInterceptors`/`composeClientInterceptors` helpers, `createLoggingInterceptor` and `createMetadataInjectionInterceptor` examples
- **Cancellation** (`src/cancellation.ts`) — `makeAbortablePromise` for augmenting Promises with cancel, `attachAbortSignal` for connecting `AbortSignal` to grpc-js calls, `isCallCancelled` for server-side detection. Integrated into all client streaming methods
- **Compression** (`src/compression.ts`) — `GRPCCompressionConfig` type and `resolveCompressionOptions` mapping `gzip`/`deflate`/`none` to `grpc.compressionAlgorithms`. Merged into server and client channel options
- **Deadline & metadata propagation** (`src/propagation.ts`) — `AsyncLocalStorage`-based context with `runWithPropagationContext`/`getPropagationContext`, `extractDeadlineFromMetadata` parsing `grpc-timeout` header, `createPropagationInterceptor` client interceptor injecting metadata via `start` listener. Server wraps handler execution with propagation context
- **Server reflection** (`src/reflection.ts`) — `createReflectionService` and `addReflectionToServer` integrating `@grpc/reflection` for `grpc.reflection.v1alpha.ServerReflection`. Enabled via `listenGRPC({ reflection: true })`
- **grpc-web proxy** (`src/grpc-web.ts`) — `startGRPCWebProxy` HTTP/1.1 server translating between grpc-web format and native gRPC via HTTP/2, with CORS headers, frame conversion (data + trailer frames), and content-type validation
- **OpenTelemetry tracing** (`src/tracing.ts`) — `createTracingServerInterceptor` and `createTracingClientInterceptor` creating OTel spans with `SpanKind.SERVER`/`CLIENT`, trace context extraction/injection via `api.propagation`, and status attribution on OK/error
- **Load balancing** (`src/load-balancing.ts`) — `GRPCLoadBalancingConfig` type, `resolveLoadBalancingOptions` mapping to `grpc.lb_policy_name` channel option, `formatLoadBalancingTarget` for multi-address `dns:///` targeting. Supports `pick_first`, `round_robin`, `grpclb`
- **TLS certificate rotation** (`src/tls-rotation.ts`) — `readCertData`, `createServerCredentialsFromData`, `createChannelCredentialsFromData`, and `watchCertificates` using `fs.watchFile` polling with debounce for reliable cross-platform file change detection
- **Channel pooling** (`src/channel-pool.ts`) — `GRPCChannelPool` class with `round_robin` and `least_connections` selection strategies, `channelsPerAddress` multiplier, active call tracking, `unaryCall` helper, and `close()` cleanup

### Added — Integration

- `GRPCListenOptions` extended with `health`, `interceptors`, `compression`, `propagation`, and `reflection` options
- `GRPCClientConfig` extended with `interceptors`, `compression`, and `propagation` options
- All feature modules re-exported from `src/index.ts`

### Added — Tests

- **214 tests across 14 test files**, all passing:
  - `server.test.ts` (61 tests) — core router, service registration, streaming, hooks, graceful shutdown, error mapping
  - `client.test.ts` (28 tests) — typed client with unary, streaming, bidi, reconnection, and interceptors
  - `health.test.ts` (21 tests) — `GRPCHealthChecker` with Check, Watch, Shutdown, and integration
  - `propagation.test.ts` (18 tests) — context propagation, deadline extraction, and interceptor
  - `cancellation.test.ts` (17 tests) — abortable promises, abort signal, and call cancellation
  - `channel-pool.test.ts` (17 tests) — channel pool with round-robin, least-connections, and cleanup
  - `interceptors.test.ts` (14 tests) — interceptor composition, logging, and metadata injection
  - `compression.test.ts` (13 tests) — compression option resolution for server and client
  - `load-balancing.test.ts` (13 tests) — LB policy configuration and target formatting
  - `tracing.test.ts` (12 tests) — OTel tracing interceptors for server and client
  - `index.test.ts` (10 tests) — module registration and re-exports
  - `grpc-web.test.ts` (10 tests) — grpc-web proxy with CORS, forwarding, and error handling
  - `reflection.test.ts` (7 tests) — server reflection service and integration
  - `tls-rotation.test.ts` (6 tests) — certificate reading, credentials creation, and file watching

### Coverage

- `tsc --noEmit` passes with zero errors
- `bun test` passes with 214/214 tests across 14 files
