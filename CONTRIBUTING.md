# Contributing to sinwan-grpc

Thank you for your interest in contributing to sinwan-grpc. This document describes the architecture, conventions, and workflow we expect every contributor to follow.

---

## 1. Architecture Overview

sinwan-grpc is structured as a set of focused modules, each with a single responsibility:

```
src/
  index.ts          — Module registration + public re-exports
  server.ts         — GRPCRouter, service registration, listen(), hooks
  client.ts         — GRPCClient, unary/streaming/bidi calls, reconnection
  health.ts         — GRPCHealthChecker (grpc.health.v1.Health)
  interceptors.ts   — Interceptor types, composition, examples
  cancellation.ts   — AbortSignal integration, abortable promises
  compression.ts    — Compression config → grpc-js options mapping
  propagation.ts    — AsyncLocalStorage context, deadline extraction
  reflection.ts     — @grpc/reflection integration
  grpc-web.ts       — HTTP/1.1 grpc-web proxy server
  tracing.ts        — OpenTelemetry interceptor factory
  load-balancing.ts — LB policy config → channel options mapping
  tls-rotation.ts   — TLS cert file watching and credential reload
  channel-pool.ts   — Multi-channel pool with selection strategies
```

Each module exports its own types and functions. `index.ts` re-exports everything for the public API.

---

## 2. Design Principles

### SOLID

- **Single Responsibility**: Each module handles one feature (health, compression, tracing, etc.)
- **Open/Closed**: New features are added as new modules, not by modifying existing ones
- **Interface Segregation**: Config types (`GRPCListenOptions`, `GRPCClientConfig`) expose only what each feature needs

### TypeScript Discipline

- **No `any`** — Use `unknown` when the type isn't known, generics when it should be inferred, specific types when known
- **Define types first** — Before writing a function, define its name, return type, and parameter types
- **`Record<string, any>`** only at dynamic interop boundaries (e.g. gRPC service shapes from proto-loader)
- **Strict `tsc --noEmit`** must pass with zero errors

### Clean Code

- Meaningful names, small functions, no dead code
- No commented-out blocks, no magic numbers without explanation
- Minimal edits — prefer single-line changes when sufficient
- Encapsulation first — never reach into private internals; extend the public API

---

## 3. Development Setup

```sh
# Clone the monorepo
git clone https://github.com/sinwanjs/sinwan-grpc.git
cd sinwan-grpc

# Install dependencies
bun install

# Run tests
bun test

# Type check
bun run typecheck

# Build
bun run build
```

**Requirements**: [Bun](https://bun.sh) runtime.

---

## 4. Adding a New Feature Module

1. **Create `src/<feature>.ts`** with typed exports (interfaces, functions, classes)
2. **Define types first**: `export interface GRPC<Feature>Config`, `export type GRPC<Feature>Options`
3. **Implement the logic** with strict TypeScript — no `any`, no `as any`
4. **Add re-exports** to `src/index.ts` under a `// <Feature>` section
5. **Integrate into server/client** if needed (extend `GRPCListenOptions` or `GRPCClientConfig`)
6. **Write tests** in `__test__/<feature>.test.ts` — both unit and integration tests
7. **Run `bun test` and `bun run typecheck`** — both must pass

### Example module structure

```ts
// src/my-feature.ts

export interface GRPCMyFeatureConfig {
  enabled: boolean;
  option?: string;
}

export function resolveMyFeatureOptions(
  config?: GRPCMyFeatureConfig,
): Record<string, unknown> {
  if (!config?.enabled) return {};
  return { "grpc.my_feature": config.option ?? "default" };
}
```

---

## 5. Testing Guidelines

- **Unit tests**: Test individual functions and classes in isolation
- **Integration tests**: Start a real gRPC server with `GRPCRouter.listen()` and verify end-to-end behavior
- Use `createTestRuntime()` from `__test__/helpers.ts` for integration tests
- Every public export should have at least one test
- Test both success and error paths
- Run `bun test` before submitting — all 176 tests must pass

### Test file template

```ts
import { describe, expect, test, beforeEach, afterEach } from "bun:test";
import { myFunction } from "../src/my-feature";
import { createTestRuntime } from "./helpers";

describe("myFunction (unit)", () => {
  test("does the thing", () => {
    expect(myFunction("input")).toBe("output");
  });
});

describe("myFunction (integration)", () => {
  // Start a real server, make real calls, verify behavior
});
```

---

## 6. Commit Conventions

Use [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add TLS certificate rotation module
fix: handle undefined metadata in propagation interceptor
docs: update README with channel pooling section
test: add integration tests for grpc-web proxy
refactor: simplify compression option resolution
```

---

## 7. Pull Request Checklist

- [ ] `bun test` passes (all 176 tests)
- [ ] `bun run typecheck` passes (zero errors)
- [ ] No `any` types — use `unknown`, generics, or specific types
- [ ] New exports added to `src/index.ts`
- [ ] Tests written for new functionality
- [ ] CHANGELOG.md updated under `[Unreleased]` section
- [ ] Commit messages follow Conventional Commits

---

## Author

Mohammed Ben Cheikh

## License

MIT — see [LICENSE](./LICENSE).
