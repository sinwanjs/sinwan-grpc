import {
  Context,
  EventBus,
  ErrorHandler,
  HTTPRouter,
  Runtime,
} from "sinwan-engine";
import type { Server } from "bun";

/**
 * Create a real Runtime instance for integration tests.
 * Uses actual EventBus, ErrorHandler, and HTTPRouter.
 */
export function createTestRuntime(): Runtime {
  const bus = new EventBus();
  const errorHandler = new ErrorHandler();
  const globalState = new Map<string, unknown>();
  const httpRouter = new HTTPRouter();

  return new Runtime({
    bus,
    errorHandler,
    globalState,
    httpRouter,
    maxPoolSize: 10,
  });
}

/**
 * Create a mock Server for testing.
 */
export function createMockServer(): Server<unknown> {
  return {} as Server<unknown>;
}

export { Context, EventBus, ErrorHandler };

// ─── Test for helpers ──────────────────────────────────────

import { describe, expect, test } from "bun:test";

describe("helpers", () => {
  test("createTestRuntime returns a Runtime instance", () => {
    const runtime = createTestRuntime();
    expect(runtime).toBeInstanceOf(Runtime);
  });

  test("createMockServer returns a Server object", () => {
    const server = createMockServer();
    expect(server).toBeDefined();
    expect(typeof server).toBe("object");
  });

  test("re-exports Context, EventBus, ErrorHandler", () => {
    expect(Context).toBeDefined();
    expect(EventBus).toBeDefined();
    expect(ErrorHandler).toBeDefined();
  });
});
