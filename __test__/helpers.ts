import {
  Context,
  EventBus,
  ErrorHandler,
  StepEngine,
  Runtime,
} from "sinwan-engine";
import type { Server } from "bun";

/**
 * Create a real Runtime instance for integration tests.
 * Uses actual StepEngine, EventBus, and ErrorHandler.
 */
export function createTestRuntime(): Runtime {
  const engine = new StepEngine();
  const bus = new EventBus();
  const errorHandler = new ErrorHandler();
  const globalState = new Map<string, unknown>();

  return new Runtime({
    engine,
    bus,
    errorHandler,
    globalState,
    maxPoolSize: 10,
  });
}

/**
 * Create a mock Server for testing.
 */
export function createMockServer(): Server<unknown> {
  return {} as Server<unknown>;
}

export { Context, EventBus, ErrorHandler, StepEngine };

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

  test("re-exports Context, EventBus, ErrorHandler, StepEngine", () => {
    expect(Context).toBeDefined();
    expect(EventBus).toBeDefined();
    expect(ErrorHandler).toBeDefined();
    expect(StepEngine).toBeDefined();
  });
});
