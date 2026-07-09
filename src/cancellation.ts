/**
 * SinwanJS gRPC — Cancellation
 *
 * Provides first-class cancellation support for client gRPC calls.
 * - Augments unary promises with a cancel() method
 * - Supports AbortSignal for automatic cancellation
 * - Exposes server-side cancellation detection helpers
 */

import type * as grpc from "@grpc/grpc-js";

export interface GRPCAbortable {
  cancel(): void;
}

export type GRPCAbortablePromise<T> = Promise<T> & GRPCAbortable;

/**
 * Augment a Promise with a cancel method backed by a grpc.ClientUnaryCall.
 */
export function makeAbortablePromise<T>(
  promise: Promise<T>,
  call: { cancel(): void },
): GRPCAbortablePromise<T> {
  const abortable = promise as GRPCAbortablePromise<T>;
  abortable.cancel = () => call.cancel();
  return abortable;
}

/**
 * Attach an AbortSignal to a grpc call (unary or stream).
 * When the signal fires, the call is cancelled.
 */
export function attachAbortSignal(
  signal: AbortSignal | undefined,
  call: { cancel(): void; on(event: "cancelled", listener: () => void): unknown },
): void {
  if (!signal) return;

  if (signal.aborted) {
    call.cancel();
    return;
  }

  const onAbort = (): void => {
    call.cancel();
    signal.removeEventListener("abort", onAbort);
  };

  signal.addEventListener("abort", onAbort);

  // Clean up listener when the call is cancelled by other means
  call.on("cancelled", () => {
    signal.removeEventListener("abort", onAbort);
  });
}

/**
 * Check if a gRPC server call has been cancelled.
 * Works with ServerUnaryCall, ServerWritableStream, ServerReadableStream, ServerDuplexStream.
 */
export function isCallCancelled(
  call: { cancelled?: boolean; destroyed?: boolean },
): boolean {
  if (typeof call.cancelled === "boolean") return call.cancelled;
  if (typeof call.destroyed === "boolean") return call.destroyed;
  return false;
}
