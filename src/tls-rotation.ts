/**
 * SinwanJS gRPC — TLS Certificate Rotation
 *
 * Provides utilities for watching TLS certificate files and recreating
 * gRPC credentials when they change. Uses fs.watch for file monitoring
 * and grpc.ServerCredentials for server-side, grpc.ChannelCredentials
 * for client-side.
 */

import * as fs from "node:fs";
import * as grpc from "@grpc/grpc-js";

export interface GRPCTLSCertPaths {
  /** Path to the private key file (PEM). */
  keyPath: string;
  /** Path to the certificate chain file (PEM). */
  certPath: string;
  /** Path to the root CA certificate file (PEM). Optional. */
  caPath?: string;
}

export interface GRPCTLSCertData {
  /** Private key contents (PEM). */
  key: Buffer;
  /** Certificate chain contents (PEM). */
  cert: Buffer;
  /** Root CA certificate contents (PEM). */
  ca: Buffer | null;
}

export interface GRPCTLSCertRotationOptions {
  /** Paths to certificate files to watch. */
  paths: GRPCTLSCertPaths;
  /** Called when certificates are reloaded. Receives new credentials. */
  onRotate?: (credentials: grpc.ServerCredentials) => void;
  /** Debounce interval in ms to avoid rapid reloads. Default: 1000. */
  debounceMs?: number;
}

/**
 * Read certificate data from files.
 */
export function readCertData(paths: GRPCTLSCertPaths): GRPCTLSCertData {
  return {
    key: fs.readFileSync(paths.keyPath),
    cert: fs.readFileSync(paths.certPath),
    ca: paths.caPath ? fs.readFileSync(paths.caPath) : null,
  };
}

/**
 * Create server credentials from certificate data.
 */
export function createServerCredentialsFromData(
  data: GRPCTLSCertData,
): grpc.ServerCredentials {
  return grpc.ServerCredentials.createSsl(
    data.ca,
    [
      {
        private_key: data.key,
        cert_chain: data.cert,
      },
    ],
    false,
  );
}

/**
 * Create channel credentials from certificate data.
 */
export function createChannelCredentialsFromData(
  data: GRPCTLSCertData,
): grpc.ChannelCredentials {
  return grpc.ChannelCredentials.createSsl(data.ca, data.key, data.cert);
}

/**
 * Watch certificate files and call onRotate when they change.
 * Returns a stop function to clean up watchers.
 */
export function watchCertificates(
  options: GRPCTLSCertRotationOptions,
): () => void {
  const { paths, onRotate, debounceMs = 1000 } = options;
  let debounceTimer: ReturnType<typeof setTimeout> | undefined;

  const reload = (): void => {
    if (debounceTimer) clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => {
      try {
        const data = readCertData(paths);
        const credentials = createServerCredentialsFromData(data);
        onRotate?.(credentials);
      } catch {
        // Ignore read errors — will retry on next file change
      }
    }, debounceMs);
  };

  const filesToWatch = [paths.keyPath, paths.certPath];
  if (paths.caPath) filesToWatch.push(paths.caPath);

  for (const file of filesToWatch) {
    fs.watchFile(file, { interval: 500 }, (curr: fs.Stats, prev: fs.Stats) => {
      if (curr.mtimeMs !== prev.mtimeMs) {
        reload();
      }
    });
  }

  return () => {
    if (debounceTimer) clearTimeout(debounceTimer);
    for (const file of filesToWatch) fs.unwatchFile(file);
  };
}
