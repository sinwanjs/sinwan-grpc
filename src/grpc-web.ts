/**
 * SinwanJS gRPC — grpc-web Proxy
 *
 * Provides an HTTP/1.1 server that translates between grpc-web format
 * (used by browsers) and native gRPC (used by the backend).
 *
 * The proxy accepts POST requests with Content-Type: application/grpc-web
 * and forwards them to the gRPC server, converting the response back to
 * grpc-web format (with trailers appended as a frame).
 */

import * as http2 from "node:http2";

export interface GRPCWebProxyOptions {
  /** Port for the HTTP/1.1 proxy server. Default: 8080. */
  port?: number;
  /** Host to bind. Default: "0.0.0.0". */
  host?: string;
  /** Target gRPC server address (e.g. "localhost:50051"). */
  target: string;
  /** CORS origin. Default: "*". */
  corsOrigin?: string;
  /** Enable verbose logging. Default: false. */
  verbose?: boolean;
}

export interface GRPCWebProxyHandle {
  port: number;
  close: () => Promise<void>;
}

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers":
    "content-type, x-grpc-web, x-user-agent, grpc-timeout",
};

/**
 * Start a grpc-web proxy server.
 *
 * The proxy listens for HTTP POST requests with content type
 * `application/grpc-web` or `application/grpc-web+proto` and forwards
 * them to the target gRPC server using HTTP/2.
 */
export function startGRPCWebProxy(
  options: GRPCWebProxyOptions,
): Promise<GRPCWebProxyHandle> {
  const target = options.target;
  const corsOrigin = options.corsOrigin ?? "*";
  const verbose = options.verbose ?? false;

  const corsHeaders = {
    ...CORS_HEADERS,
    "Access-Control-Allow-Origin": corsOrigin,
  };

  const server = Bun.serve({
    port: options.port ?? 8080,
    hostname: options.host ?? "0.0.0.0",
    fetch: async (req: Request): Promise<Response> => {
      if (req.method === "OPTIONS") {
        return new Response(null, { status: 204, headers: corsHeaders });
      }

      if (req.method !== "POST") {
        return new Response("Method Not Allowed", {
          status: 405,
          headers: { "content-type": "text/plain", ...corsHeaders },
        });
      }

      const contentType = req.headers.get("content-type") ?? "";
      if (!contentType.includes("grpc-web")) {
        return new Response("Unsupported Media Type", {
          status: 415,
          headers: { "content-type": "text/plain", ...corsHeaders },
        });
      }

      const path = new URL(req.url).pathname;
      if (verbose) console.log(`[grpc-web-proxy] ${req.method} ${path}`);

      const body = Buffer.from(await req.arrayBuffer());
      return forwardToGRPC(target, path, body, req.headers, verbose);
    },
  });

  return Promise.resolve({
    port: server.port ?? options.port ?? 8080,
    close: () => {
      server.stop();
      return Promise.resolve();
    },
  });
}

/**
 * Forward a grpc-web request to the gRPC server using HTTP/2.
 */
function forwardToGRPC(
  target: string,
  path: string,
  body: Buffer,
  headers: Headers,
  verbose: boolean,
): Promise<Response> {
  return new Promise((resolve) => {
    const client = http2.connect(`http://${target}`);
    let resolved = false;

    const respond = (response: Response) => {
      if (resolved) return;
      resolved = true;
      resolve(response);
    };

    client.on("error", (err: Error) => {
      if (verbose)
        console.error(`[grpc-web-proxy] HTTP/2 error: ${err.message}`);
      respond(
        new Response(`Bad Gateway: ${err.message}`, {
          status: 502,
          headers: { "content-type": "text/plain" },
        }),
      );
      client.close();
    });

    // Strip grpc-web specific headers and set native gRPC headers
    const grpcHeaders: Record<string, string> = {
      ":method": "POST",
      ":path": path,
      ":scheme": "http",
      "content-type": "application/grpc",
      te: "trailers",
    };

    // Forward relevant headers
    const xGrpcWeb = headers.get("x-grpc-web");
    if (xGrpcWeb) grpcHeaders["x-grpc-web"] = xGrpcWeb;
    const grpcTimeout = headers.get("grpc-timeout");
    if (grpcTimeout) grpcHeaders["grpc-timeout"] = grpcTimeout;

    const stream = client.request(grpcHeaders);

    // Attach error handler before writing to prevent unhandled stream errors
    stream.on("error", (err: Error) => {
      if (verbose)
        console.error(`[grpc-web-proxy] Stream error: ${err.message}`);
      respond(
        new Response(`Stream error: ${err.message}`, {
          status: 502,
          headers: { "content-type": "text/plain" },
        }),
      );
      client.close();
    });

    // Write the request body (strip grpc-web frame, pass through as-is for now)
    stream.write(body);
    stream.end();

    const responseChunks: Buffer[] = [];
    let responseTrailers: http2.IncomingHttpHeaders | undefined;

    stream.on("response", (responseHeaders: http2.IncomingHttpHeaders) => {
      const status = responseHeaders[":status"] ?? 200;
      if (verbose)
        console.log(`[grpc-web-proxy] gRPC response status: ${status}`);
    });

    stream.on("data", (chunk: Buffer) => {
      responseChunks.push(chunk);
    });

    stream.on("trailers", (trailers: http2.IncomingHttpHeaders) => {
      responseTrailers = trailers;
    });

    stream.on("end", () => {
      const responseBody = Buffer.concat(responseChunks);

      // Convert gRPC response to grpc-web format:
      // Data frame: [1 byte flag=0][4 byte length][data]
      // Trailer frame: [1 byte flag=0x80][4 byte length][trailers as text]
      const dataFrame = Buffer.alloc(5 + responseBody.length);
      dataFrame.writeUInt8(0, 0); // 0 = data frame
      dataFrame.writeUInt32BE(responseBody.length, 1);
      responseBody.copy(dataFrame, 5);

      let trailerFrame = Buffer.alloc(0);
      if (responseTrailers) {
        const trailerText = Object.entries(responseTrailers)
          .filter(([key]) => !key.startsWith(":"))
          .map(([key, value]) => `${key}: ${value}`)
          .join("\r\n");
        const trailerBuf = Buffer.from(trailerText + "\r\n", "utf-8");
        trailerFrame = Buffer.alloc(5 + trailerBuf.length);
        trailerFrame.writeUInt8(0x80, 0); // 0x80 = trailer frame
        trailerFrame.writeUInt32BE(trailerBuf.length, 1);
        trailerBuf.copy(trailerFrame, 5);
      }

      const grpcWebBody = Buffer.concat([dataFrame, trailerFrame]);

      respond(
        new Response(grpcWebBody, {
          status: 200,
          headers: {
            "content-type": "application/grpc-web",
            "x-grpc-web": "1",
          },
        }),
      );

      client.close();
    });
  });
}
