/**
 * SinwanJS gRPC — Channel Pooling
 *
 * Provides a pool of gRPC clients (channels) to distribute load across
 * multiple connections. Supports round-robin and least-connections
 * selection strategies.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";

export type GRPCChannelPoolStrategy = "round_robin" | "least_connections";

export interface GRPCChannelPoolOptions {
  /** Target addresses for the pool. */
  addresses: string[];
  /** Number of channels per address. Default: 1. */
  channelsPerAddress?: number;
  /** Selection strategy. Default: "round_robin". */
  strategy?: GRPCChannelPoolStrategy;
  /** Channel credentials. Default: insecure. */
  credentials?: grpc.ChannelCredentials;
  /** Channel options passed to each client. */
  channelOptions?: Record<string, unknown>;
}

interface PooledChannel {
  client: grpc.Client;
  address: string;
  activeCalls: number;
}

/**
 * A pool of gRPC clients that distributes calls across multiple channels.
 */
export class GRPCChannelPool {
  private readonly channels: PooledChannel[] = [];
  private rrIndex = 0;
  private closed = false;

  constructor(private readonly options: GRPCChannelPoolOptions) {
    const strategy = options.strategy ?? "round_robin";
    const channelsPerAddress = options.channelsPerAddress ?? 1;
    const credentials =
      options.credentials ?? grpc.credentials.createInsecure();

    for (const address of options.addresses) {
      for (let i = 0; i < channelsPerAddress; i++) {
        this.channels.push({
          client: new grpc.Client(address, credentials, options.channelOptions),
          address,
          activeCalls: 0,
        });
      }
    }

    if (this.channels.length === 0) {
      throw new Error("ChannelPool requires at least one address");
    }

    // Store strategy for selection
    this.strategy = strategy;
  }

  private readonly strategy: GRPCChannelPoolStrategy;

  /**
   * Select the next channel based on the pool strategy.
   */
  private selectChannel(): PooledChannel {
    if (this.strategy === "least_connections") {
      let min = this.channels[0]!;
      for (const ch of this.channels) {
        if (ch.activeCalls < min.activeCalls) min = ch;
      }
      return min;
    }

    // round_robin
    const channel = this.channels[this.rrIndex % this.channels.length]!;
    this.rrIndex = (this.rrIndex + 1) % this.channels.length;
    return channel;
  }

  /**
   * Get the raw grpc.Client for the next channel.
   */
  getClient(): grpc.Client {
    if (this.closed) throw new Error("ChannelPool is closed");
    return this.selectChannel().client;
  }

  /**
   * Get the address of the next channel.
   */
  getAddress(): string {
    if (this.closed) throw new Error("ChannelPool is closed");
    return this.selectChannel().address;
  }

  /**
   * Execute a unary call on the next channel.
   */
  unaryCall<RequestType, ResponseType>(
    method: string,
    request: RequestType,
    metadata?: grpc.Metadata,
  ): Promise<ResponseType> {
    if (this.closed) throw new Error("ChannelPool is closed");
    const channel = this.selectChannel();
    channel.activeCalls++;

    return new Promise<ResponseType>((resolve, reject) => {
      channel.client.makeUnaryRequest<RequestType, ResponseType>(
        method,
        (value: RequestType) => Buffer.from(JSON.stringify(value)),
        (data: Buffer) => JSON.parse(data.toString()) as ResponseType,
        request,
        metadata ?? new grpc.Metadata(),
        (err: grpc.ServiceError | null, response: ResponseType | undefined) => {
          channel.activeCalls--;
          if (err) reject(err);
          else resolve(response as ResponseType);
        },
      );
    });
  }

  /**
   * Get the number of channels in the pool.
   */
  get size(): number {
    return this.channels.length;
  }

  /**
   * Get active call counts per channel.
   */
  getActiveCallCounts(): number[] {
    return this.channels.map((ch) => ch.activeCalls);
  }

  /**
   * Close all channels in the pool.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const channel of this.channels) {
      channel.client.close();
    }
  }
}
