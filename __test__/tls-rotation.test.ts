import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import * as fs from "node:fs";
import * as path from "node:path";
import * as grpc from "@grpc/grpc-js";
import {
  readCertData,
  createServerCredentialsFromData,
  createChannelCredentialsFromData,
  watchCertificates,
} from "../src/tls-rotation";

const KEY_PATH = "/tmp/test-key-rotation.pem";
const CERT_PATH = "/tmp/test-cert-rotation.pem";
const CA_PATH = "/tmp/test-ca-rotation.pem";

beforeAll(() => {
  if (!fs.existsSync(KEY_PATH) || !fs.existsSync(CERT_PATH)) {
    Bun.spawnSync([
      "openssl",
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-keyout",
      KEY_PATH,
      "-out",
      CERT_PATH,
      "-days",
      "1",
      "-nodes",
      "-subj",
      "/CN=localhost",
    ]);
  }
  if (!fs.existsSync(CA_PATH)) {
    Bun.spawnSync([
      "openssl",
      "req",
      "-x509",
      "-newkey",
      "rsa:2048",
      "-keyout",
      "/tmp/test-ca-key-rotation.pem",
      "-out",
      CA_PATH,
      "-days",
      "1",
      "-nodes",
      "-subj",
      "/CN=test-ca",
    ]);
  }
});

afterAll(() => {
  for (const file of [
    KEY_PATH,
    CERT_PATH,
    CA_PATH,
    "/tmp/test-ca-key-rotation.pem",
  ]) {
    if (fs.existsSync(file)) fs.unlinkSync(file);
  }
});

describe("readCertData", () => {
  test("reads certificate files from disk", () => {
    const data = readCertData({ keyPath: KEY_PATH, certPath: CERT_PATH });
    expect(data.key).toBeDefined();
    expect(data.key.length).toBeGreaterThan(0);
    expect(data.cert).toBeDefined();
    expect(data.cert.length).toBeGreaterThan(0);
    expect(data.ca).toBeNull();
  });

  test("reads CA file when provided", () => {
    const data = readCertData({
      keyPath: KEY_PATH,
      certPath: CERT_PATH,
      caPath: CA_PATH,
    });
    expect(data.ca).toBeDefined();
    expect(data.ca).not.toBeNull();
    expect(data.ca!.length).toBeGreaterThan(0);
  });
});

describe("createServerCredentialsFromData", () => {
  test("creates ServerCredentials from cert data", () => {
    const data = readCertData({ keyPath: KEY_PATH, certPath: CERT_PATH });
    const credentials = createServerCredentialsFromData(data);
    expect(credentials).toBeDefined();
  });
});

describe("createChannelCredentialsFromData", () => {
  test("creates ChannelCredentials from cert data", () => {
    const data = readCertData({ keyPath: KEY_PATH, certPath: CERT_PATH });
    const credentials = createChannelCredentialsFromData(data);
    expect(credentials).toBeDefined();
  });
});

describe("watchCertificates", () => {
  test("returns a stop function", () => {
    const stop = watchCertificates({
      paths: { keyPath: KEY_PATH, certPath: CERT_PATH },
      onRotate: () => {},
    });

    expect(typeof stop).toBe("function");
    stop();
  });

  test("calls onRotate when certificate file changes", async () => {
    const tmpCert = "/tmp/test-rotate-watch-cert.pem";
    const tmpKey = "/tmp/test-rotate-watch-key.pem";
    fs.copyFileSync(CERT_PATH, tmpCert);
    fs.copyFileSync(KEY_PATH, tmpKey);

    let rotated = false;
    const stop = watchCertificates({
      paths: { keyPath: tmpKey, certPath: tmpCert },
      debounceMs: 100,
      onRotate: () => {
        rotated = true;
      },
    });

    await new Promise((resolve) => setTimeout(resolve, 200));

    const original = fs.readFileSync(tmpCert);
    fs.writeFileSync(tmpCert, Buffer.concat([original, Buffer.from("\n")]));

    // Wait for fs.watchFile polling (500ms) + debounce (100ms) + buffer
    await new Promise((resolve) => setTimeout(resolve, 1200));

    expect(rotated).toBe(true);
    stop();

    fs.unlinkSync(tmpCert);
    fs.unlinkSync(tmpKey);
  });
});
