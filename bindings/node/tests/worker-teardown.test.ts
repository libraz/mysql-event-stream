// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const ADDON_PATH = resolve(
  fileURLToPath(new URL("../build/Release/mes-node.node", import.meta.url)),
);

// The host process runs a Worker that starts a metadata handshake against a
// server that never answers, then ends the Worker while the handshake is on
// the thread pool.
const HOST_SCRIPT = `
const net = require("node:net");
const { Worker } = require("node:worker_threads");

const workerCode = \`
  const { parentPort, workerData } = require("node:worker_threads");
  const addon = require(workerData.addon);
  const engine = new addon.CdcEngine();
  engine
    .enableMetadataAsync({
      host: "127.0.0.1",
      port: workerData.port,
      user: "u",
      password: "p",
      connectTimeoutS: 2,
      readTimeoutS: 2,
    })
    .catch(() => {});
  parentPort.postMessage("started");
  if (workerData.mode === "exit") setTimeout(() => process.exit(0), 100);
\`;

const server = net.createServer(() => {});
server.listen(0, "127.0.0.1", () => {
  const worker = new Worker(workerCode, {
    eval: true,
    workerData: {
      addon: process.env.MES_ADDON,
      port: server.address().port,
      mode: process.env.MES_MODE,
    },
  });
  worker.on("message", () => {
    if (process.env.MES_MODE === "terminate") setTimeout(() => worker.terminate(), 100);
  });
  worker.on("exit", () => {
    server.close();
    console.log("worker-exited");
  });
});
`;

function runHost(mode: "terminate" | "exit"): Promise<{ code: number | null; stdout: string }> {
  return new Promise((resolvePromise) => {
    execFile(
      process.execPath,
      ["-e", HOST_SCRIPT],
      { env: { ...process.env, MES_ADDON: ADDON_PATH, MES_MODE: mode }, timeout: 30_000 },
      (error, stdout) => {
        const exitCode = error === null ? 0 : (error as { code?: unknown }).code;
        resolvePromise({ code: typeof exitCode === "number" ? exitCode : null, stdout });
      },
    );
  });
}

describe("environment teardown with a native call in flight", () => {
  it("survives worker.terminate() during a metadata handshake", async () => {
    const result = await runHost("terminate");
    expect(result.stdout).toContain("worker-exited");
    expect(result.code).toBe(0);
  }, 40_000);

  it("survives process.exit() inside a worker during a metadata handshake", async () => {
    const result = await runHost("exit");
    expect(result.stdout).toContain("worker-exited");
    expect(result.code).toBe(0);
  }, 40_000);
});
