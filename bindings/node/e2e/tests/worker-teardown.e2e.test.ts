// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { execFile } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const ADDON_PATH = resolve(
  fileURLToPath(new URL("../../build/Release/mes-node.node", import.meta.url)),
);

// A Worker streams from the server and re-polls the moment each heartbeat
// arrives. The host terminates it shortly after, while that poll waits on the
// next heartbeat, and reports how long the termination took.
const HOST_SCRIPT = `
const { Worker } = require("node:worker_threads");

const workerCode = \`
  const { parentPort, workerData } = require("node:worker_threads");
  const addon = require(workerData.addon);
  const client = new addon.BinlogClient();
  client.connect({
    host: "127.0.0.1",
    port: 13307,
    user: "root",
    password: "test_root_password",
    serverId: 112,
    connectTimeoutS: 10,
    readTimeoutS: 60,
    allowPublicKeyRetrieval: true,
  });
  client.start();
  const loop = () =>
    client.poll().then(
      (result) => {
        if (result.isHeartbeat) parentPort.postMessage("heartbeat");
        loop();
      },
      (error) => parentPort.postMessage("error: " + error.message),
    );
  loop();
\`;

const worker = new Worker(workerCode, { eval: true, workerData: { addon: process.env.MES_ADDON } });
let terminatedAt = 0;
worker.on("message", (message) => {
  if (message !== "heartbeat") {
    console.log(message);
    return;
  }
  if (terminatedAt !== 0) return;
  terminatedAt = -1;
  setTimeout(() => {
    terminatedAt = Date.now();
    worker.terminate();
  }, 200);
});
worker.on("exit", () => console.log("terminate-ms=" + (Date.now() - terminatedAt)));
`;

describe("worker termination with a poll in flight", () => {
  it("ends the worker without waiting for the next heartbeat", async () => {
    const stdout = await new Promise<string>((resolvePromise, reject) => {
      execFile(
        process.execPath,
        ["-e", HOST_SCRIPT],
        { env: { ...process.env, MES_ADDON: ADDON_PATH }, timeout: 30_000 },
        (error, out) => (error === null ? resolvePromise(out) : reject(error)),
      );
    });
    const match = /terminate-ms=(\d+)/.exec(stdout);
    expect(match, stdout).not.toBeNull();
    // Heartbeats arrive every 3 s at this read timeout; a teardown that waits
    // for the in-flight poll takes most of that.
    expect(Number(match?.[1])).toBeLessThan(1_000);
  }, 40_000);
});
