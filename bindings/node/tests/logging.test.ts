// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { spawnSync } from "node:child_process";
import { Worker } from "node:worker_threads";
import { afterEach, describe, expect, it } from "vitest";
import { CdcEngine } from "../src/engine.js";
import { LogLevel, setLogCallback } from "../src/logging.js";

const builtLoggingModule = new URL("../dist/logging.js", import.meta.url);

function oversizedEventHeader(): Uint8Array {
  const event = new Uint8Array(19);
  event[4] = 19; // TABLE_MAP_EVENT
  const view = new DataView(event.buffer);
  view.setUint32(5, 1, true); // server_id
  view.setUint32(9, 1024 * 1024 * 1024, true); // event_length > configured maximum
  return event;
}

describe("setLogCallback", () => {
  afterEach(() => {
    // Always detach so a registered callback never leaks into later tests.
    setLogCallback(null);
  });

  it("exposes the log level constants", () => {
    expect(LogLevel.Error).toBe(0);
    expect(LogLevel.Warn).toBe(1);
    expect(LogLevel.Info).toBe(2);
    expect(LogLevel.Debug).toBe(3);
  });

  it("accepts a handler without throwing", () => {
    expect(() => setLogCallback(() => {}, LogLevel.Debug)).not.toThrow();
  });

  it("accepts null to detach without throwing", () => {
    setLogCallback(() => {});
    expect(() => setLogCallback(null)).not.toThrow();
  });

  it("can be re-registered repeatedly", () => {
    expect(() => {
      setLogCallback(() => {});
      setLogCallback(() => {});
      setLogCallback(null);
    }).not.toThrow();
  });

  it("rejects a non-function, non-null argument", () => {
    // @ts-expect-error intentional misuse
    expect(() => setLogCallback(42)).toThrow();
  });

  it("does not keep a child process alive when a handler remains registered", () => {
    const child = spawnSync(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        `import { setLogCallback } from ${JSON.stringify(builtLoggingModule.href)}; setLogCallback(() => {});`,
      ],
      { timeout: 2_000, encoding: "utf8" },
    );

    expect(child.error).toBeUndefined();
    expect(child.signal).toBeNull();
    expect(child.status, child.stderr).toBe(0);
  });

  it("allows a worker to tear down with a registered handler", async () => {
    const worker = new Worker(
      `
        const { parentPort } = require("node:worker_threads");
        import(${JSON.stringify(builtLoggingModule.href)}).then(({ setLogCallback }) => {
          setLogCallback(() => {});
          parentPort.postMessage("ready");
        });
      `,
      { eval: true },
    );

    await new Promise<void>((resolve, reject) => {
      let ready = false;
      const timer = setTimeout(() => {
        void worker.terminate();
        reject(new Error("worker did not exit after registering a log callback"));
      }, 2_000);
      worker.on("message", (message) => {
        if (message === "ready") ready = true;
      });
      worker.on("error", (error) => {
        clearTimeout(timer);
        reject(error);
      });
      worker.on("exit", (code) => {
        clearTimeout(timer);
        if (!ready) {
          reject(new Error(`worker exited before registration completed (code ${code})`));
        } else if (code !== 0) {
          reject(new Error(`worker exited with code ${code}`));
        } else {
          resolve();
        }
      });
    });
  });

  it("bounds a blocked event-loop log flood and reports dropped records", async () => {
    const messages: string[] = [];
    setLogCallback((_level, message) => messages.push(message), LogLevel.Debug);
    const engine = new CdcEngine();
    const invalidEvent = oversizedEventHeader();

    try {
      // This synchronous loop prevents JS callbacks from draining while native
      // parse errors try to enqueue log records.
      for (let i = 0; i < 2_000; i++) {
        expect(() => engine.feed(invalidEvent)).toThrow();
        engine.reset();
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      const deliveredBeforeRecovery = messages.length;
      expect(deliveredBeforeRecovery).toBeGreaterThan(0);
      expect(deliveredBeforeRecovery).toBeLessThanOrEqual(256);

      expect(() => engine.feed(invalidEvent)).toThrow();
      engine.reset();
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
      expect(
        messages.some((message) => message.startsWith("event=node_log_queue_overflow dropped=")),
      ).toBe(true);
    } finally {
      engine.destroy();
    }
  });
});
