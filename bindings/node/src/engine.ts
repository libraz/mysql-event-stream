// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import { createRequire } from "node:module";
import type { ChangeEvent, ClientConfig } from "./types.js";

const require = createRequire(import.meta.url);

interface NativeAddon {
  CdcEngine: new () => NativeEngine;
  hasClient: boolean;
}

interface NativeEngine {
  feed(data: Uint8Array | Buffer): number;
  nextEvent(): ChangeEvent | null;
  hasEvents(): boolean;
  getPosition(): { file: string; offset: number };
  reset(): void;
  setMaxQueueSize(maxSize: number): void;
  setIncludeDatabases(databases: string[]): void;
  setIncludeTables(tables: string[]): void;
  setExcludeTables(tables: string[]): void;
  destroy(): void;
  enableMetadata?(config: ClientConfig): void;
}

const addon: NativeAddon = require("../build/Release/mes-node.node");

/** Native N-API based CDC engine for parsing MySQL 8.4 binlog streams. */
export class CdcEngine {
  private engine: NativeEngine | null;

  constructor() {
    this.engine = new addon.CdcEngine();
  }

  /** Create a new CDC engine instance (async for backward compatibility). */
  static async create(): Promise<CdcEngine> {
    return new CdcEngine();
  }

  /** Feed raw binlog bytes into the engine. Returns number of bytes consumed. */
  feed(data: Uint8Array): number {
    this.ensureNotDestroyed();
    if (data.length === 0) return 0;
    return this.engine!.feed(data);
  }

  /** Get the next change event, or null if no events are available. */
  nextEvent(): ChangeEvent | null {
    this.ensureNotDestroyed();
    return this.engine!.nextEvent();
  }

  /** Check if there are pending events. */
  hasEvents(): boolean {
    this.ensureNotDestroyed();
    return this.engine!.hasEvents();
  }

  /** Get current binlog position. */
  getPosition(): { file: string; offset: number } {
    this.ensureNotDestroyed();
    return this.engine!.getPosition();
  }

  /** Reset the engine, clearing all state. */
  reset(): void {
    this.ensureNotDestroyed();
    this.engine!.reset();
  }

  /** Set maximum event queue size for backpressure control. 0 = unlimited. */
  setMaxQueueSize(maxSize: number): void {
    this.ensureNotDestroyed();
    this.engine!.setMaxQueueSize(maxSize);
  }

  /** Set database include filter. Only events from these databases are processed. Empty array = all. */
  setIncludeDatabases(databases: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setIncludeDatabases(databases);
  }

  /** Set table include filter. Only events from these tables are processed. Empty array = all.
   *  Format: "database.table" or just "table" (matches any database). */
  setIncludeTables(tables: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setIncludeTables(tables);
  }

  /** Set table exclude filter. Events from these tables are skipped.
   *  Format: "database.table" or just "table" (matches any database). */
  setExcludeTables(tables: string[]): void {
    this.ensureNotDestroyed();
    this.engine!.setExcludeTables(tables);
  }

  /** Enable metadata queries for column name resolution. */
  enableMetadata(config: ClientConfig): void {
    this.ensureNotDestroyed();
    if (typeof this.engine!.enableMetadata === "function") {
      this.engine!.enableMetadata(config);
    }
  }

  /** Destroy the engine and free native resources. */
  destroy(): void {
    if (!this.engine) return;
    this.engine.destroy();
    this.engine = null;
  }

  private ensureNotDestroyed(): void {
    if (!this.engine) throw new Error("Engine has been destroyed");
  }
}
