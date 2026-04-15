#!/usr/bin/env npx tsx
/**
 * Search Index Sync - Keep an in-memory index in sync with MySQL / MariaDB via CDC.
 *
 * Demonstrates a practical CDC use case: building and maintaining a
 * secondary index from row events. This pattern is commonly used with
 * Elasticsearch, Meilisearch, or any external search engine.
 *
 * Table setup (run in MySQL or MariaDB):
 *
 *   CREATE TABLE products (
 *       id         INT NOT NULL AUTO_INCREMENT,
 *       name       VARCHAR(200) NOT NULL,
 *       category   VARCHAR(50) NOT NULL,
 *       price      DECIMAL(10, 2) NOT NULL,
 *       stock      INT NOT NULL DEFAULT 0,
 *       is_active  TINYINT(1) NOT NULL DEFAULT 1,
 *       created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
 *       PRIMARY KEY (id),
 *       INDEX idx_category (category),
 *       INDEX idx_active_price (is_active, price)
 *   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
 *
 * Sample DML to trigger events:
 *
 *   INSERT INTO products (name, category, price, stock)
 *   VALUES ('Wireless Mouse', 'electronics', 29.99, 150);
 *
 *   INSERT INTO products (name, category, price, stock)
 *   VALUES ('USB-C Cable', 'electronics', 12.50, 500),
 *          ('Standing Desk', 'furniture', 499.00, 30);
 *
 *   UPDATE products SET price = 24.99, stock = stock - 1
 *   WHERE name = 'Wireless Mouse';
 *
 *   UPDATE products SET is_active = 0 WHERE stock = 0;
 *
 *   DELETE FROM products WHERE category = 'furniture' AND is_active = 0;
 *
 * Example output:
 *   Search Index Sync
 *     Server: 127.0.0.1:13308 (MySQL or MariaDB)
 *     Table:  products
 *
 *   Waiting for changes...
 *
 *   [INDEX] + product 1: Wireless Mouse (electronics) $29.99
 *   [INDEX] + product 2: USB-C Cable (electronics) $12.50
 *   [INDEX] + product 3: Standing Desk (furniture) $499.00
 *   [INDEX] ~ product 1: price $29.99 -> $24.99, stock 150 -> 149
 *   [INDEX] - product 3: removed from index
 *   [STATS] indexed=2 updated=1 removed=1
 *
 * Prerequisites:
 *   1a. Docker MySQL running:   cd e2e/docker && docker compose up -d
 *   1b. Or Docker MariaDB:      cd e2e/docker && docker compose -f docker-compose.mariadb.yml up -d
 *       (same port 13308 / database mes_test; flavor auto-detected)
 *   2. Build native addon:      yarn build
 *   3. Create the products table (DDL above)
 *
 * Usage:
 *   npx tsx examples/search-index-sync.ts
 */

import { CdcStream } from "../src/stream.js";
import type { ChangeEvent } from "../src/types.js";

// --- In-memory search index (simulates Elasticsearch/Meilisearch) ---

interface ProductDoc {
  id: number;
  name: string;
  category: string;
  price: number;
  stock: number;
}

const index = new Map<number, ProductDoc>();
const stats = { indexed: 0, updated: 0, removed: 0 };

function indexProduct(row: Record<string, unknown>): void {
  const doc: ProductDoc = {
    id: row.id as number,
    name: row.name as string,
    category: row.category as string,
    price: Number(row.price),
    stock: row.stock as number,
  };

  // Skip inactive products -- remove them from the index if present
  if (row.is_active === 0) {
    if (index.has(doc.id)) {
      index.delete(doc.id);
      stats.removed++;
      console.log(`  [INDEX] - product ${doc.id}: removed from index (inactive)`);
    }
    return;
  }

  const existing = index.get(doc.id);
  index.set(doc.id, doc);

  if (existing) {
    const changes: string[] = [];
    if (existing.price !== doc.price) {
      changes.push(`price $${existing.price} -> $${doc.price}`);
    }
    if (existing.stock !== doc.stock) {
      changes.push(`stock ${existing.stock} -> ${doc.stock}`);
    }
    if (existing.name !== doc.name) {
      changes.push(`name '${existing.name}' -> '${doc.name}'`);
    }
    console.log(`  [INDEX] ~ product ${doc.id}: ${changes.join(", ") || "no visible changes"}`);
    stats.updated++;
  } else {
    console.log(`  [INDEX] + product ${doc.id}: ${doc.name} (${doc.category}) $${doc.price}`);
    stats.indexed++;
  }
}

function removeProduct(row: Record<string, unknown>): void {
  const id = row.id as number;
  if (index.delete(id)) {
    console.log(`  [INDEX] - product ${id}: removed from index`);
    stats.removed++;
  }
}

// --- Event handler ---

function handleEvent(event: ChangeEvent): void {
  if (event.table !== "products") return;

  switch (event.type) {
    case "INSERT":
      if (event.after) indexProduct(event.after);
      break;
    case "UPDATE":
      if (event.after) indexProduct(event.after);
      break;
    case "DELETE":
      if (event.before) removeProduct(event.before);
      break;
  }
}

// --- Main ---

async function main(): Promise<void> {
  const stream = new CdcStream({
    host: "127.0.0.1",
    port: 13308,
    user: "root",
    password: "test_root_password",
    serverId: 97,
    startGtid: "",
  });

  let running = true;

  console.log("Search Index Sync");
  console.log("  Server: 127.0.0.1:13308 (MySQL or MariaDB)");
  console.log("  Table:  products\n");
  console.log("Waiting for changes...\n");

  const shutdown = () => {
    running = false;
    console.log(
      `\n  [STATS] indexed=${stats.indexed} updated=${stats.updated} removed=${stats.removed}`,
    );
    console.log(`  [STATS] index size: ${index.size} documents`);
    stream.close().catch(() => {});
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  try {
    for await (const event of stream) {
      if (!running) break;
      handleEvent(event);
    }
  } catch (err) {
    if (running) console.error("Stream error:", err);
  }
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
