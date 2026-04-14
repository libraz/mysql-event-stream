// Copyright 2024 mysql-event-stream Authors
// SPDX-License-Identifier: Apache-2.0

import mysql from "mysql2/promise";

export interface BinlogStatus {
  File: string;
  Position: number;
}

/** MySQL client for E2E tests. */
export class MysqlClient {
  private pool: mysql.Pool;

  constructor(
    host = "127.0.0.1",
    port = 13307,
    user = "root",
    password = "test_root_password",
    database = "mes_test",
  ) {
    this.pool = mysql.createPool({
      host,
      port,
      user,
      password,
      database,
      charset: "utf8mb4",
      waitForConnections: true,
      connectionLimit: 2,
    });
  }

  /** Execute a raw SQL query and return the rows. */
  async execute<T extends mysql.RowDataPacket[]>(sql: string, values?: unknown[]): Promise<T> {
    const [rows] = await this.pool.execute<T>(sql, values);
    return rows;
  }

  /** Insert a row and return the auto-increment ID. */
  async insert(table: string, data: Record<string, unknown>): Promise<number> {
    const cols = Object.keys(data).join(", ");
    const placeholders = Object.keys(data)
      .map(() => "?")
      .join(", ");
    const sql = `INSERT INTO ${table} (${cols}) VALUES (${placeholders})`;
    const [result] = await this.pool.execute<mysql.ResultSetHeader>(sql, Object.values(data));
    return result.insertId;
  }

  /** Update rows matching the WHERE clause. */
  async update(table: string, where: string, data: Record<string, unknown>): Promise<number> {
    const sets = Object.keys(data)
      .map((k) => `${k} = ?`)
      .join(", ");
    const sql = `UPDATE ${table} SET ${sets} WHERE ${where}`;
    const [result] = await this.pool.execute<mysql.ResultSetHeader>(sql, Object.values(data));
    return result.affectedRows;
  }

  /** Delete rows matching the WHERE clause. */
  async delete(table: string, where: string): Promise<number> {
    const sql = `DELETE FROM ${table} WHERE ${where}`;
    const [result] = await this.pool.execute<mysql.ResultSetHeader>(sql);
    return result.affectedRows;
  }

  /** Truncate a table. */
  async truncate(table: string): Promise<void> {
    await this.pool.execute(`TRUNCATE TABLE ${table}`);
  }

  /** Get current binlog file and position. */
  async binlogStatus(): Promise<BinlogStatus> {
    const [rows] = await this.pool.execute<mysql.RowDataPacket[]>("SHOW BINARY LOG STATUS");
    if (!rows[0]) throw new Error("SHOW BINARY LOG STATUS returned no rows");
    return rows[0] as unknown as BinlogStatus;
  }

  /** Get current GTID executed position (flavor-aware). */
  async getCurrentGtid(): Promise<string> {
    // MariaDB uses @@GLOBAL.gtid_current_pos; MySQL uses @@GLOBAL.gtid_executed
    const query =
      process.env.DB_FLAVOR === "mariadb"
        ? "SELECT @@GLOBAL.gtid_current_pos AS gtid"
        : "SELECT @@GLOBAL.gtid_executed AS gtid";
    const [rows] = await this.pool.query(query);
    return (rows as any)[0].gtid;
  }

  /** Check if MySQL is reachable. */
  async ping(): Promise<boolean> {
    try {
      await this.pool.execute("SELECT 1");
      return true;
    } catch {
      return false;
    }
  }

  /** Close the connection pool. */
  async close(): Promise<void> {
    await this.pool.end();
  }
}
