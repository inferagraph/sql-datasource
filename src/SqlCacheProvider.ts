import knexFactory, { type Knex } from 'knex';
import type { CacheProvider } from '@inferagraph/core';
import type { SqlDataSourceConfig } from './types.js';

/**
 * Configuration for {@link SqlCacheProvider}.
 *
 * `defaultTtlSeconds` is applied when callers omit `opts.ttlSeconds` on a
 * given `set` call. Omitting BOTH means entries persist indefinitely.
 *
 * The provider is multi-dialect compatible — uses `INSERT … ON CONFLICT
 * (key) DO UPDATE` (PostgreSQL/SQLite) for upserts. Other dialects can
 * supply a knex instance and the provider uses parameterized SQL that knex
 * adapts.
 */
export interface SqlCacheProviderConfig {
  knex?: Knex;
  dialect?: SqlDataSourceConfig['dialect'];
  connection?: SqlDataSourceConfig['connection'];
  /** Table name (default `inferagraph_cache`). */
  tableName?: string;
  /** Default per-entry TTL applied when `set` opts omit `ttlSeconds`. */
  defaultTtlSeconds?: number;
}

const DEFAULT_TABLE = 'inferagraph_cache';

/**
 * SQL-backed implementation of core 0.9's {@link CacheProvider}.
 *
 * Schema:
 *   `inferagraph_cache (key TEXT PRIMARY KEY, value TEXT, expires_at TIMESTAMPTZ)`
 *
 * `get` filters by `expires_at IS NULL OR expires_at > NOW()` so expired
 * rows are invisible without an explicit cleanup step. Use a periodic
 * `DELETE FROM ... WHERE expires_at < NOW()` job to reclaim space.
 */
export class SqlCacheProvider implements CacheProvider {
  private readonly db: Knex;
  private readonly ownsKnex: boolean;
  private readonly tableName: string;
  private readonly defaultTtlSeconds: number | undefined;

  constructor(config: SqlCacheProviderConfig) {
    if (config.knex) {
      this.db = config.knex;
      this.ownsKnex = false;
    } else if (config.dialect && config.connection !== undefined) {
      this.db = knexFactory({
        client: config.dialect,
        connection: config.connection,
        useNullAsDefault: true,
      });
      this.ownsKnex = true;
    } else {
      throw new Error(
        'SqlCacheProvider requires either { knex } or { dialect, connection }',
      );
    }
    this.tableName = config.tableName ?? DEFAULT_TABLE;
    this.defaultTtlSeconds = config.defaultTtlSeconds;
  }

  async get(key: string): Promise<string | undefined> {
    const sql = `
      SELECT value
      FROM ${this.tableName}
      WHERE key = ?
        AND (expires_at IS NULL OR expires_at > NOW())
      LIMIT 1
    `;
    const rows = await this.runSelect(sql, [key]);
    if (!rows[0]) return undefined;
    return String(rows[0].value ?? '');
  }

  async set(key: string, value: string, opts?: { ttlSeconds?: number }): Promise<void> {
    const ttl = opts?.ttlSeconds ?? this.defaultTtlSeconds;
    if (ttl !== undefined) {
      const sql = `
        INSERT INTO ${this.tableName} (key, value, expires_at)
        VALUES (?, ?, NOW() + (? * INTERVAL '1 second'))
        ON CONFLICT (key) DO UPDATE
          SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
      `;
      await this.db.raw(sql, [key, value, ttl]);
    } else {
      const sql = `
        INSERT INTO ${this.tableName} (key, value, expires_at)
        VALUES (?, ?, NULL)
        ON CONFLICT (key) DO UPDATE
          SET value = EXCLUDED.value, expires_at = NULL
      `;
      await this.db.raw(sql, [key, value]);
    }
  }

  async delete(key: string): Promise<void> {
    const sql = `DELETE FROM ${this.tableName} WHERE key = ?`;
    await this.db.raw(sql, [key]);
  }

  async clear(): Promise<void> {
    await this.db.raw(`DELETE FROM ${this.tableName}`);
  }

  async disconnect(): Promise<void> {
    if (this.ownsKnex) {
      await this.db.destroy();
    }
  }

  private async runSelect(
    sql: string,
    bindings: readonly unknown[],
  ): Promise<Record<string, unknown>[]> {
    const result = (await this.db.raw(sql, bindings as unknown[])) as unknown;
    return extractRows(result);
  }
}

/** Factory for {@link SqlCacheProvider}. */
export function sqlCacheProvider(config: SqlCacheProviderConfig): SqlCacheProvider {
  return new SqlCacheProvider(config);
}

// --- Helpers ----------------------------------------------------------------

function extractRows(result: unknown): Record<string, unknown>[] {
  if (result == null) return [];
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  if (typeof result === 'object') {
    const obj = result as Record<string, unknown>;
    if (Array.isArray(obj.rows)) return obj.rows as Record<string, unknown>[];
  }
  return [];
}
