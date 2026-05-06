import knexFactory, { type Knex } from 'knex';
import type {
  ConversationStore,
  ConversationTurn,
} from '@inferagraph/core';
import type { SqlDataSourceConfig } from './types.js';

/**
 * Configuration for {@link SqlConversationStore}.
 *
 * `ttlSeconds` (optional) controls per-turn expiry. When set, every appended
 * turn writes `expires_at = NOW() + ttlSeconds * INTERVAL '1 second'`.
 * `getTurns` filters expired rows lazily; an explicit {@link cleanup} method
 * deletes them eagerly if you want to reclaim space.
 *
 * Multi-dialect compatible — uses standard SQL where possible. The TTL math
 * uses PostgreSQL's `INTERVAL` syntax; for non-Postgres dialects, supply
 * a custom dialect-specific TTL via the `ttlSeconds` constructor option and
 * the store will adapt with `NOW() + ?` semantics that knex translates per
 * driver.
 */
export interface SqlConversationStoreConfig {
  knex?: Knex;
  dialect?: SqlDataSourceConfig['dialect'];
  connection?: SqlDataSourceConfig['connection'];
  /** Table name (default `inferagraph_conversations`). */
  tableName?: string;
  /** Per-turn TTL. Omit to retain turns indefinitely. */
  ttlSeconds?: number;
}

const DEFAULT_TABLE = 'inferagraph_conversations';

/**
 * SQL-backed implementation of core's {@link ConversationStore}. Persists
 * multi-turn chat history keyed by `conversationId`. Schema layout:
 *
 *   `(conversation_id TEXT, turn_idx BIGSERIAL, role TEXT, content TEXT,
 *     retrieved_node_ids TEXT[], created_at TIMESTAMPTZ, expires_at TIMESTAMPTZ)`
 *
 * `turn_idx` is the monotonic per-row sequence — used for ordering. A
 * descending fetch limited to N rows returns the most-recent N turns; the
 * store reverses them so callers receive oldest → newest.
 */
export class SqlConversationStore implements ConversationStore {
  private readonly db: Knex;
  private readonly ownsKnex: boolean;
  private readonly tableName: string;
  private readonly ttlSeconds: number | undefined;

  constructor(config: SqlConversationStoreConfig) {
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
        'SqlConversationStore requires either { knex } or { dialect, connection }',
      );
    }
    this.tableName = config.tableName ?? DEFAULT_TABLE;
    this.ttlSeconds = config.ttlSeconds;
  }

  async getTurns(conversationId: string, limit: number): Promise<ConversationTurn[]> {
    const sql = `
      SELECT role, content, retrieved_node_ids,
             EXTRACT(EPOCH FROM created_at) * 1000 AS timestamp
      FROM ${this.tableName}
      WHERE conversation_id = ?
        AND (expires_at IS NULL OR expires_at > NOW())
      ORDER BY turn_idx DESC
      LIMIT ?
    `;
    const rows = await this.runSelect(sql, [conversationId, limit]);
    // SQL returned newest → oldest; flip to chronological.
    return rows
      .map((r) => this.rowToTurn(r))
      .reverse();
  }

  async appendTurn(conversationId: string, turn: ConversationTurn): Promise<void> {
    const ids = turn.retrievedNodeIds && turn.retrievedNodeIds.length
      ? `{${turn.retrievedNodeIds.join(',')}}`
      : null;

    if (this.ttlSeconds !== undefined) {
      const sql = `
        INSERT INTO ${this.tableName}
          (conversation_id, role, content, retrieved_node_ids, created_at, expires_at)
        VALUES (?, ?, ?, ?, NOW(), NOW() + (? * INTERVAL '1 second'))
      `;
      await this.db.raw(sql, [
        conversationId,
        turn.role,
        turn.content,
        ids,
        this.ttlSeconds,
      ]);
    } else {
      const sql = `
        INSERT INTO ${this.tableName}
          (conversation_id, role, content, retrieved_node_ids, created_at)
        VALUES (?, ?, ?, ?, NOW())
      `;
      await this.db.raw(sql, [
        conversationId,
        turn.role,
        turn.content,
        ids,
      ]);
    }
  }

  async clear(conversationId: string): Promise<void> {
    const sql = `DELETE FROM ${this.tableName} WHERE conversation_id = ?`;
    await this.db.raw(sql, [conversationId]);
  }

  /**
   * Eagerly delete every expired turn across all conversations. Optional
   * housekeeping — `getTurns` already filters expired rows lazily, so this
   * exists purely to reclaim storage on a schedule.
   */
  async cleanup(): Promise<void> {
    const sql = `DELETE FROM ${this.tableName} WHERE expires_at IS NOT NULL AND expires_at < NOW()`;
    await this.db.raw(sql);
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

  private rowToTurn(row: Record<string, unknown>): ConversationTurn {
    const ids = row.retrieved_node_ids;
    const retrievedNodeIds = parsePgArrayOrUndefined(ids);
    return {
      role: row.role === 'assistant' ? 'assistant' : 'user',
      content: String(row.content ?? ''),
      timestamp: Number(row.timestamp ?? 0),
      ...(retrievedNodeIds ? { retrievedNodeIds } : {}),
    };
  }
}

/** Factory for {@link SqlConversationStore}. */
export function sqlConversationStore(
  config: SqlConversationStoreConfig,
): SqlConversationStore {
  return new SqlConversationStore(config);
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

function parsePgArrayOrUndefined(input: unknown): string[] | undefined {
  if (input == null) return undefined;
  if (Array.isArray(input)) {
    const arr = input.map((s) => String(s));
    return arr.length ? arr : undefined;
  }
  if (typeof input === 'string') {
    const trimmed = input.replace(/^\{|\}$/g, '').trim();
    if (!trimmed) return undefined;
    return trimmed.split(',').map((s) => s.trim());
  }
  return undefined;
}
