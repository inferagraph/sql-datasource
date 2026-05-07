import knexFactory, { type Knex } from 'knex';
import type {
  EmbeddingStore,
  EmbeddingRecord,
  NodeId,
  SearchVectorHit,
  SimilarHit,
  Vector,
} from '@inferagraph/core/data';
import type { SqlDataSourceConfig } from './types.js';

/**
 * Configuration for {@link SqlVectorEmbeddingStore}.
 *
 * Two construction shapes are supported:
 *   1. `knex` — pass an existing Knex instance (the recommended shape when
 *      multiple stores share a connection pool).
 *   2. `dialect` + `connection` — let the store construct its own knex
 *      instance internally.
 *
 * `tableName` defaults to `'inferagraph_embeddings'` so multiple stores can
 * coexist by passing distinct names.
 *
 * **Vector storage requires PostgreSQL with the `pgvector` extension.**
 * Other dialects supported by the rest of `@inferagraph/sql` (MySQL, SQLite,
 * MSSQL) do not have a portable vector type yet — pgvector is the de-facto
 * open-source standard. Use {@link provisionSqlSchemas} to bootstrap the
 * extension and table together.
 */
export interface SqlVectorEmbeddingStoreConfig {
  knex?: Knex;
  dialect?: SqlDataSourceConfig['dialect'];
  connection?: SqlDataSourceConfig['connection'];
  /** Table name (default `inferagraph_embeddings`). */
  tableName?: string;
}

const DEFAULT_TABLE = 'inferagraph_embeddings';

/**
 * PostgreSQL + pgvector–backed implementation of core's {@link EmbeddingStore}
 * contract. Persists `(nodeId, model, modelVersion, contentHash) → vector`
 * tuples and supports vector-native top-K via the pgvector cosine operator
 * (`<=>`). Implements both the legacy {@link EmbeddingStore.similar} method
 * and the newer {@link EmbeddingStore.searchVector} entry point.
 */
export class SqlVectorEmbeddingStore implements EmbeddingStore {
  private readonly db: Knex;
  private readonly ownsKnex: boolean;
  private readonly tableName: string;

  constructor(config: SqlVectorEmbeddingStoreConfig) {
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
        'SqlVectorEmbeddingStore requires either { knex } or { dialect, connection }',
      );
    }
    this.tableName = config.tableName ?? DEFAULT_TABLE;
  }

  async get(
    nodeId: NodeId,
    model: string,
    modelVersion: string,
    contentHash: string,
  ): Promise<EmbeddingRecord | undefined> {
    const sql = `
      SELECT node_id, embedding, embedding_model, embedding_version,
             embedding_hash, generated_at
      FROM ${this.tableName}
      WHERE node_id = ?
        AND embedding_model = ?
        AND embedding_version = ?
        AND embedding_hash = ?
      LIMIT 1
    `;
    const result = await this.runSelect(sql, [nodeId, model, modelVersion, contentHash]);
    const row = result[0];
    if (!row) return undefined;
    return this.rowToRecord(row);
  }

  async set(record: EmbeddingRecord): Promise<void> {
    const vectorLiteral = vectorToPgLiteral(record.vector);
    const sql = `
      INSERT INTO ${this.tableName}
        (node_id, embedding, embedding_model, embedding_version, embedding_hash, generated_at)
      VALUES (?, ?::vector, ?, ?, ?, ?)
      ON CONFLICT (node_id, embedding_model, embedding_version, embedding_hash)
      DO UPDATE SET embedding = EXCLUDED.embedding, generated_at = EXCLUDED.generated_at
    `;
    await this.db.raw(sql, [
      record.nodeId,
      vectorLiteral,
      record.meta.model,
      record.meta.modelVersion,
      record.meta.contentHash,
      record.meta.generatedAt,
    ]);
  }

  async similar(
    queryVector: Vector,
    k: number,
    model: string = '',
    modelVersion: string = '',
  ): Promise<SimilarHit[]> {
    const literal = vectorToPgLiteral(queryVector);
    const filters: string[] = [];
    const bindings: unknown[] = [literal];
    if (model) {
      filters.push('embedding_model = ?');
      bindings.push(model);
    }
    if (modelVersion) {
      filters.push('embedding_version = ?');
      bindings.push(modelVersion);
    }
    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    bindings.push(k);

    const sql = `
      SELECT node_id, 1 - (embedding <=> ?::vector) AS score
      FROM ${this.tableName}
      ${where}
      ORDER BY embedding <=> ?::vector
      LIMIT ?
    `;
    // The query needs the literal twice (in projection AND ORDER BY); duplicate it.
    const finalBindings = [literal, ...bindings.slice(1, bindings.length - 1), literal, k];
    const rows = await this.runSelect(sql, finalBindings);
    return rows.map((r) => ({ nodeId: String(r.node_id), score: Number(r.score) }));
  }

  async searchVector(
    queryEmbedding: Vector,
    opts: { top: number; container?: 'units' | 'inferred_edges' },
  ): Promise<SearchVectorHit[]> {
    const literal = vectorToPgLiteral(queryEmbedding);
    // Single-table architecture: container is documented as a tag only,
    // ignored here since the store holds one logical container.
    void opts.container;
    const sql = `
      SELECT node_id, 1 - (embedding <=> ?::vector) AS score
      FROM ${this.tableName}
      ORDER BY embedding <=> ?::vector
      LIMIT ?
    `;
    const rows = await this.runSelect(sql, [literal, literal, opts.top]);
    return rows.map((r) => ({ nodeId: String(r.node_id), score: Number(r.score) }));
  }

  async clear(): Promise<void> {
    await this.db.raw(`DELETE FROM ${this.tableName}`);
  }

  /** Tear down the underlying knex pool, if owned by this store. */
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

  private rowToRecord(row: Record<string, unknown>): EmbeddingRecord {
    const vector = parsePgVectorLiteral(row.embedding);
    return {
      nodeId: String(row.node_id),
      vector,
      meta: {
        model: String(row.embedding_model ?? ''),
        modelVersion: String(row.embedding_version ?? ''),
        contentHash: String(row.embedding_hash ?? ''),
        generatedAt: String(row.generated_at ?? ''),
      },
    };
  }
}

/** Factory for {@link SqlVectorEmbeddingStore}. */
export function sqlVectorEmbeddingStore(
  config: SqlVectorEmbeddingStoreConfig,
): SqlVectorEmbeddingStore {
  return new SqlVectorEmbeddingStore(config);
}

// --- Internal helpers -------------------------------------------------------

/**
 * pgvector accepts vectors as the textual literal `'[0.1,0.2,0.3]'` cast to
 * `::vector`. We normalize the array to that string here.
 */
function vectorToPgLiteral(v: Vector): string {
  return `[${v.join(',')}]`;
}

function parsePgVectorLiteral(input: unknown): Vector {
  if (Array.isArray(input)) return (input as unknown[]).map((n) => Number(n));
  if (typeof input === 'string') {
    const trimmed = input.replace(/^\[|\]$/g, '').trim();
    if (!trimmed) return [];
    return trimmed.split(',').map((s) => Number(s.trim()));
  }
  return [];
}

/**
 * Knex `.raw()` returns different shapes per dialect. PostgreSQL (`pg`) yields
 * `{ rows: [...] }`. Other dialects yield arrays directly. Normalize.
 */
function extractRows(result: unknown): Record<string, unknown>[] {
  if (result == null) return [];
  if (Array.isArray(result)) return result as Record<string, unknown>[];
  if (typeof result === 'object') {
    const obj = result as Record<string, unknown>;
    if (Array.isArray(obj.rows)) return obj.rows as Record<string, unknown>[];
  }
  return [];
}
