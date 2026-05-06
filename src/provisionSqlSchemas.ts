import knexFactory, { type Knex } from 'knex';
import type { SqlDataSourceConfig } from './types.js';

/**
 * One-shot schema bootstrapper for `@inferagraph/sql` storage tables.
 *
 * Idempotent — every CREATE uses `IF NOT EXISTS` so re-running is safe. The
 * function does NOT install the `pgvector` extension; instead it issues
 * `CREATE EXTENSION IF NOT EXISTS vector` which requires the extension to be
 * available on the server. If it isn't, PostgreSQL emits a clear error and
 * the caller can run `CREATE EXTENSION vector;` once with appropriate
 * privileges.
 *
 * Pass `{ tablePrefix: 'myapp_' }` to namespace the tables — useful when
 * multiple apps share a database. Pass `{ embeddingDimensions: 1024 }` to
 * match your embedding model's dimensionality (default 3072 covers the
 * widest current production model).
 *
 * Each table is opt-in via the boolean flags. Omitting them all is a no-op.
 */
export interface ProvisionSqlSchemasConfig {
  /** Connection string or knex connection config. */
  connectionString?: string;
  connection?: SqlDataSourceConfig['connection'];
  dialect?: SqlDataSourceConfig['dialect'];
  /** Pass an existing knex instance to skip the internal connection. */
  knex?: Knex;
  /** Vector column dimensions (default 3072). */
  embeddingDimensions?: number;
  /** Table name prefix (default `inferagraph_`). */
  tablePrefix?: string;
  embeddings?: boolean;
  inferredEdges?: boolean;
  conversations?: boolean;
  cache?: boolean;
}

export async function provisionSqlSchemas(
  config: ProvisionSqlSchemasConfig,
): Promise<void> {
  let db: Knex;
  let owns = false;

  if (config.knex) {
    db = config.knex;
  } else {
    const connection = config.connectionString ?? config.connection;
    if (!connection) {
      throw new Error(
        'provisionSqlSchemas requires connectionString, connection, or knex',
      );
    }
    db = knexFactory({
      client: config.dialect ?? 'pg',
      connection,
      useNullAsDefault: true,
    });
    owns = true;
  }

  const dim = config.embeddingDimensions ?? 3072;
  const prefix = config.tablePrefix ?? 'inferagraph_';

  try {
    if (config.embeddings || config.inferredEdges) {
      // pgvector extension is required only when the caller actually wants
      // vector storage. Cache + conversations work without it.
      await db.raw(`CREATE EXTENSION IF NOT EXISTS vector`);
    }

    if (config.embeddings) {
      const t = `${prefix}embeddings`;
      await db.raw(`
        CREATE TABLE IF NOT EXISTS ${t} (
          node_id TEXT NOT NULL,
          embedding vector(${dim}),
          embedding_model TEXT NOT NULL,
          embedding_version TEXT NOT NULL,
          embedding_hash TEXT NOT NULL,
          generated_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY (node_id, embedding_model, embedding_version, embedding_hash)
        )
      `);
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_hnsw_idx ON ${t} USING hnsw (embedding vector_cosine_ops)`,
      );
    }

    if (config.inferredEdges) {
      const t = `${prefix}inferred_edges`;
      await db.raw(`
        CREATE TABLE IF NOT EXISTS ${t} (
          id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
          source_id TEXT NOT NULL,
          target_id TEXT NOT NULL,
          type TEXT NOT NULL,
          score REAL NOT NULL,
          sources TEXT[] NOT NULL,
          reasoning TEXT,
          embedding vector(${dim}),
          embedding_model TEXT,
          embedding_version TEXT,
          embedding_hash TEXT
        )
      `);
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_source_idx ON ${t} (source_id)`,
      );
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_target_idx ON ${t} (target_id)`,
      );
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_hnsw_idx ON ${t} USING hnsw (embedding vector_cosine_ops)`,
      );
    }

    if (config.conversations) {
      const t = `${prefix}conversations`;
      await db.raw(`
        CREATE TABLE IF NOT EXISTS ${t} (
          conversation_id TEXT NOT NULL,
          turn_idx BIGSERIAL,
          role TEXT NOT NULL,
          content TEXT NOT NULL,
          retrieved_node_ids TEXT[],
          created_at TIMESTAMPTZ DEFAULT NOW(),
          expires_at TIMESTAMPTZ,
          PRIMARY KEY (conversation_id, turn_idx)
        )
      `);
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_lookup_idx ON ${t} (conversation_id, turn_idx DESC)`,
      );
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_expires_idx ON ${t} (expires_at)`,
      );
    }

    if (config.cache) {
      const t = `${prefix}cache`;
      await db.raw(`
        CREATE TABLE IF NOT EXISTS ${t} (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL,
          expires_at TIMESTAMPTZ
        )
      `);
      await db.raw(
        `CREATE INDEX IF NOT EXISTS ${t}_expires_idx ON ${t} (expires_at)`,
      );
    }
  } finally {
    if (owns) {
      await db.destroy();
    }
  }
}
