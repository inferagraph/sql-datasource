# @inferagraph/sql

SQL persistence for [@inferagraph/core](https://github.com/inferagraph/core). One package, five plug-in implementations:

| Implementation | Contract (core) | Backing | Dialects |
|---|---|---|---|
| `SqlDataSource` | `Datasource` | `nodes` / `edges` / `node_properties` / `content` | PostgreSQL, MySQL, SQLite, MSSQL |
| `SqlVectorEmbeddingStore` | `EmbeddingStore` | `inferagraph_embeddings` (pgvector) | PostgreSQL + pgvector |
| `SqlInferredEdgeStore` | `InferredEdgeStore` | `inferagraph_inferred_edges` (pgvector) | PostgreSQL + pgvector |
| `SqlConversationStore` | `ConversationStore` | `inferagraph_conversations` | PostgreSQL (TTL math via `INTERVAL`); other dialects via custom config |
| `SqlCacheProvider` | `CacheProvider` | `inferagraph_cache` | PostgreSQL/SQLite (`ON CONFLICT`); other dialects with custom upsert |

> Migration from `@inferagraph/sql-datasource`:
>
> ```bash
> pnpm remove @inferagraph/sql-datasource
> pnpm add @inferagraph/sql
> ```
>
> The class is now `SqlDataSource` (capital `S` in `Source`) — update imports accordingly.

## Installation

```bash
pnpm add @inferagraph/sql @inferagraph/core
```

You also need to install the driver for your chosen dialect:

```bash
# PostgreSQL (required for vector stores)
pnpm add pg

# MySQL
pnpm add mysql2

# SQLite
pnpm add better-sqlite3

# MSSQL
pnpm add tedious
```

The vector-backed stores (`SqlVectorEmbeddingStore`, `SqlInferredEdgeStore`) require **PostgreSQL with the [pgvector](https://github.com/pgvector/pgvector) extension**. Other dialects supported by Knex (MySQL/SQLite/MSSQL) work fine for `SqlDataSource`, `SqlConversationStore`, and `SqlCacheProvider`.

## Schema bootstrap

Run once at deploy time to create every table this package needs:

```typescript
import { provisionSqlSchemas } from '@inferagraph/sql';

await provisionSqlSchemas({
  connectionString: process.env.DATABASE_URL!,
  embeddingDimensions: 3072,
  embeddings: true,
  inferredEdges: true,
  conversations: true,
  cache: true,
});
```

The function is idempotent (every CREATE uses `IF NOT EXISTS`). It issues `CREATE EXTENSION IF NOT EXISTS vector` when any vector-backed table is requested. If pgvector is not installed on the server, the call fails with a clear error — install with `CREATE EXTENSION vector;` (one line, requires superuser).

Pass `tablePrefix: 'myapp_'` to namespace the tables; pass `embeddingDimensions: 1024` to match a smaller embedding model.

## SqlDataSource — graph data

```typescript
import { sqlDataSource } from '@inferagraph/sql';

const datasource = sqlDataSource({
  dialect: 'postgres',
  connection: {
    host: 'localhost',
    port: 5432,
    user: 'app',
    password: 'secret',
    database: 'graph_db',
  },
  autoMigrate: true, // auto-create tables on connect
});

await datasource.connect();

const view = await datasource.getInitialView();
const node = await datasource.getNode('node-1');
const neighbors = await datasource.getNeighbors('node-1', 2);
const results = await datasource.search('keyword');

await datasource.disconnect();
```

`getNeighbors(nodeId, depth)` supports `depth > 1` via application-level BFS; SQL has no native graph traversal so each level fans out one query at a time, deduping nodes and edges.

## SqlVectorEmbeddingStore — vector storage

```typescript
import { sqlVectorEmbeddingStore } from '@inferagraph/sql';

const store = sqlVectorEmbeddingStore({
  dialect: 'postgres',
  connection: process.env.DATABASE_URL!,
});

await store.set({
  nodeId: 'gen-2-7-adam',
  vector: [0.123, -0.045, /* ... */],
  meta: {
    model: 'text-embedding-3-large',
    modelVersion: '1',
    contentHash: 'abc123',
    generatedAt: new Date().toISOString(),
  },
});

const hits = await store.searchVector!([0.111, 0.222, /* ... */], { top: 25 });
```

The composite key is `(nodeId, model, modelVersion, contentHash)` — model bumps and content edits naturally bypass stale hits without explicit invalidation.

`searchVector` issues a pgvector cosine query (`embedding <=> $1::vector`) ordered ASC and limited to `top` rows.

## SqlInferredEdgeStore — inferred edges

```typescript
import { sqlInferredEdgeStore } from '@inferagraph/sql';

const store = sqlInferredEdgeStore({
  dialect: 'postgres',
  connection: process.env.DATABASE_URL!,
});

await store.set([
  {
    sourceId: 'adam',
    targetId: 'eve',
    type: 'related_to',
    score: 0.92,
    sources: ['embedding', 'llm'],
    reasoning: 'consistently co-occur',
  },
]);

const incident = await store.getAllForNode('adam');
const topMatches = await store.searchInferredEdges([0.1, 0.2 /* ... */], 10);
```

`set` is bulk-replace by contract — the table is cleared and re-populated atomically. Duplicate ordered `(sourceId, targetId)` keys keep the LAST occurrence.

## SqlConversationStore — chat memory

```typescript
import { sqlConversationStore } from '@inferagraph/sql';

const store = sqlConversationStore({
  dialect: 'postgres',
  connection: process.env.DATABASE_URL!,
  ttlSeconds: 60 * 60 * 24, // optional: 24h per turn
});

await store.appendTurn('conv-42', {
  role: 'user',
  content: 'Tell me about Adam',
  timestamp: Date.now(),
});

const turns = await store.getTurns('conv-42', 20); // oldest -> newest

await store.cleanup(); // optional housekeeping: delete expired rows
```

`getTurns` filters expired rows lazily (`WHERE expires_at IS NULL OR expires_at > NOW()`). The `cleanup()` method exists for periodic housekeeping when you want to reclaim space.

## SqlCacheProvider — generic cache

```typescript
import { sqlCacheProvider } from '@inferagraph/sql';

const cache = sqlCacheProvider({
  dialect: 'postgres',
  connection: process.env.DATABASE_URL!,
  defaultTtlSeconds: 300, // optional default TTL
});

await cache.set('llm:abc123', JSON.stringify(response));
await cache.set('llm:xyz999', JSON.stringify(response), { ttlSeconds: 30 });

const cached = await cache.get('llm:abc123');
await cache.delete('llm:xyz999');
await cache.clear();
```

Schema: `inferagraph_cache (key TEXT PRIMARY KEY, value TEXT, expires_at TIMESTAMPTZ)`. UPSERT via `INSERT … ON CONFLICT (key) DO UPDATE`.

## Sharing one Knex instance across stores

Pass the same `knex` instance to every store to share a single connection pool:

```typescript
import knex from 'knex';
import {
  sqlDataSource,
  SqlVectorEmbeddingStore,
  SqlInferredEdgeStore,
  SqlConversationStore,
  SqlCacheProvider,
} from '@inferagraph/sql';

const db = knex({
  client: 'postgres',
  connection: process.env.DATABASE_URL,
});

const embeddings = new SqlVectorEmbeddingStore({ knex: db });
const inferredEdges = new SqlInferredEdgeStore({ knex: db });
const conversations = new SqlConversationStore({ knex: db, ttlSeconds: 86400 });
const cache = new SqlCacheProvider({ knex: db, defaultTtlSeconds: 300 });
```

When you pass `knex`, the store does NOT call `db.destroy()` on `disconnect()` — you own the lifetime.

## Configuration reference

### `SqlDataSourceConfig`

| Option | Type | Description |
|---|---|---|
| `dialect` | `'postgres' \| 'mysql' \| 'sqlite' \| 'mssql'` | SQL dialect |
| `connection` | `string \| object` | Knex connection config or connection string |
| `tables.nodes` | `string` | Node table name (default: `'nodes'`) |
| `tables.edges` | `string` | Edge table name (default: `'edges'`) |
| `tables.properties` | `string` | EAV properties table name (default: `'node_properties'`) |
| `tables.content` | `string` | Content table name (default: `'content'`) |
| `autoMigrate` | `boolean` | Create tables on connect (default: `false`) |

### `SqlVectorEmbeddingStoreConfig` / `SqlInferredEdgeStoreConfig` / `SqlConversationStoreConfig` / `SqlCacheProviderConfig`

All four store configs accept either:
- `knex: Knex` — share an existing pool, OR
- `dialect` + `connection` — let the store construct knex internally.

Plus `tableName?` to namespace the table. Per-store extras:
- `SqlConversationStoreConfig.ttlSeconds` — per-turn TTL (default: none).
- `SqlCacheProviderConfig.defaultTtlSeconds` — default TTL applied when `set` opts omit `ttlSeconds`.

### `ProvisionSqlSchemasConfig`

| Option | Type | Description |
|---|---|---|
| `connectionString` / `connection` / `knex` | one required | Connection target |
| `dialect` | `'postgres' \| ...` | Default `'pg'` |
| `embeddingDimensions` | `number` | Vector column size (default: 3072) |
| `tablePrefix` | `string` | Prefix for every table (default: `'inferagraph_'`) |
| `embeddings` / `inferredEdges` / `conversations` / `cache` | `boolean` | Opt-in per table |

## License

MIT
