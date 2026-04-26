# @inferagraph/sql-datasource

SQL datasource plugin for [@inferagraph/core](https://github.com/inferagraph/core). Supports PostgreSQL, MySQL, SQLite, and MSSQL via [Knex.js](https://knexjs.org/).

## Installation

```bash
pnpm add @inferagraph/sql-datasource @inferagraph/core
```

You also need to install the driver for your chosen dialect:

```bash
# PostgreSQL
pnpm add pg

# MySQL
pnpm add mysql2

# SQLite
pnpm add better-sqlite3

# MSSQL
pnpm add tedious
```

## Usage

```typescript
import { SqlDatasource } from '@inferagraph/sql-datasource';

const datasource = new SqlDatasource({
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
const neighbors = await datasource.getNeighbors('node-1');
const results = await datasource.search('keyword');

await datasource.disconnect();
```

## Configuration

| Option | Type | Description |
|---|---|---|
| `dialect` | `'postgres' \| 'mysql' \| 'sqlite' \| 'mssql'` | SQL dialect |
| `connection` | `string \| object` | Knex connection config or connection string |
| `tables.nodes` | `string` | Node table name (default: `'nodes'`) |
| `tables.edges` | `string` | Edge table name (default: `'edges'`) |
| `tables.properties` | `string` | EAV properties table name (default: `'node_properties'`) |
| `tables.content` | `string` | Content table name (default: `'content'`) |
| `autoMigrate` | `boolean` | Create tables on connect (default: `false`) |

## SQL Schema

When `autoMigrate: true`, the following tables are created:

### nodes

| Column | Type | Description |
|---|---|---|
| `id` | `VARCHAR(255)` | Primary key |
| `name` | `VARCHAR(500)` | Node display name |
| `type` | `VARCHAR(100)` | Node type |
| `created_at` | `TIMESTAMP` | Creation timestamp |
| `updated_at` | `TIMESTAMP` | Last update timestamp |

### edges

| Column | Type | Description |
|---|---|---|
| `id` | `VARCHAR(255)` | Primary key |
| `source_id` | `VARCHAR(255)` | FK to nodes.id |
| `target_id` | `VARCHAR(255)` | FK to nodes.id |
| `type` | `VARCHAR(100)` | Relationship type |
| `weight` | `FLOAT` | Edge weight (default: 1.0) |
| `created_at` | `TIMESTAMP` | Creation timestamp |

### node_properties (EAV)

| Column | Type | Description |
|---|---|---|
| `node_id` | `VARCHAR(255)` | FK to nodes.id |
| `key` | `VARCHAR(255)` | Property name |
| `value` | `TEXT` | Serialized value |
| `value_type` | `VARCHAR(50)` | Type hint (`string`, `number`, `boolean`, `json`) |

### content

| Column | Type | Description |
|---|---|---|
| `node_id` | `VARCHAR(255)` | FK to nodes.id (PK) |
| `content` | `TEXT` | Content body |
| `content_type` | `VARCHAR(50)` | MIME-like type (default: `'markdown'`) |
| `metadata` | `TEXT` | JSON string of metadata |
| `updated_at` | `TIMESTAMP` | Last update timestamp |

## Dialect Support

| Dialect | Driver | Status |
|---|---|---|
| PostgreSQL | `pg` | Supported |
| MySQL | `mysql2` | Supported |
| SQLite | `better-sqlite3` | Supported |
| MSSQL | `tedious` | Supported |

## License

MIT
