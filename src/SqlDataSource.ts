import { DataSource } from '@inferagraph/core';
import type {
  DataAdapterConfig,
  GraphData,
  NodeId,
  NodeData,
  ContentData,
  PaginationOptions,
  PaginatedResult,
  DataFilter,
} from '@inferagraph/core';
import knex, { type Knex } from 'knex';
import type { SqlDataSourceConfig, TableNames } from './types.js';
import { createSchema } from './schema.js';

export class SqlDataSource extends DataSource {
  readonly name = 'sql';
  private db: Knex | null = null;
  private config: SqlDataSourceConfig;
  private tables: TableNames;

  constructor(config: SqlDataSourceConfig) {
    super();
    this.config = config;
    this.tables = {
      nodes: config.tables?.nodes ?? 'nodes',
      edges: config.tables?.edges ?? 'edges',
      properties: config.tables?.properties ?? 'node_properties',
      content: config.tables?.content ?? 'content',
    };
  }

  async connect(): Promise<void> {
    this.db = knex({
      client: this.config.dialect,
      connection: this.config.connection,
      useNullAsDefault: true,
    });

    if (this.config.autoMigrate) {
      await createSchema(this.db, this.tables);
    }
  }

  async disconnect(): Promise<void> {
    if (this.db) {
      await this.db.destroy();
      this.db = null;
    }
  }

  isConnected(): boolean {
    return this.db !== null;
  }

  async getInitialView(config?: DataAdapterConfig): Promise<GraphData> {
    this.ensureConnected();
    const limit = (config?.limit as number) ?? 100;

    const rows = await this.db!(this.tables.nodes).limit(limit);
    const nodes = await Promise.all(rows.map((row) => this.rowToNodeData(row)));

    const nodeIds = nodes.map((n) => n.id);
    if (nodeIds.length === 0) return { nodes: [], edges: [] };

    const edgeRows = await this.db!(this.tables.edges)
      .whereIn('source_id', nodeIds)
      .whereIn('target_id', nodeIds);
    const edges = edgeRows.map((row) => this.rowToEdgeData(row));

    return { nodes, edges };
  }

  async getNode(id: NodeId): Promise<NodeData | undefined> {
    this.ensureConnected();

    const row = await this.db!(this.tables.nodes).where('id', id).first();
    if (!row) return undefined;
    return this.rowToNodeData(row);
  }

  async getNeighbors(nodeId: NodeId, depth: number = 1): Promise<GraphData> {
    this.ensureConnected();

    // SQL has no native graph traversal, so depth>1 is implemented as
    // application-level BFS mirroring the existing findPath pattern: iterate
    // 1-hop fan-out from each newly discovered frontier node up to `depth`
    // levels. Dedupe edges by id and nodes by id.
    const effectiveDepth = Math.max(1, Math.floor(depth));

    const visitedNodeIds = new Set<string>([nodeId]);
    const collectedEdgeRows = new Map<string, Record<string, unknown>>();
    let frontier: string[] = [nodeId];

    for (let level = 0; level < effectiveDepth && frontier.length > 0; level++) {
      const nextFrontier: string[] = [];

      for (const currentId of frontier) {
        const edgeRows = await this.fetchEdgeRowsForNode(currentId);
        for (const edge of edgeRows) {
          const edgeId = String(edge.id);
          if (!collectedEdgeRows.has(edgeId)) {
            collectedEdgeRows.set(edgeId, edge);
          }
          const sourceId = String(edge.source_id);
          const targetId = String(edge.target_id);
          const otherId = sourceId === currentId ? targetId : sourceId;
          if (!visitedNodeIds.has(otherId)) {
            visitedNodeIds.add(otherId);
            nextFrontier.push(otherId);
          }
        }
      }

      frontier = nextFrontier;
    }

    // Fetch all visited node rows in a single query
    const nodeRows = await this.db!(this.tables.nodes).whereIn('id', [...visitedNodeIds]);
    const nodes = await Promise.all(nodeRows.map((row) => this.rowToNodeData(row)));
    const edges = [...collectedEdgeRows.values()].map((row) => this.rowToEdgeData(row));

    return { nodes, edges };
  }

  private async fetchEdgeRowsForNode(nodeId: NodeId): Promise<Record<string, unknown>[]> {
    return this.db!(this.tables.edges)
      .where('source_id', nodeId)
      .orWhere('target_id', nodeId);
  }

  async findPath(fromId: NodeId, toId: NodeId): Promise<GraphData> {
    this.ensureConnected();

    // Application-level BFS
    const visited = new Set<string>([fromId]);
    const parent = new Map<string, { nodeId: string; edge: Record<string, unknown> }>();
    let frontier = [fromId];
    let found = false;
    const maxDepth = 20;
    let depth = 0;

    while (frontier.length > 0 && !found && depth < maxDepth) {
      const nextFrontier: string[] = [];

      for (const currentId of frontier) {
        const edgeRows = await this.db!(this.tables.edges)
          .where('source_id', currentId)
          .orWhere('target_id', currentId);

        for (const edge of edgeRows) {
          const neighborId = edge.source_id === currentId ? edge.target_id : edge.source_id;
          if (!visited.has(neighborId)) {
            visited.add(neighborId);
            parent.set(neighborId, { nodeId: currentId, edge });
            nextFrontier.push(neighborId);
            if (neighborId === toId) {
              found = true;
              break;
            }
          }
        }
        if (found) break;
      }

      frontier = nextFrontier;
      depth++;
    }

    if (!found) return { nodes: [], edges: [] };

    // Reconstruct path
    const pathIds: string[] = [toId];
    const pathEdges: Record<string, unknown>[] = [];
    let current = toId;
    while (parent.has(current)) {
      const p = parent.get(current)!;
      pathIds.push(p.nodeId);
      pathEdges.push(p.edge);
      current = p.nodeId;
    }

    const nodeRows = await this.db!(this.tables.nodes).whereIn('id', pathIds);
    const nodes = await Promise.all(nodeRows.map((row) => this.rowToNodeData(row)));

    return {
      nodes,
      edges: pathEdges.map((row) => this.rowToEdgeData(row)),
    };
  }

  async search(
    query: string,
    pagination?: PaginationOptions,
  ): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    const rows = await this.db!(this.tables.nodes).where('name', 'like', `%${query}%`);

    const allItems = await Promise.all(rows.map((row) => this.rowToNodeData(row)));
    return this.paginate(allItems, pagination);
  }

  async filter(
    filter: DataFilter,
    pagination?: PaginationOptions,
  ): Promise<PaginatedResult<NodeData>> {
    this.ensureConnected();

    let queryBuilder = this.db!(this.tables.nodes);

    if (filter.types?.length) {
      queryBuilder = queryBuilder.whereIn('type', filter.types);
    }
    if (filter.search) {
      queryBuilder = queryBuilder.where('name', 'like', `%${filter.search}%`);
    }
    if (filter.attributes) {
      for (const [key, value] of Object.entries(filter.attributes)) {
        // Check if it's a base column or an EAV property
        if (['name', 'type'].includes(key)) {
          queryBuilder = queryBuilder.where(key, value as string);
        } else {
          // Sub-query into properties table
          queryBuilder = queryBuilder.whereIn('id', (sub) => {
            sub
              .select('node_id')
              .from(this.tables.properties)
              .where('key', key)
              .where('value', String(value));
          });
        }
      }
    }
    if (filter.tags?.length) {
      // Tags are stored in properties table as key='tags'
      queryBuilder = queryBuilder.whereIn('id', (sub) => {
        sub
          .select('node_id')
          .from(this.tables.properties)
          .where('key', 'tags')
          .whereIn('value', filter.tags!);
      });
    }

    const rows = await queryBuilder;
    const allItems = await Promise.all(rows.map((row) => this.rowToNodeData(row)));
    return this.paginate(allItems, pagination);
  }

  async getContent(nodeId: NodeId): Promise<ContentData | undefined> {
    this.ensureConnected();

    const row = await this.db!(this.tables.content).where('node_id', nodeId).first();
    if (!row) return undefined;

    return {
      nodeId,
      content: row.content,
      contentType: row.content_type ?? 'markdown',
      metadata: row.metadata ? JSON.parse(row.metadata) : undefined,
    };
  }

  // --- Private Helpers ---

  private ensureConnected(): void {
    if (!this.db) {
      throw new Error('SqlDataSource is not connected. Call connect() first.');
    }
  }

  private async rowToNodeData(row: Record<string, unknown>): Promise<NodeData> {
    const attributes: Record<string, unknown> = {
      name: row.name,
      type: row.type,
    };

    // Fetch EAV properties
    if (this.db) {
      const props = await this.db(this.tables.properties).where('node_id', row.id);

      for (const prop of props) {
        const value = this.deserializeValue(prop.value, prop.value_type);
        // For repeated keys, collect into arrays
        if (attributes[prop.key] !== undefined) {
          if (Array.isArray(attributes[prop.key])) {
            (attributes[prop.key] as unknown[]).push(value);
          } else {
            attributes[prop.key] = [attributes[prop.key], value];
          }
        } else {
          attributes[prop.key] = value;
        }
      }
    }

    return { id: String(row.id), attributes };
  }

  private rowToEdgeData(row: Record<string, unknown>) {
    return {
      id: String(row.id),
      sourceId: String(row.source_id),
      targetId: String(row.target_id),
      attributes: {
        type: row.type as string,
        weight: row.weight as number,
      },
    };
  }

  private deserializeValue(value: string, valueType: string): unknown {
    switch (valueType) {
      case 'number':
        return Number(value);
      case 'boolean':
        return value === 'true';
      case 'json':
        return JSON.parse(value);
      default:
        return value;
    }
  }

  private paginate(
    items: NodeData[],
    pagination?: PaginationOptions,
  ): PaginatedResult<NodeData> {
    const total = items.length;
    if (!pagination) return { items, total, hasMore: false };
    const { offset, limit } = pagination;
    const sliced = items.slice(offset, offset + limit);
    return { items: sliced, total, hasMore: offset + limit < total };
  }
}

/**
 * Factory for {@link SqlDataSource}. The recommended public entry point —
 * matches the `@inferagraph/*` ecosystem convention of factory functions
 * over class construction. The constructor stays as an escape hatch for
 * advanced subclassing.
 *
 * The factory does not call {@link SqlDataSource.connect}; callers connect
 * explicitly so they control when the underlying knex pool spins up.
 */
export function sqlDataSource(config: SqlDataSourceConfig): SqlDataSource {
  return new SqlDataSource(config);
}

