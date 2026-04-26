import { Datasource } from '@inferagraph/core';
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
import type { SqlDatasourceConfig, TableNames } from './types.js';
import { createSchema } from './schema.js';

export class SqlDatasource extends Datasource {
  readonly name = 'sql';
  private db: Knex | null = null;
  private config: SqlDatasourceConfig;
  private tables: TableNames;

  constructor(config: SqlDatasourceConfig) {
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

  async getNeighbors(nodeId: NodeId, _depth: number = 1): Promise<GraphData> {
    this.ensureConnected();

    // Get edges connected to this node
    const edgeRows = await this.db!(this.tables.edges)
      .where('source_id', nodeId)
      .orWhere('target_id', nodeId);

    // Collect neighbor IDs
    const neighborIds = new Set<string>();
    neighborIds.add(nodeId);
    for (const edge of edgeRows) {
      neighborIds.add(edge.source_id);
      neighborIds.add(edge.target_id);
    }

    // Fetch all nodes
    const nodeRows = await this.db!(this.tables.nodes).whereIn('id', [...neighborIds]);
    const nodes = await Promise.all(nodeRows.map((row) => this.rowToNodeData(row)));
    const edges = edgeRows.map((row) => this.rowToEdgeData(row));

    return { nodes, edges };
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
      throw new Error('SqlDatasource is not connected. Call connect() first.');
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
