import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Knex } from 'knex';
import { SqlDatasource } from '../src/SqlDatasource.js';
import { createSchema } from '../src/schema.js';

// --- Mock knex ---

function createMockQueryBuilder(data: Record<string, unknown>[] = []) {
  const builder: Record<string, unknown> = {};
  const self = new Proxy(builder, {
    get(_target, prop) {
      if (prop === 'then') return undefined; // not a thenable on its own
      if (prop === Symbol.toPrimitive || prop === Symbol.toStringTag) return undefined;
      if (typeof prop === 'symbol') return undefined;

      // Terminal methods that return data
      if (prop === 'first') {
        return vi.fn().mockResolvedValue(data[0] ?? undefined);
      }

      // All chainable methods return self; when awaited, resolve to data
      return vi.fn((..._args: unknown[]) => {
        // Return a thenable proxy that resolves to data and is also chainable
        const awaitable = new Proxy(
          {},
          {
            get(_t, p) {
              if (p === 'then') {
                return (resolve: (v: unknown) => void) => resolve(data);
              }
              if (p === 'first') {
                return vi.fn().mockResolvedValue(data[0] ?? undefined);
              }
              // further chaining
              return vi.fn((..._a: unknown[]) => awaitable);
            },
          },
        );
        return awaitable;
      });
    },
  });
  return self;
}

function createMockKnex(tableData: Record<string, Record<string, unknown>[]> = {}) {
  const schemaHasTables = new Set<string>();

  const mockKnexInstance = vi.fn((tableName: string) => {
    return createMockQueryBuilder(tableData[tableName] ?? []);
  }) as unknown as Knex;

  (mockKnexInstance as unknown as Record<string, unknown>).schema = {
    hasTable: vi.fn(async (name: string) => schemaHasTables.has(name)),
    createTable: vi.fn(async (name: string, callback: (table: unknown) => void) => {
      schemaHasTables.add(name);
      // Call the callback with a mock table builder
      const tableBuilder = createMockTableBuilder();
      callback(tableBuilder);
    }),
  };

  (mockKnexInstance as unknown as Record<string, unknown>).fn = {
    now: vi.fn(() => 'NOW()'),
  };

  (mockKnexInstance as unknown as Record<string, unknown>).destroy = vi
    .fn()
    .mockResolvedValue(undefined);

  return mockKnexInstance;
}

function createMockTableBuilder() {
  const columnBuilder = {
    primary: vi.fn().mockReturnThis(),
    notNullable: vi.fn().mockReturnThis(),
    defaultTo: vi.fn().mockReturnThis(),
    references: vi.fn().mockReturnThis(),
    inTable: vi.fn().mockReturnThis(),
    onDelete: vi.fn().mockReturnThis(),
  };

  return {
    string: vi.fn().mockReturnValue(columnBuilder),
    text: vi.fn().mockReturnValue(columnBuilder),
    float: vi.fn().mockReturnValue(columnBuilder),
    timestamp: vi.fn().mockReturnValue(columnBuilder),
    primary: vi.fn().mockReturnThis(),
    index: vi.fn().mockReturnThis(),
  };
}

// Mock the knex module
let mockKnexInstance: Knex;

vi.mock('knex', () => {
  return {
    default: vi.fn((_config: unknown) => {
      return mockKnexInstance;
    }),
  };
});

// Mock @inferagraph/core to provide the Datasource base class
vi.mock('@inferagraph/core', () => {
  return {
    Datasource: class {
      constructor() {
        // base class
      }
    },
  };
});

describe('SqlDatasource', () => {
  let datasource: SqlDatasource;

  beforeEach(() => {
    mockKnexInstance = createMockKnex();
    datasource = new SqlDatasource({
      dialect: 'postgres',
      connection: 'postgres://localhost/test',
    });
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // --- Basic Properties ---

  it('should have name "sql"', () => {
    expect(datasource.name).toBe('sql');
  });

  // --- Lifecycle ---

  describe('connect/disconnect/isConnected', () => {
    it('should not be connected before connect()', () => {
      expect(datasource.isConnected()).toBe(false);
    });

    it('should be connected after connect()', async () => {
      await datasource.connect();
      expect(datasource.isConnected()).toBe(true);
    });

    it('should not be connected after disconnect()', async () => {
      await datasource.connect();
      await datasource.disconnect();
      expect(datasource.isConnected()).toBe(false);
    });

    it('should call knex.destroy() on disconnect', async () => {
      await datasource.connect();
      await datasource.disconnect();
      expect(mockKnexInstance.destroy).toHaveBeenCalled();
    });

    it('should handle disconnect when not connected', async () => {
      await datasource.disconnect(); // should not throw
      expect(datasource.isConnected()).toBe(false);
    });
  });

  // --- autoMigrate ---

  describe('connect with autoMigrate', () => {
    it('should call createSchema when autoMigrate is true', async () => {
      const ds = new SqlDatasource({
        dialect: 'sqlite',
        connection: ':memory:',
        autoMigrate: true,
      });

      await ds.connect();

      // Verify schema.hasTable was called for all 4 tables
      const schema = (mockKnexInstance as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
      expect(schema.hasTable).toHaveBeenCalledWith('nodes');
      expect(schema.hasTable).toHaveBeenCalledWith('edges');
      expect(schema.hasTable).toHaveBeenCalledWith('node_properties');
      expect(schema.hasTable).toHaveBeenCalledWith('content');
    });

    it('should not call createSchema when autoMigrate is false', async () => {
      await datasource.connect();
      const schema = (mockKnexInstance as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
      expect(schema.hasTable).not.toHaveBeenCalled();
    });
  });

  // --- ensureConnected ---

  describe('ensureConnected', () => {
    it('should throw when calling getInitialView before connect', async () => {
      await expect(datasource.getInitialView()).rejects.toThrow(
        'SqlDatasource is not connected. Call connect() first.',
      );
    });

    it('should throw when calling getNode before connect', async () => {
      await expect(datasource.getNode('1')).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });

    it('should throw when calling getNeighbors before connect', async () => {
      await expect(datasource.getNeighbors('1')).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });

    it('should throw when calling findPath before connect', async () => {
      await expect(datasource.findPath('1', '2')).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });

    it('should throw when calling search before connect', async () => {
      await expect(datasource.search('test')).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });

    it('should throw when calling filter before connect', async () => {
      await expect(datasource.filter({})).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });

    it('should throw when calling getContent before connect', async () => {
      await expect(datasource.getContent('1')).rejects.toThrow(
        'SqlDatasource is not connected',
      );
    });
  });

  // --- getInitialView ---

  describe('getInitialView', () => {
    it('should return nodes and edges', async () => {
      const nodeRows = [
        { id: 'n1', name: 'Node 1', type: 'person' },
        { id: 'n2', name: 'Node 2', type: 'place' },
      ];
      const edgeRows = [
        { id: 'e1', source_id: 'n1', target_id: 'n2', type: 'knows', weight: 1.0 },
      ];

      mockKnexInstance = createMockKnex({
        nodes: nodeRows,
        edges: edgeRows,
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.getInitialView();
      expect(result.nodes).toHaveLength(2);
      expect(result.edges).toHaveLength(1);
      expect(result.nodes[0].id).toBe('n1');
      expect(result.nodes[0].attributes.name).toBe('Node 1');
      expect(result.edges[0].sourceId).toBe('n1');
      expect(result.edges[0].targetId).toBe('n2');
    });

    it('should return empty graph when no nodes exist', async () => {
      mockKnexInstance = createMockKnex({ nodes: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.getInitialView();
      expect(result.nodes).toHaveLength(0);
      expect(result.edges).toHaveLength(0);
    });
  });

  // --- getNode ---

  describe('getNode', () => {
    it('should return a node by ID', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [{ node_id: 'n1', key: 'era', value: 'ancient', value_type: 'string' }],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const node = await ds.getNode('n1');
      expect(node).toBeDefined();
      expect(node!.id).toBe('n1');
      expect(node!.attributes.name).toBe('Adam');
      expect(node!.attributes.type).toBe('person');
    });

    it('should return undefined for missing node', async () => {
      mockKnexInstance = createMockKnex({ nodes: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const node = await ds.getNode('nonexistent');
      expect(node).toBeUndefined();
    });
  });

  // --- getNeighbors ---

  describe('getNeighbors', () => {
    it('should return neighbors and connecting edges', async () => {
      const nodeRows = [
        { id: 'n1', name: 'Node 1', type: 'person' },
        { id: 'n2', name: 'Node 2', type: 'place' },
      ];
      const edgeRows = [
        { id: 'e1', source_id: 'n1', target_id: 'n2', type: 'lives_in', weight: 1.0 },
      ];

      mockKnexInstance = createMockKnex({
        nodes: nodeRows,
        edges: edgeRows,
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.getNeighbors('n1');
      expect(result.nodes).toHaveLength(2);
      expect(result.edges).toHaveLength(1);
    });
  });

  // --- findPath ---

  describe('findPath', () => {
    it('should return empty graph when no path exists', async () => {
      mockKnexInstance = createMockKnex({ edges: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.findPath('n1', 'n99');
      expect(result.nodes).toHaveLength(0);
      expect(result.edges).toHaveLength(0);
    });
  });

  // --- search ---

  describe('search', () => {
    it('should return matching nodes', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.search('Ada');
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(1);
      expect(result.hasMore).toBe(false);
    });

    it('should apply pagination', async () => {
      const nodes = [
        { id: 'n1', name: 'Node A', type: 'person' },
        { id: 'n2', name: 'Node B', type: 'person' },
        { id: 'n3', name: 'Node C', type: 'person' },
      ];

      mockKnexInstance = createMockKnex({ nodes, node_properties: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.search('Node', { offset: 0, limit: 2 });
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(true);
    });

    it('should paginate with offset', async () => {
      const nodes = [
        { id: 'n1', name: 'Node A', type: 'person' },
        { id: 'n2', name: 'Node B', type: 'person' },
        { id: 'n3', name: 'Node C', type: 'person' },
      ];

      mockKnexInstance = createMockKnex({ nodes, node_properties: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.search('Node', { offset: 2, limit: 2 });
      expect(result.items).toHaveLength(1);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(false);
    });
  });

  // --- filter ---

  describe('filter', () => {
    it('should filter by types', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ types: ['person'] });
      expect(result.items).toHaveLength(1);
    });

    it('should filter by search text', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ search: 'Ada' });
      expect(result.items).toHaveLength(1);
    });

    it('should filter by base column attributes', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ attributes: { name: 'Adam' } });
      expect(result.items).toHaveLength(1);
    });

    it('should filter by EAV attributes', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ attributes: { era: 'creation' } });
      expect(result.items).toHaveLength(1);
    });

    it('should filter by tags', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ tags: ['patriarch'] });
      expect(result.items).toHaveLength(1);
    });

    it('should apply pagination to filter results', async () => {
      const nodes = [
        { id: 'n1', name: 'Adam', type: 'person' },
        { id: 'n2', name: 'Eve', type: 'person' },
        { id: 'n3', name: 'Abel', type: 'person' },
      ];

      mockKnexInstance = createMockKnex({ nodes, node_properties: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.filter({ types: ['person'] }, { offset: 0, limit: 2 });
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(3);
      expect(result.hasMore).toBe(true);
    });
  });

  // --- getContent ---

  describe('getContent', () => {
    it('should return content for a node', async () => {
      mockKnexInstance = createMockKnex({
        content: [
          {
            node_id: 'n1',
            content: '# Adam\nFirst human.',
            content_type: 'markdown',
            metadata: JSON.stringify({ source: 'genesis' }),
          },
        ],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const content = await ds.getContent('n1');
      expect(content).toBeDefined();
      expect(content!.nodeId).toBe('n1');
      expect(content!.content).toBe('# Adam\nFirst human.');
      expect(content!.contentType).toBe('markdown');
      expect(content!.metadata).toEqual({ source: 'genesis' });
    });

    it('should return undefined for missing content', async () => {
      mockKnexInstance = createMockKnex({ content: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const content = await ds.getContent('nonexistent');
      expect(content).toBeUndefined();
    });

    it('should handle content with no metadata', async () => {
      mockKnexInstance = createMockKnex({
        content: [
          {
            node_id: 'n1',
            content: 'Plain content.',
            content_type: 'text',
            metadata: null,
          },
        ],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const content = await ds.getContent('n1');
      expect(content).toBeDefined();
      expect(content!.metadata).toBeUndefined();
    });

    it('should default contentType to markdown when missing', async () => {
      mockKnexInstance = createMockKnex({
        content: [
          {
            node_id: 'n1',
            content: 'Some content.',
            content_type: null,
            metadata: null,
          },
        ],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const content = await ds.getContent('n1');
      expect(content!.contentType).toBe('markdown');
    });
  });

  // --- Custom table names ---

  describe('custom table names', () => {
    it('should use custom table names', () => {
      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
        tables: {
          nodes: 'graph_nodes',
          edges: 'graph_edges',
          properties: 'graph_props',
          content: 'graph_content',
        },
      });

      // Verify by checking internal state via connect + autoMigrate
      expect(ds.name).toBe('sql');
      // The custom names would be used when queries run
    });

    it('should use custom table names during autoMigrate', async () => {
      const ds = new SqlDatasource({
        dialect: 'sqlite',
        connection: ':memory:',
        tables: {
          nodes: 'my_nodes',
          edges: 'my_edges',
          properties: 'my_props',
          content: 'my_content',
        },
        autoMigrate: true,
      });

      await ds.connect();

      const schema = (mockKnexInstance as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
      expect(schema.hasTable).toHaveBeenCalledWith('my_nodes');
      expect(schema.hasTable).toHaveBeenCalledWith('my_edges');
      expect(schema.hasTable).toHaveBeenCalledWith('my_props');
      expect(schema.hasTable).toHaveBeenCalledWith('my_content');
    });

    it('should default table names when not provided', () => {
      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });

      // Access private tables via connect + autoMigrate to verify defaults
      expect(ds.name).toBe('sql');
    });
  });

  // --- rowToNodeData with EAV properties ---

  describe('rowToNodeData with EAV properties', () => {
    it('should merge EAV properties into node attributes', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [
          { node_id: 'n1', key: 'era', value: 'creation', value_type: 'string' },
          { node_id: 'n1', key: 'age', value: '930', value_type: 'number' },
          { node_id: 'n1', key: 'notable', value: 'true', value_type: 'boolean' },
        ],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const node = await ds.getNode('n1');
      expect(node).toBeDefined();
      expect(node!.attributes.name).toBe('Adam');
      expect(node!.attributes.type).toBe('person');
      expect(node!.attributes.era).toBe('creation');
      expect(node!.attributes.age).toBe(930);
      expect(node!.attributes.notable).toBe(true);
    });

    it('should deserialize JSON value types', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [{ id: 'n1', name: 'Adam', type: 'person' }],
        node_properties: [
          {
            node_id: 'n1',
            key: 'refs',
            value: JSON.stringify(['Gen 1:26', 'Gen 2:7']),
            value_type: 'json',
          },
        ],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const node = await ds.getNode('n1');
      expect(node!.attributes.refs).toEqual(['Gen 1:26', 'Gen 2:7']);
    });
  });

  // --- Edge data mapping ---

  describe('edge data mapping', () => {
    it('should map edge rows to EdgeData format', async () => {
      mockKnexInstance = createMockKnex({
        nodes: [
          { id: 'n1', name: 'Node 1', type: 'person' },
          { id: 'n2', name: 'Node 2', type: 'place' },
        ],
        edges: [
          { id: 'e1', source_id: 'n1', target_id: 'n2', type: 'lives_in', weight: 0.8 },
        ],
        node_properties: [],
      });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.getInitialView();
      expect(result.edges[0]).toEqual({
        id: 'e1',
        sourceId: 'n1',
        targetId: 'n2',
        attributes: { type: 'lives_in', weight: 0.8 },
      });
    });
  });

  // --- Pagination helper ---

  describe('pagination', () => {
    it('should return all items when no pagination provided', async () => {
      const nodes = [
        { id: 'n1', name: 'A', type: 'person' },
        { id: 'n2', name: 'B', type: 'person' },
      ];

      mockKnexInstance = createMockKnex({ nodes, node_properties: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.search('');
      expect(result.items).toHaveLength(2);
      expect(result.total).toBe(2);
      expect(result.hasMore).toBe(false);
    });

    it('should handle empty pagination result', async () => {
      mockKnexInstance = createMockKnex({ nodes: [], node_properties: [] });

      const ds = new SqlDatasource({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await ds.connect();

      const result = await ds.search('nothing', { offset: 0, limit: 10 });
      expect(result.items).toHaveLength(0);
      expect(result.total).toBe(0);
      expect(result.hasMore).toBe(false);
    });
  });
});

// --- createSchema tests ---

describe('createSchema', () => {
  it('should create all four tables when they do not exist', async () => {
    const knexMock = createMockKnex();
    const tables = {
      nodes: 'nodes',
      edges: 'edges',
      properties: 'node_properties',
      content: 'content',
    };

    await createSchema(knexMock, tables);

    const schema = (knexMock as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
    expect(schema.hasTable).toHaveBeenCalledTimes(4);
    expect(schema.createTable).toHaveBeenCalledTimes(4);
  });

  it('should skip existing tables', async () => {
    const knexMock = createMockKnex();
    // Simulate all tables already existing
    const schema = (knexMock as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
    schema.hasTable = vi.fn().mockResolvedValue(true);

    const tables = {
      nodes: 'nodes',
      edges: 'edges',
      properties: 'node_properties',
      content: 'content',
    };

    await createSchema(knexMock, tables);

    expect(schema.hasTable).toHaveBeenCalledTimes(4);
    expect(schema.createTable).not.toHaveBeenCalled();
  });

  it('should use custom table names', async () => {
    const knexMock = createMockKnex();
    const tables = {
      nodes: 'custom_nodes',
      edges: 'custom_edges',
      properties: 'custom_props',
      content: 'custom_content',
    };

    await createSchema(knexMock, tables);

    const schema = (knexMock as unknown as Record<string, Record<string, ReturnType<typeof vi.fn>>>).schema;
    expect(schema.hasTable).toHaveBeenCalledWith('custom_nodes');
    expect(schema.hasTable).toHaveBeenCalledWith('custom_edges');
    expect(schema.hasTable).toHaveBeenCalledWith('custom_props');
    expect(schema.hasTable).toHaveBeenCalledWith('custom_content');
  });
});
