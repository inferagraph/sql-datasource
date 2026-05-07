import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import {
  SqlVectorEmbeddingStore,
  sqlVectorEmbeddingStore,
} from '../src/SqlVectorEmbeddingStore.js';
import type { EmbeddingRecord } from '@inferagraph/core';

// --- Capturing mock knex ---

interface CapturedCall {
  sql: string;
  bindings: readonly unknown[];
}

function createCapturingKnex(rowsFor: (sql: string) => Record<string, unknown>[] = () => []) {
  const calls: CapturedCall[] = [];
  const mockKnex = (() => undefined) as unknown as Knex;
  Object.assign(mockKnex, {
    raw: vi.fn((sql: string, bindings?: readonly unknown[]) => {
      calls.push({ sql, bindings: bindings ?? [] });
      return Promise.resolve({ rows: rowsFor(sql) });
    }),
    destroy: vi.fn().mockResolvedValue(undefined),
  });
  return { mockKnex, calls };
}

let factoryMockKnex: Knex;

vi.mock('knex', () => ({
  default: vi.fn(() => factoryMockKnex),
}));

vi.mock('@inferagraph/core', () => ({
  DataSource: class {},
}));

describe('SqlVectorEmbeddingStore', () => {
  const sampleRecord: EmbeddingRecord = {
    nodeId: 'n1',
    vector: [0.1, 0.2, 0.3],
    meta: {
      model: 'text-embedding-3-large',
      modelVersion: '1',
      generatedAt: '2026-05-06T00:00:00Z',
      contentHash: 'abc123',
    },
  };

  describe('factory', () => {
    it('factory returns a SqlVectorEmbeddingStore', () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlVectorEmbeddingStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      expect(store).toBeInstanceOf(SqlVectorEmbeddingStore);
    });
  });

  describe('set', () => {
    it('inserts row with provenance metadata', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });

      await store.set(sampleRecord);

      const insert = calls.find((c) => /insert into/i.test(c.sql));
      expect(insert).toBeDefined();
      expect(insert!.sql).toMatch(/inferagraph_embeddings/);
      // bindings include node id, vector (JSON-stringified), model, version, hash
      expect(insert!.bindings).toContain('n1');
      expect(insert!.bindings).toContain('text-embedding-3-large');
      expect(insert!.bindings).toContain('abc123');
    });
  });

  describe('get', () => {
    it('returns record when row exists with matching hash', async () => {
      const row = {
        node_id: 'n1',
        embedding: '[0.1,0.2,0.3]',
        embedding_model: 'text-embedding-3-large',
        embedding_version: '1',
        embedding_hash: 'abc123',
        generated_at: '2026-05-06T00:00:00Z',
      };
      const { mockKnex } = createCapturingKnex((sql) =>
        /select/i.test(sql) ? [row] : [],
      );
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });

      const result = await store.get('n1', 'text-embedding-3-large', '1', 'abc123');
      expect(result).toBeDefined();
      expect(result!.nodeId).toBe('n1');
      expect(result!.vector).toEqual([0.1, 0.2, 0.3]);
      expect(result!.meta.contentHash).toBe('abc123');
    });

    it('returns undefined when row hash differs', async () => {
      const { mockKnex } = createCapturingKnex(() => []);
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });

      const result = await store.get('n1', 'text-embedding-3-large', '1', 'mismatched-hash');
      expect(result).toBeUndefined();
    });
  });

  describe('similar', () => {
    it('returns hits ordered by descending score', async () => {
      const rows = [
        { node_id: 'n1', score: 0.9 },
        { node_id: 'n2', score: 0.7 },
      ];
      const { mockKnex } = createCapturingKnex((sql) =>
        /select/i.test(sql) ? rows : [],
      );
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });
      const out = await store.similar([0.1, 0.2, 0.3], 5);
      expect(out).toEqual([
        { nodeId: 'n1', score: 0.9 },
        { nodeId: 'n2', score: 0.7 },
      ]);
    });
  });

  describe('searchVector', () => {
    it('issues SELECT with vector cosine operator and LIMIT', async () => {
      const { mockKnex, calls } = createCapturingKnex((sql) =>
        /select/i.test(sql)
          ? [{ node_id: 'n1', score: 0.9 }, { node_id: 'n2', score: 0.7 }]
          : [],
      );
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });

      const out = await store.searchVector!([0.1, 0.2, 0.3], { top: 2 });
      expect(out).toHaveLength(2);
      expect(out[0]).toEqual({ nodeId: 'n1', score: 0.9 });

      const select = calls.find((c) => /select/i.test(c.sql));
      expect(select).toBeDefined();
      expect(select!.sql).toMatch(/<=>/); // pgvector cosine distance operator
      expect(select!.sql).toMatch(/limit/i);
    });
  });

  describe('clear', () => {
    it('truncates the table', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });
      await store.clear();

      const truncate = calls.find((c) => /delete from|truncate/i.test(c.sql));
      expect(truncate).toBeDefined();
      expect(truncate!.sql).toMatch(/inferagraph_embeddings/);
    });
  });

  describe('custom tableName', () => {
    it('uses configured tableName in queries', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlVectorEmbeddingStore({
        knex: mockKnex,
        tableName: 'custom_vec',
      });

      await store.set(sampleRecord);
      const insert = calls.find((c) => /insert into/i.test(c.sql));
      expect(insert!.sql).toMatch(/custom_vec/);
    });
  });

  describe('error paths', () => {
    it('constructor throws when neither knex nor dialect provided', () => {
      expect(() => new SqlVectorEmbeddingStore({})).toThrow(
        /requires either/i,
      );
    });
  });

  describe('similar with model + version filters', () => {
    it('appends model + version WHERE clauses when provided', async () => {
      const { mockKnex, calls } = createCapturingKnex(() => [
        { node_id: 'n1', score: 0.9 },
      ]);
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });

      await store.similar([0.1, 0.2], 5, 'text-embedding-3-large', '1');
      const select = calls.find((c) => /select/i.test(c.sql));
      expect(select!.sql).toMatch(/embedding_model = \?/);
      expect(select!.sql).toMatch(/embedding_version = \?/);
    });
  });

  describe('disconnect', () => {
    it('destroys knex when constructed via dialect/connection', async () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlVectorEmbeddingStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await store.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).toHaveBeenCalled();
    });

    it('does not destroy a passed-in knex', async () => {
      const { mockKnex } = createCapturingKnex();
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });
      await store.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).not.toHaveBeenCalled();
    });
  });

  describe('row decoding', () => {
    it('parses pgvector array literal returned as a string', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          node_id: 'n1',
          embedding: '[0.5,0.6,0.7]',
          embedding_model: 'm',
          embedding_version: 'v',
          embedding_hash: 'h',
          generated_at: '2026-01-01',
        },
      ]);
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });
      const out = await store.get('n1', 'm', 'v', 'h');
      expect(out!.vector).toEqual([0.5, 0.6, 0.7]);
    });

    it('parses pgvector array literal returned as a JS array', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          node_id: 'n1',
          embedding: [1, 2, 3],
          embedding_model: 'm',
          embedding_version: 'v',
          embedding_hash: 'h',
          generated_at: '2026-01-01',
        },
      ]);
      const store = new SqlVectorEmbeddingStore({ knex: mockKnex });
      const out = await store.get('n1', 'm', 'v', 'h');
      expect(out!.vector).toEqual([1, 2, 3]);
    });
  });
});
