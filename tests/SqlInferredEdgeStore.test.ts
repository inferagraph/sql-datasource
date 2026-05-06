import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import {
  SqlInferredEdgeStore,
  sqlInferredEdgeStore,
} from '../src/SqlInferredEdgeStore.js';
import type { InferredEdge } from '@inferagraph/core';

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
  Datasource: class {},
}));

const edgeA: InferredEdge = {
  sourceId: 'n1',
  targetId: 'n2',
  type: 'related_to',
  score: 0.8,
  sources: ['embedding'],
};

const edgeB: InferredEdge = {
  sourceId: 'n2',
  targetId: 'n3',
  type: 'related_to',
  score: 0.7,
  sources: ['llm'],
  reasoning: 'they share a setting',
};

describe('SqlInferredEdgeStore', () => {
  describe('factory', () => {
    it('factory returns a SqlInferredEdgeStore', () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlInferredEdgeStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      expect(store).toBeInstanceOf(SqlInferredEdgeStore);
    });
  });

  describe('set', () => {
    it('bulk-replaces edges (delete + insert)', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlInferredEdgeStore({ knex: mockKnex });

      await store.set([edgeA, edgeB]);

      const del = calls.find((c) => /delete from/i.test(c.sql));
      expect(del).toBeDefined();
      expect(del!.sql).toMatch(/inferagraph_inferred_edges/);

      const ins = calls.filter((c) => /insert into/i.test(c.sql));
      expect(ins.length).toBeGreaterThan(0);
    });
  });

  describe('get', () => {
    it('returns edge by ordered (source, target)', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          source_id: 'n1',
          target_id: 'n2',
          type: 'related_to',
          score: 0.8,
          sources: ['embedding'],
          reasoning: null,
        },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const out = await store.get('n1', 'n2');
      expect(out).toBeDefined();
      expect(out!.sourceId).toBe('n1');
      expect(out!.targetId).toBe('n2');
      expect(out!.score).toBe(0.8);
    });

    it('returns undefined when nothing matches', async () => {
      const { mockKnex } = createCapturingKnex(() => []);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const out = await store.get('n1', 'n99');
      expect(out).toBeUndefined();
    });
  });

  describe('getAllForNode', () => {
    it('returns edges incident to node in either direction', async () => {
      const { mockKnex, calls } = createCapturingKnex(() => [
        {
          source_id: 'n1',
          target_id: 'n2',
          type: 'related_to',
          score: 0.8,
          sources: ['embedding'],
          reasoning: null,
        },
        {
          source_id: 'n3',
          target_id: 'n1',
          type: 'related_to',
          score: 0.6,
          sources: ['graph'],
          reasoning: null,
        },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });

      const out = await store.getAllForNode('n1');
      expect(out).toHaveLength(2);
      const select = calls.find((c) => /select/i.test(c.sql));
      expect(select!.sql).toMatch(/source_id|target_id/i);
    });
  });

  describe('getAll', () => {
    it('returns every stored edge', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          source_id: 'n1',
          target_id: 'n2',
          type: 'related_to',
          score: 0.8,
          sources: ['embedding'],
          reasoning: null,
        },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const out = await store.getAll();
      expect(out).toHaveLength(1);
    });
  });

  describe('searchInferredEdges', () => {
    it('issues vector cosine SQL with LIMIT', async () => {
      const { mockKnex, calls } = createCapturingKnex(() => [
        { id: 'e1', score: 0.95 },
        { id: 'e2', score: 0.85 },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });

      const out = await store.searchInferredEdges([0.1, 0.2, 0.3], 2);
      expect(out).toHaveLength(2);
      const select = calls.find((c) => /select/i.test(c.sql));
      expect(select).toBeDefined();
      expect(select!.sql).toMatch(/<=>/);
      expect(select!.sql).toMatch(/limit/i);
    });
  });

  describe('clear', () => {
    it('removes every entry', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      await store.clear();
      const del = calls.find((c) => /delete from|truncate/i.test(c.sql));
      expect(del).toBeDefined();
      expect(del!.sql).toMatch(/inferagraph_inferred_edges/);
    });
  });

  describe('set with empty input', () => {
    it('clears the table even when no edges supplied', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      await store.set([]);
      const del = calls.find((c) => /delete from/i.test(c.sql));
      expect(del).toBeDefined();
      const inserts = calls.filter((c) => /insert into/i.test(c.sql));
      expect(inserts).toHaveLength(0);
    });

    it('dedupes ordered (source, target) keeping the LAST occurrence', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const a1 = { ...edgeA, score: 0.5 };
      const a2 = { ...edgeA, score: 0.99 };
      await store.set([a1, a2]);
      const inserts = calls.filter((c) => /insert into/i.test(c.sql));
      expect(inserts).toHaveLength(1);
      expect(inserts[0].bindings).toContain(0.99);
    });
  });

  describe('error paths', () => {
    it('constructor throws when neither knex nor dialect provided', () => {
      expect(() => new SqlInferredEdgeStore({})).toThrow(/requires either/i);
    });
  });

  describe('row decoding', () => {
    it('parses pg array literal sources as string', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          source_id: 'n1',
          target_id: 'n2',
          type: 'related_to',
          score: 0.5,
          sources: '{embedding,llm}',
          reasoning: null,
        },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const out = await store.get('n1', 'n2');
      expect(out!.sources).toEqual(['embedding', 'llm']);
    });

    it('returns reasoning when populated', async () => {
      const { mockKnex } = createCapturingKnex(() => [
        {
          source_id: 'n1',
          target_id: 'n2',
          type: 'related_to',
          score: 0.5,
          sources: ['llm'],
          reasoning: 'because reasons',
        },
      ]);
      const store = new SqlInferredEdgeStore({ knex: mockKnex });
      const out = await store.get('n1', 'n2');
      expect(out!.reasoning).toBe('because reasons');
    });
  });

  describe('disconnect', () => {
    it('destroys knex when owned', async () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlInferredEdgeStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await store.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).toHaveBeenCalled();
    });
  });
});
