import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import {
  SqlConversationStore,
  sqlConversationStore,
} from '../src/SqlConversationStore.js';

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

describe('SqlConversationStore', () => {
  describe('factory', () => {
    it('factory returns a SqlConversationStore', () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlConversationStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      expect(store).toBeInstanceOf(SqlConversationStore);
    });
  });

  describe('appendTurn', () => {
    it('INSERTs row without expires_at when no ttlSeconds set', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex });

      await store.appendTurn('conv-1', {
        role: 'user',
        content: 'hello',
        timestamp: 1700000000000,
      });

      const ins = calls.find((c) => /insert into/i.test(c.sql));
      expect(ins).toBeDefined();
      expect(ins!.sql).toMatch(/inferagraph_conversations/);
      expect(ins!.bindings).toContain('conv-1');
      expect(ins!.bindings).toContain('user');
      expect(ins!.bindings).toContain('hello');
    });

    it('INSERTs row with expires_at when ttlSeconds set', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex, ttlSeconds: 3600 });

      await store.appendTurn('conv-1', {
        role: 'assistant',
        content: 'hi',
        timestamp: 1700000000000,
      });

      const ins = calls.find((c) => /insert into/i.test(c.sql));
      expect(ins).toBeDefined();
      // ttlSeconds is bound or substituted into SQL
      expect(ins!.sql.toLowerCase()).toMatch(/expires_at|interval/);
    });
  });

  describe('getTurns', () => {
    it('SELECTs only non-expired rows', async () => {
      const { mockKnex, calls } = createCapturingKnex(() => [
        {
          role: 'user',
          content: 'hello',
          timestamp: 1700000000000,
          retrieved_node_ids: null,
        },
      ]);
      const store = new SqlConversationStore({ knex: mockKnex });
      await store.getTurns('conv-1', 10);

      const sel = calls.find((c) => /select/i.test(c.sql));
      expect(sel).toBeDefined();
      expect(sel!.sql.toLowerCase()).toMatch(/expires_at is null|expires_at >/);
    });

    it('reverses to oldest -> newest order', async () => {
      // SQL returns DESC; the store reverses for ChronologicalOrder.
      const rows = [
        { role: 'assistant', content: 'newer', timestamp: 200, retrieved_node_ids: null },
        { role: 'user', content: 'older', timestamp: 100, retrieved_node_ids: null },
      ];
      const { mockKnex } = createCapturingKnex(() => rows);
      const store = new SqlConversationStore({ knex: mockKnex });

      const out = await store.getTurns('conv-1', 10);
      expect(out.map((t) => t.content)).toEqual(['older', 'newer']);
    });

    it('returns empty array when no turns exist', async () => {
      const { mockKnex } = createCapturingKnex(() => []);
      const store = new SqlConversationStore({ knex: mockKnex });
      const out = await store.getTurns('conv-1', 10);
      expect(out).toEqual([]);
    });
  });

  describe('clear', () => {
    it('DELETEs all rows for the conversation', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex });
      await store.clear('conv-1');

      const del = calls.find((c) => /delete from/i.test(c.sql));
      expect(del).toBeDefined();
      expect(del!.sql).toMatch(/inferagraph_conversations/);
      expect(del!.bindings).toContain('conv-1');
    });
  });

  describe('cleanup', () => {
    it('DELETEs rows whose expires_at is in the past', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex });
      await store.cleanup();

      const del = calls.find((c) => /delete from/i.test(c.sql));
      expect(del).toBeDefined();
      expect(del!.sql.toLowerCase()).toMatch(/expires_at <|expires_at < now/);
    });
  });

  describe('error paths', () => {
    it('constructor throws when neither knex nor dialect provided', () => {
      expect(() => new SqlConversationStore({})).toThrow(/requires either/i);
    });
  });

  describe('appendTurn with retrievedNodeIds', () => {
    it('serializes ids into pg array literal', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex });
      await store.appendTurn('conv-1', {
        role: 'assistant',
        content: 'a',
        timestamp: 1,
        retrievedNodeIds: ['n1', 'n2'],
      });

      const ins = calls.find((c) => /insert into/i.test(c.sql));
      expect(ins).toBeDefined();
      expect(ins!.bindings).toContain('{n1,n2}');
    });

    it('passes null when retrievedNodeIds is empty array', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const store = new SqlConversationStore({ knex: mockKnex });
      await store.appendTurn('conv-1', {
        role: 'user',
        content: 'q',
        timestamp: 1,
        retrievedNodeIds: [],
      });

      const ins = calls.find((c) => /insert into/i.test(c.sql));
      expect(ins!.bindings).toContain(null);
    });
  });

  describe('row decoding', () => {
    it('returns retrievedNodeIds when stored as pg array string', async () => {
      const rows = [
        {
          role: 'assistant',
          content: 'response',
          timestamp: 100,
          retrieved_node_ids: '{n1,n2}',
        },
      ];
      const { mockKnex } = createCapturingKnex(() => rows);
      const store = new SqlConversationStore({ knex: mockKnex });
      const out = await store.getTurns('conv-1', 10);
      expect(out[0].retrievedNodeIds).toEqual(['n1', 'n2']);
    });

    it('returns retrievedNodeIds when stored as JS array', async () => {
      const rows = [
        {
          role: 'assistant',
          content: 'response',
          timestamp: 100,
          retrieved_node_ids: ['n1', 'n2'],
        },
      ];
      const { mockKnex } = createCapturingKnex(() => rows);
      const store = new SqlConversationStore({ knex: mockKnex });
      const out = await store.getTurns('conv-1', 10);
      expect(out[0].retrievedNodeIds).toEqual(['n1', 'n2']);
    });

    it('omits retrievedNodeIds when null', async () => {
      const rows = [
        {
          role: 'user',
          content: 'q',
          timestamp: 100,
          retrieved_node_ids: null,
        },
      ];
      const { mockKnex } = createCapturingKnex(() => rows);
      const store = new SqlConversationStore({ knex: mockKnex });
      const out = await store.getTurns('conv-1', 10);
      expect(out[0].retrievedNodeIds).toBeUndefined();
    });

    it('coerces unknown role to "user"', async () => {
      const rows = [
        {
          role: 'system',
          content: 'msg',
          timestamp: 100,
          retrieved_node_ids: null,
        },
      ];
      const { mockKnex } = createCapturingKnex(() => rows);
      const store = new SqlConversationStore({ knex: mockKnex });
      const out = await store.getTurns('conv-1', 10);
      expect(out[0].role).toBe('user');
    });
  });

  describe('disconnect', () => {
    it('destroys knex when owned', async () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const store = sqlConversationStore({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await store.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).toHaveBeenCalled();
    });
  });
});
