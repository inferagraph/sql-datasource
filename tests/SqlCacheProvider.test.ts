import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import {
  SqlCacheProvider,
  sqlCacheProvider,
} from '../src/SqlCacheProvider.js';

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

vi.mock('@inferagraph/core/data', () => ({
  DataSource: class {},
}));

describe('SqlCacheProvider', () => {
  describe('factory', () => {
    it('factory returns a SqlCacheProvider', () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const cache = sqlCacheProvider({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      expect(cache).toBeInstanceOf(SqlCacheProvider);
    });
  });

  describe('set', () => {
    it('UPSERTs without expires_at when no ttl configured', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex });
      await cache.set('k1', 'v1');

      const upsert = calls.find((c) => /insert into/i.test(c.sql));
      expect(upsert).toBeDefined();
      expect(upsert!.sql).toMatch(/inferagraph_cache/);
      expect(upsert!.sql.toLowerCase()).toMatch(/on conflict/);
      expect(upsert!.bindings).toContain('k1');
      expect(upsert!.bindings).toContain('v1');
    });

    it('writes expires_at = NOW() + opts.ttlSeconds when provided', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex });
      await cache.set('k2', 'v2', { ttlSeconds: 60 });

      const upsert = calls.find((c) => /insert into/i.test(c.sql));
      expect(upsert).toBeDefined();
      expect(upsert!.sql.toLowerCase()).toMatch(/now\(\)|interval/);
      expect(upsert!.bindings).toContain(60);
    });

    it('falls back to defaultTtlSeconds when opts omitted', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex, defaultTtlSeconds: 120 });
      await cache.set('k3', 'v3');

      const upsert = calls.find((c) => /insert into/i.test(c.sql));
      expect(upsert).toBeDefined();
      expect(upsert!.bindings).toContain(120);
    });
  });

  describe('get', () => {
    it('returns value when row exists and is not expired', async () => {
      const { mockKnex, calls } = createCapturingKnex(() => [{ value: 'v1' }]);
      const cache = new SqlCacheProvider({ knex: mockKnex });

      const v = await cache.get('k1');
      expect(v).toBe('v1');

      const sel = calls.find((c) => /select/i.test(c.sql));
      expect(sel).toBeDefined();
      // filters by expires_at NULL OR expires_at > NOW()
      expect(sel!.sql.toLowerCase()).toMatch(/expires_at is null|expires_at >/);
    });

    it('returns undefined when row missing', async () => {
      const { mockKnex } = createCapturingKnex(() => []);
      const cache = new SqlCacheProvider({ knex: mockKnex });
      const v = await cache.get('missing');
      expect(v).toBeUndefined();
    });
  });

  describe('delete', () => {
    it('DELETEs the row by key', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex });
      await cache.delete('k1');

      const del = calls.find((c) => /delete from/i.test(c.sql));
      expect(del).toBeDefined();
      expect(del!.bindings).toContain('k1');
    });
  });

  describe('clear', () => {
    it('truncates the table', async () => {
      const { mockKnex, calls } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex });
      await cache.clear();

      const truncate = calls.find((c) => /delete from|truncate/i.test(c.sql));
      expect(truncate).toBeDefined();
      expect(truncate!.sql).toMatch(/inferagraph_cache/);
    });
  });

  describe('error paths', () => {
    it('constructor throws when neither knex nor dialect provided', () => {
      expect(() => new SqlCacheProvider({})).toThrow(/requires either/i);
    });
  });

  describe('disconnect', () => {
    it('destroys knex when owned', async () => {
      const { mockKnex } = createCapturingKnex();
      factoryMockKnex = mockKnex;
      const cache = sqlCacheProvider({
        dialect: 'postgres',
        connection: 'postgres://localhost/test',
      });
      await cache.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).toHaveBeenCalled();
    });

    it('does not destroy a passed-in knex', async () => {
      const { mockKnex } = createCapturingKnex();
      const cache = new SqlCacheProvider({ knex: mockKnex });
      await cache.disconnect();
      expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).not.toHaveBeenCalled();
    });
  });

  describe('get', () => {
    it('returns empty string when value column is null/missing', async () => {
      const { mockKnex } = createCapturingKnex(() => [{ value: null }]);
      const cache = new SqlCacheProvider({ knex: mockKnex });
      const v = await cache.get('k1');
      expect(v).toBe('');
    });
  });
});
