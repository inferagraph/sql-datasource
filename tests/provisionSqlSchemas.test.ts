import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import { provisionSqlSchemas } from '../src/provisionSqlSchemas.js';

interface CapturedCall {
  sql: string;
  bindings: readonly unknown[];
}

function createCapturingKnex() {
  const calls: CapturedCall[] = [];
  const mockKnex = (() => undefined) as unknown as Knex;

  Object.assign(mockKnex, {
    raw: vi.fn((sql: string, bindings?: readonly unknown[]) => {
      calls.push({ sql, bindings: bindings ?? [] });
      return Promise.resolve({ rows: [] });
    }),
    destroy: vi.fn().mockResolvedValue(undefined),
  });

  return { mockKnex, calls };
}

let mockKnexInstance: Knex;
let mockCalls: CapturedCall[];

vi.mock('knex', () => ({
  default: vi.fn(() => mockKnexInstance),
}));

vi.mock('@inferagraph/core', () => ({
  Datasource: class {},
}));

describe('provisionSqlSchemas', () => {
  it('issues CREATE EXTENSION + CREATE TABLE IF NOT EXISTS for each enabled table', async () => {
    const { mockKnex, calls } = createCapturingKnex();
    mockKnexInstance = mockKnex;
    mockCalls = calls;

    await provisionSqlSchemas({
      connectionString: 'postgres://localhost/test',
      embeddings: true,
      inferredEdges: true,
      conversations: true,
      cache: true,
    });

    const sqls = mockCalls.map((c) => c.sql);
    expect(sqls.some((s) => /create extension if not exists vector/i.test(s))).toBe(true);
    expect(
      sqls.some((s) => /create table if not exists.*inferagraph_embeddings/i.test(s)),
    ).toBe(true);
    expect(
      sqls.some((s) => /create table if not exists.*inferagraph_inferred_edges/i.test(s)),
    ).toBe(true);
    expect(
      sqls.some((s) => /create table if not exists.*inferagraph_conversations/i.test(s)),
    ).toBe(true);
    expect(
      sqls.some((s) => /create table if not exists.*inferagraph_cache/i.test(s)),
    ).toBe(true);
  });

  it('respects custom tablePrefix', async () => {
    const { mockKnex, calls } = createCapturingKnex();
    mockKnexInstance = mockKnex;
    mockCalls = calls;

    await provisionSqlSchemas({
      connectionString: 'postgres://localhost/test',
      tablePrefix: 'myapp_',
      embeddings: true,
      inferredEdges: true,
      conversations: true,
      cache: true,
    });

    const sqls = mockCalls.map((c) => c.sql);
    expect(sqls.some((s) => /myapp_embeddings/.test(s))).toBe(true);
    expect(sqls.some((s) => /myapp_inferred_edges/.test(s))).toBe(true);
    expect(sqls.some((s) => /myapp_conversations/.test(s))).toBe(true);
    expect(sqls.some((s) => /myapp_cache/.test(s))).toBe(true);
  });

  it('uses configured embeddingDimensions in vector column', async () => {
    const { mockKnex, calls } = createCapturingKnex();
    mockKnexInstance = mockKnex;
    mockCalls = calls;

    await provisionSqlSchemas({
      connectionString: 'postgres://localhost/test',
      embeddingDimensions: 1024,
      embeddings: true,
    });

    const embeddingsTable = mockCalls
      .map((c) => c.sql)
      .find((s) => /inferagraph_embeddings/.test(s));
    expect(embeddingsTable).toMatch(/vector\(1024\)/);
  });

  it('skips pgvector extension when only cache + conversations requested', async () => {
    const { mockKnex, calls } = createCapturingKnex();
    mockKnexInstance = mockKnex;
    mockCalls = calls;

    await provisionSqlSchemas({
      connectionString: 'postgres://localhost/test',
      conversations: true,
      cache: true,
    });

    const sqls = mockCalls.map((c) => c.sql);
    expect(sqls.some((s) => /create extension/i.test(s))).toBe(false);
    expect(sqls.some((s) => /inferagraph_conversations/.test(s))).toBe(true);
    expect(sqls.some((s) => /inferagraph_cache/.test(s))).toBe(true);
  });

  it('throws when neither connection nor knex provided', async () => {
    await expect(provisionSqlSchemas({ embeddings: true })).rejects.toThrow(
      /requires connectionString/i,
    );
  });

  it('uses passed-in knex without destroying it', async () => {
    const { mockKnex } = createCapturingKnex();
    await provisionSqlSchemas({ knex: mockKnex, embeddings: true });
    expect((mockKnex as unknown as { destroy: ReturnType<typeof vi.fn> }).destroy).not.toHaveBeenCalled();
  });
});
