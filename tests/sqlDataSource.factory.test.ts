import { describe, it, expect, vi } from 'vitest';
import type { Knex } from 'knex';
import { sqlDataSource, SqlDataSource } from '../src/SqlDataSource.js';

// Minimal knex mock — the factory should construct a SqlDataSource without
// actually connecting. We never call .connect() here.
let mockKnexInstance: Knex;

vi.mock('knex', () => ({
  default: vi.fn(() => mockKnexInstance),
}));

vi.mock('@inferagraph/core', () => ({
  Datasource: class {},
}));

describe('sqlDataSource factory', () => {
  it('returns a SqlDataSource instance', () => {
    const ds = sqlDataSource({
      dialect: 'postgres',
      connection: 'postgres://localhost/test',
    });
    expect(ds).toBeInstanceOf(SqlDataSource);
    expect(ds.name).toBe('sql');
  });
});
