import type { Knex } from 'knex';
import type { TableNames } from './types.js';

export async function createSchema(knex: Knex, tables: TableNames): Promise<void> {
  // Create nodes table
  if (!(await knex.schema.hasTable(tables.nodes))) {
    await knex.schema.createTable(tables.nodes, (table) => {
      table.string('id', 255).primary();
      table.string('name', 500).notNullable();
      table.string('type', 100).notNullable();
      table.timestamp('created_at').defaultTo(knex.fn.now());
      table.timestamp('updated_at').defaultTo(knex.fn.now());
    });
  }

  // Create edges table
  if (!(await knex.schema.hasTable(tables.edges))) {
    await knex.schema.createTable(tables.edges, (table) => {
      table.string('id', 255).primary();
      table
        .string('source_id', 255)
        .notNullable()
        .references('id')
        .inTable(tables.nodes)
        .onDelete('CASCADE');
      table
        .string('target_id', 255)
        .notNullable()
        .references('id')
        .inTable(tables.nodes)
        .onDelete('CASCADE');
      table.string('type', 100).notNullable();
      table.float('weight').defaultTo(1.0);
      table.timestamp('created_at').defaultTo(knex.fn.now());
      table.index(['source_id']);
      table.index(['target_id']);
    });
  }

  // Create node_properties table (EAV pattern)
  if (!(await knex.schema.hasTable(tables.properties))) {
    await knex.schema.createTable(tables.properties, (table) => {
      table
        .string('node_id', 255)
        .notNullable()
        .references('id')
        .inTable(tables.nodes)
        .onDelete('CASCADE');
      table.string('key', 255).notNullable();
      table.text('value');
      table.string('value_type', 50).defaultTo('string');
      table.primary(['node_id', 'key']);
    });
  }

  // Create content table
  if (!(await knex.schema.hasTable(tables.content))) {
    await knex.schema.createTable(tables.content, (table) => {
      table
        .string('node_id', 255)
        .primary()
        .references('id')
        .inTable(tables.nodes)
        .onDelete('CASCADE');
      table.text('content').notNullable();
      table.string('content_type', 50).defaultTo('markdown');
      table.text('metadata'); // JSON string
      table.timestamp('updated_at').defaultTo(knex.fn.now());
    });
  }
}
