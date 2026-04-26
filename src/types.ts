export interface SqlDatasourceConfig {
  connection: string | Record<string, unknown>; // Knex connection config
  dialect: 'postgres' | 'mysql' | 'sqlite' | 'mssql';
  tables?: {
    nodes?: string; // default: 'nodes'
    edges?: string; // default: 'edges'
    properties?: string; // default: 'node_properties'
    content?: string; // default: 'content'
  };
  autoMigrate?: boolean; // create tables on connect (default: false)
}

export interface TableNames {
  nodes: string;
  edges: string;
  properties: string;
  content: string;
}
