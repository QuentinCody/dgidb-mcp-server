import type {McpServer} from "@bio-mcp/shared/mcp";
import {z} from "zod";
import type {DgidbEnv} from "../lib/datasets";
import {graphqlHandler, sqlHandler} from "../lib/legacy-handlers";

export function registerGraphql(server: McpServer, env: DgidbEnv): void {
  for (const name of ["dgidb_graphql_query", "mcp_dgidb_graphql_query"]) server.tool(name,
    "Query DGIdb GraphQL. Large responses stage losslessly in SQLite, including introspection and partial errors. Top-level genes, drugs and interactions use connections; nested interactions and claims are unpaged lists. Preserve claim-level sources/publications separately from interaction ranking. Use schema discovery for valid fields.",
    {query: z.string().min(1), variables: z.record(z.string(), z.unknown()).optional()},
    // SAFETY: SDK extra is the shared staging scope carrier; no shape-specific fields are read here.
    async (args, extra) => graphqlHandler(args, env, extra as Record<string, unknown>));
}

export function registerSql(server: McpServer, env: DgidbEnv): void {
  for (const name of ["dgidb_query_sql", "mcp_dgidb_query_sql"]) server.tool(name,
    "Execute read-only SQL against a staged DGIdb dataset; large results are staged with a new handle. The params field is retained for compatibility but nonempty bindings are unsupported and rejected.",
    {data_access_id: z.string().min(1), sql: z.string().min(1), params: z.array(z.string()).optional(), include_quality_analysis: z.boolean().optional()},
    // SAFETY: SDK extra passes directly to the shared staging scope resolver.
    async (args, extra) => sqlHandler(args, env, extra as Record<string, unknown>));
}
