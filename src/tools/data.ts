import type {McpServer} from "@bio-mcp/shared/mcp";
import {z} from "zod";
import {createGetSchemaHandler, createQueryDataHandler} from "@bio-mcp/shared/staging/utils";
import type {DgidbEnv} from "../lib/datasets";
import {dgidbResponse} from "../lib/tool-result";

export function registerQueryData(server: McpServer, env: DgidbEnv): void {
  const handler = createQueryDataHandler("DGIDB_DATA_DO", "dgidb", {workspaceNamespace: env.WORKSPACE_DO});
  for (const name of ["dgidb_query_data", "mcp_dgidb_query_data"]) server.registerTool(name, {
    title: "Query Staged DGIdb Data",
    description: "Query staged DGIdb data using read-only SQL. Pass workspace for cross-server joins; omit it for a per-server data_access_id.",
    inputSchema: {data_access_id: z.string().min(1).optional(), sql: z.string().min(1),
      limit: z.number().int().positive().max(10000).default(100).optional(), workspace: z.string().min(1).optional()},
  }, async (args, extra) => {
    // SAFETY: handler consumes named Worker bindings; SDK extra optionally supplies the same environment.
    const environment = (extra as {env?: Record<string, unknown>})?.env ?? {...env};
    return dgidbResponse(() => handler(args, environment), environment as DgidbEnv, "dgidb_query_data", extra);
  });
}

export function registerGetSchema(server: McpServer, env: DgidbEnv): void {
  const handler = createGetSchemaHandler("DGIDB_DATA_DO", "dgidb", {workspaceNamespace: env.WORKSPACE_DO});
  for (const name of ["dgidb_get_schema", "mcp_dgidb_get_schema"]) server.registerTool(name, {
    title: "Get Staged DGIdb Data Schema",
    description: "Inspect a staged dataset, or omit data_access_id to list this session's datasets. Pass workspace and optional dataset for the shared workspace catalog.",
    inputSchema: {data_access_id: z.string().min(1).optional(), workspace: z.string().min(1).optional(), dataset: z.string().min(1).optional()},
  }, async (args, extra) => {
    // SAFETY: Worker bindings and SDK scope are forwarded to their shared resolvers.
    const environment = (extra as {env?: Record<string, unknown>})?.env ?? {...env};
    return dgidbResponse(() => handler(args, environment, extra as Record<string, unknown>), environment as DgidbEnv, "dgidb_get_schema", extra);
  });
}
