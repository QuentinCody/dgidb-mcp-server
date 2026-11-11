import {stageToDoAndRespond} from "@bio-mcp/shared/staging/utils";

export interface DgidbEnv {
  DGIDB_DATA_DO?: DurableObjectNamespace;
  WORKSPACE_DO?: DurableObjectNamespace;
  CODE_MODE_LOADER?: {get: (...args: unknown[]) => unknown};
}

// Preserve the existing /datasets management surface for passthrough/SQL results.
export const datasetRegistry = new Map<string, {created: string; table_count?: number; total_rows?: number}>();

export interface StagedDataset {
  staged: boolean;
  data_access_id: string;
  processing_details: {table_count?: number; total_rows?: number};
  _staging: Awaited<ReturnType<typeof stageToDoAndRespond>>["_staging"];
}

export async function stageResult(result: unknown, env: DgidbEnv, provenance: {toolName: string; args: Record<string, unknown>}, extra?: Record<string, unknown>): Promise<StagedDataset> {
  if (!env.DGIDB_DATA_DO) throw new Error("Response exceeds 30KB; configure DGIDB_DATA_DO or narrow the query");
  const staged = await stageToDoAndRespond(result, env.DGIDB_DATA_DO, "dgidb", undefined,
    {...provenance, serverName: "dgidb"}, "dgidb", extra);
  const processing_details = {table_count: staged.tablesCreated?.length, total_rows: staged.totalRows};
  datasetRegistry.set(staged.dataAccessId, {created: new Date().toISOString(), ...processing_details});
  return {staged: true, data_access_id: staged.dataAccessId, processing_details,
    _staging: {...staged._staging, payload_size_bytes: new TextEncoder().encode(JSON.stringify(result)).byteLength}};
}
