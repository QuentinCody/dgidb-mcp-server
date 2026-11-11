import {getRequestLicenseProfile, type MaybeExtra} from "@bio-mcp/shared";
import {assertReadOnlySql} from "@bio-mcp/shared/staging/sql-guard";
import {fetchDgidb} from "./graphql-fetch";
import type {DgidbEnv} from "./datasets";
import {errorResult, toolResponse, type DgidbToolResult} from "./tool-response";

export async function graphqlHandler(args: {query: string; variables?: Record<string, unknown>}, env: DgidbEnv, extra?: Record<string, unknown>): Promise<DgidbToolResult> {
  try {
    // A reselling caller gets the source-license rule (./commercial-profile); a refusal is a GraphQL error.
    const licenseProfile = getRequestLicenseProfile(extra as MaybeExtra | undefined);
    const result = licenseProfile ? await fetchDgidb(args.query, args.variables, {licenseProfile}) : await fetchDgidb(args.query, args.variables);
    const errors = Array.isArray(result.errors) ? result.errors : [];
    const hasData = !!result.data && typeof result.data === "object" && Object.values(result.data).some(value => value !== null);
    const status = errors.length ? {success: false, partial: hasData, error_count: errors.length,
      error: {code: "GRAPHQL_ERROR", message: "DGIdb returned GraphQL errors; inspect the preserved native envelope"}} : {success: true};
    return await toolResponse({tool: "dgidb_graphql_query", args, result, env, extra, status, cite: !errors.length || hasData});
  } catch (error) {return errorResult("EXECUTION_FAILED", error instanceof Error ? error.message : String(error));}
}

export interface SqlArgs {
  data_access_id: string;
  sql: string;
  params?: string[];
  include_quality_analysis?: boolean;
}

async function sqlRead(args: SqlArgs, env: DgidbEnv): Promise<Record<string, unknown>> {
  if (args.params?.length) throw new Error("SQL parameter binding is unsupported by this endpoint; params must be omitted or empty");
  const sql = assertReadOnlySql(args.sql);
  if (!env.DGIDB_DATA_DO) throw new Error("DGIDB_DATA_DO binding not available");
  const stub = env.DGIDB_DATA_DO.get(env.DGIDB_DATA_DO.idFromName(args.data_access_id));
  const response = await stub.fetch(new Request("https://do/query-enhanced", {method: "POST",
    headers: {"Content-Type": "application/json"}, body: JSON.stringify({sql}), signal: AbortSignal.timeout(30000)}));
  if (!response.ok) throw new Error(`SQL execution failed: HTTP ${response.status}`);
  const data: unknown = await response.json();
  if (!data || typeof data !== "object" || Array.isArray(data)) throw new Error("Invalid SQL response");
  // SAFETY: JSON object narrowed above; the DO's explicit failure flag is checked next.
  const result = data as Record<string, unknown>;
  if (result.success === false) throw new Error(typeof result.error === "string" ? result.error : "SQL query failed");
  if (args.include_quality_analysis) result.quality_analysis_note = "Quality analysis feature available - contact for details";
  return result;
}

export async function sqlHandler(args: SqlArgs, env: DgidbEnv, extra?: Record<string, unknown>): Promise<DgidbToolResult> {
  try {
    const result = await sqlRead(args, env);
    return await toolResponse({tool: "dgidb_query_sql", args: {...args}, result, env, extra});
  } catch (error) {return errorResult("EXECUTION_FAILED", error instanceof Error ? error.message : String(error));}
}
