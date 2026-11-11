import { shouldStage, stageToDoAndRespond } from "@bio-mcp/shared/staging/utils";

type Result = { [key: string]: unknown; content: Array<{type: "text"; text: string}>;
  structuredContent: Record<string, unknown>; isError?: boolean };
export type DgidbDataEnv = {DGIDB_DATA_DO?: Parameters<typeof stageToDoAndRespond>[1]};
const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;

/** Preserve complete legacy responses while enforcing the MCP byte and error contracts. */
export async function dgidbResponse(work: () => Promise<Omit<Result, "structuredContent"> & {structuredContent?: Record<string, unknown>}>, env: DgidbDataEnv, toolName: string, extra: unknown): Promise<Result> {
  try {
    const value = await work();
    const original = {...value, structuredContent: value.structuredContent ?? {success: value.isError !== true}} as Result;
    const failed = original.isError === true || original.structuredContent.success === false;
    const result = {...original, ...(failed ? {isError: true} : {})};
    if (!shouldStage(bytes(result))) return result;
    if (!env.DGIDB_DATA_DO) throw new Error("Missing staging storage");
    const staged = await stageToDoAndRespond(result, env.DGIDB_DATA_DO, "dgidb", undefined,
      {toolName, serverName: "dgidb"}, "dgidb", extra as Parameters<typeof stageToDoAndRespond>[6]);
    const structuredContent = {success: !failed, staged: true, data: undefined,
      data_access_id: staged.dataAccessId, _staging: staged._staging, _meta: result.structuredContent._meta};
    if (bytes(structuredContent) > 100000) throw new Error("Staging metadata exceeds transport limit");
    return {content: [{type: "text", text: "Response staged. Use dgidb_query_data; payloads retains the complete original response."}],
      structuredContent, ...(failed ? {isError: true} : {})};
  } catch {
    return {content: [{type: "text", text: "Dgidb response could not be returned."}],
      structuredContent: {success: false, error: {code: "RESPONSE_ERROR", message: "Dgidb response could not be returned."}}, isError: true};
  }
}
