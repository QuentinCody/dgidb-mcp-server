import {buildPassthroughCitation} from "@bio-mcp/shared";
import {shouldStage} from "@bio-mcp/shared/staging/utils";
import {stageResult, type DgidbEnv} from "./datasets";

export const DGIDB_SOURCE = {id: "dgidb", name: "DGIdb", url: "https://www.dgidb.org"};
const byteSize = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).byteLength;

export interface DgidbToolResult extends Record<string, unknown> {
  content: Array<{type: "text"; text: string}>;
  structuredContent: Record<string, unknown>;
  isError: boolean;
}

export function errorResult(code: string, message: string): DgidbToolResult {
  const bounded = message.slice(0, 2000);
  return {content: [{type: "text" as const, text: bounded}],
    structuredContent: {success: false, error: {code, message: bounded}}, isError: true as const};
}

interface ResultInput {
  tool: string;
  args: Record<string, unknown>;
  result: Record<string, unknown>;
  env: DgidbEnv;
  extra?: Record<string, unknown>;
  status?: {success: boolean; partial?: boolean; error?: {code: string; message: string}; error_count?: number};
  cite?: boolean;
}

async function citation(input: ResultInput, dataAccessId?: string) {
  if (input.cite === false) return {};
  return buildPassthroughCitation({source: DGIDB_SOURCE, server: "dgidb", tool: input.tool,
    query: input.args, result: input.result, dataAccessId});
}

/** Preserve the full native envelope; metadata may never evade the byte limits. */
export async function toolResponse(input: ResultInput): Promise<DgidbToolResult> {
  const status = input.status ?? {success: true};
  let structuredContent: Record<string, unknown> = {...status, data: input.result, _meta: await citation(input)};
  let text = JSON.stringify(input.result);
  if (shouldStage(byteSize({content: [{type: "text", text}], structuredContent}))) {
    const staged = await stageResult(input.result, input.env, {toolName: input.tool, args: input.args}, input.extra);
    structuredContent = {...status, ...staged, _meta: await citation(input, staged.data_access_id)};
    text = JSON.stringify(staged);
  }
  if (byteSize(structuredContent) > 100000) return errorResult("RESPONSE_TOO_LARGE", "DGIdb response metadata exceeds the transport limit; narrow the query");
  return {content: [{type: "text" as const, text}], structuredContent, isError: !status.success};
}
