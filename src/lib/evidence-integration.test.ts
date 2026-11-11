import {expect, it} from "vitest";
import {buildGraphqlProxySource} from "@bio-mcp/shared/codemode/graphql-proxy";
import {createGraphqlProxyTool} from "@bio-mcp/shared/tools/graphql-proxy";
import {createStageProxyTool} from "@bio-mcp/shared/tools/api-proxy";
import {DGIDB_EVIDENCE_PREAMBLE} from "./evidence-preamble";

const json = (value: unknown) => new Response(JSON.stringify(value));
const context = {sql: () => []};
it.each([16000, 110000])("preserves source-linked claims and partial errors through real shared staging (%i chars)", async size => {
  const native = {interactions: {pageInfo: {hasNextPage: true, endCursor: "next+/="}, nodes: [{id: "edge", interactionScore: 1.25, evidenceScore: 2, interactionClaims: [
    {id: "claim-A", source: {sourceDbName: "A", sourceDbVersion: "2026-01"}, publications: [{pmid: 1, citation: "α".repeat(size)}], interactionClaimTypes: [{type: "inhibitor"}]},
    {id: "claim-B", source: null, publications: [{pmid: 1}], interactionClaimTypes: [{type: "activator"}]},
  ]}]}};
  const errors = [{message: "partial evidence", path: ["interactions", "nodes", 0, "sources"]}];
  const stored: unknown[] = [];
  const namespace = {idFromName: (name: string) => name, get: () => ({fetch: async (request: Request) => {
    if (new URL(request.url).pathname === "/process") {
      // SAFETY: the real staging helper creates this documented request envelope.
      const payload = await request.json() as {data: unknown}; stored.push(payload.data);
      return json({success: true, tables_created: ["evidence"], total_rows: 1});
    }
    return json({success: true, schema: {tables: {evidence: {columns: [{name: "id"}]}}}});
  }})};
  const proxy = createGraphqlProxyTool({stagingPrefix: "dgidb", doNamespace: namespace, stagingThreshold: 30 * 1024, gqlFetch: async (_query, variables) => {
    expect(variables).toMatchObject({genes: ["BRAF"], first: 10, after: null});
    return {data: native, errors};
  }});
  const stage = createStageProxyTool({stagingPrefix: "dgidb", doNamespace: namespace});
  const result = await new Function("codemode", `var __stagedResults=[]; ${buildGraphqlProxySource()} ${DGIDB_EVIDENCE_PREAMBLE}; return dgidb.evidence({genes:["BRAF"]});`)({
    __graphql_proxy: (input: Record<string, unknown>) => proxy.handler(input, context),
    __stage_proxy: (input: Record<string, unknown>) => stage.handler(input, context),
  });
  expect(result).toMatchObject({__staged: true, data_access_id: expect.stringMatching(/^dgidb_/), _staging: expect.any(Object)});
  const payloads = stored.map(value => typeof value === "string" ? JSON.parse(value) : value);
  const retained = payloads.some(value => JSON.stringify(value) === JSON.stringify({...native, __errors: errors}) || JSON.stringify(value?.native) === JSON.stringify({...native, __errors: errors}));
  expect(retained).toBe(true);
  expect(new TextEncoder().encode(JSON.stringify(result)).byteLength).toBeLessThan(100000);
});
it("propagates pure GraphQL errors without inventing an empty interaction list", async () => {
  const proxy = createGraphqlProxyTool({stagingPrefix: "dgidb", gqlFetch: async () => ({errors: [{message: "unsupported field"}]})});
  const result = new Function("codemode", `var __stagedResults=[]; ${buildGraphqlProxySource()} ${DGIDB_EVIDENCE_PREAMBLE}; return dgidb.evidence({genes:["BRAF"]});`)({
    __graphql_proxy: (input: Record<string, unknown>) => proxy.handler(input, context),
  });
  await expect(result).rejects.toThrow("unsupported field");
});
