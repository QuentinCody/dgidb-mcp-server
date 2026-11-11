import {afterEach, expect, it, vi} from "vitest";
import {graphqlHandler, sqlHandler} from "./legacy-handlers";
import {datasetRegistry} from "./datasets";
import {toolResponse} from "./tool-response";

afterEach(() => {vi.unstubAllGlobals(); datasetRegistry.clear();});
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {status, headers: {"content-type": "application/json"}});

function stagingFixture(queryResult: unknown = {success: true, results: [{id: 1}], row_count: 1}, failStage = false) {
  const calls: Array<{id: string; path: string; body: Record<string, unknown> | undefined}> = [];
  // SAFETY: fixture implements exactly the idFromName/get/fetch surface used by these handlers.
  const namespace = {idFromName: (id: string) => id, get: (id: string) => ({fetch: async (input: Request | string, init?: RequestInit) => {
    const request = typeof input === "string" ? new Request(input, init) : input;
    const path = new URL(request.url).pathname;
    const body = request.method === "POST" ? await request.json() as Record<string, unknown> : undefined;
    calls.push({id, path, body});
    if (path === "/query-enhanced") return json(queryResult);
    if (path === "/process") return json(failStage ? {success: false, error: "fixture staging failure"} : {success: true, tables_created: ["records"], total_rows: 1});
    if (path === "/schema") return json({success: true, schema: {tables: {records: {columns: [{name: "id"}]}}}});
    return json({success: true});
  }})} as unknown as DurableObjectNamespace;
  return {namespace, calls};
}

it("returns both fields and a result hash for small native GraphQL success", async () => {
  const native = {data: {interactions: {nodes: [], pageInfo: {hasNextPage: false}}}, extensions: {fixture: true}};
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(json(native)));
  expect(await graphqlHandler({query: "{ interactions { nodes { id } } }"}, {})).toMatchObject({isError: false, content: expect.any(Array), structuredContent: {success: true, data: native, _meta: {citation: {result_hash: expect.stringMatching(/^[a-f0-9]{64}$/)}}}});
});

it.each([false, true])("preserves native GraphQL errors and marks partial=%s as an error", async partial => {
  const native = {data: partial ? {interactions: {nodes: [{id: "fixture"}]}} : null, errors: [{message: "field unavailable", path: ["interactions"], extensions: {code: "FIXTURE"}}]};
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(json(native)));
  const result = await graphqlHandler({query: "{ __typename }"}, {});
  expect(result).toMatchObject({isError: true, content: expect.any(Array), structuredContent: {success: false, partial, error_count: 1, data: native}});
  expect(result.structuredContent._meta).toEqual(partial ? {citation: expect.any(Object)} : {});
});

it.each(["introspection", "partial", "rejection", "duplicate-fields"])("stages the entire %s envelope above the UTF-8 threshold", async kind => {
  const size = kind === "duplicate-fields" ? 9000 : 16000;
  const native = kind === "introspection" ? {data: {__schema: {description: "α".repeat(16000)}}} : {
    data: kind === "partial" ? {interactions: {nodes: [{id: "edge"}]}} : null,
    errors: [{message: "α".repeat(size), path: ["interactions"], extensions: {code: "FIELD_ERROR"}}],
  };
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(json(native)));
  const {namespace, calls} = stagingFixture();
  const result = await graphqlHandler({query: "{ __schema { description } }"}, {DGIDB_DATA_DO: namespace}, {sessionId: "session-A"});
  expect(result).toMatchObject({isError: kind !== "introspection", content: expect.any(Array), structuredContent: {staged: true, data_access_id: expect.any(String), _staging: expect.any(Object)}});
  const payloads = calls.filter(call => call.path === "/process").map(call => typeof call.body?.data === "string" ? JSON.parse(call.body.data) : call.body?.data);
  expect(payloads).toContainEqual(native);
  expect(calls).toEqual(expect.arrayContaining([expect.objectContaining({path: "/register", body: expect.objectContaining({session_id: "session-A"})})]));
  expect(datasetRegistry.has(String(result.structuredContent.data_access_id))).toBe(true);
  expect(result.structuredContent._staging).toMatchObject({payload_size_bytes: new TextEncoder().encode(JSON.stringify(native)).byteLength});
  expect(new TextEncoder().encode(JSON.stringify(result.structuredContent)).byteLength).toBeLessThan(100000);
});

it("fails explicitly when a large response cannot be staged", async () => {
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async () => json({data: {text: "x".repeat(40000)}})));
  expect(await graphqlHandler({query: "{}"}, {})).toMatchObject({isError: true, structuredContent: {success: false}});
  const {namespace} = stagingFixture({}, true);
  expect(await graphqlHandler({query: "{}"}, {DGIDB_DATA_DO: namespace})).toMatchObject({isError: true, structuredContent: {success: false}});
  expect(datasetRegistry.size).toBe(0);
});

it("bounds an oversized metadata failure without returning an over-limit structured payload", async () => {
  const result = await toolResponse({tool: "dgidb_graphql_query", args: {}, result: {}, env: {DGIDB_DATA_DO: stagingFixture().namespace}, status: {success: false, error: {code: "ERROR", message: "α".repeat(60000)}}});
  expect(result).toMatchObject({isError: true, structuredContent: {error: {code: "RESPONSE_TOO_LARGE"}}});
  expect(new TextEncoder().encode(JSON.stringify(result.structuredContent)).byteLength).toBeLessThan(100000);
});

it("returns legacy SQL content plus structured success and preserves quality-analysis option", async () => {
  const {namespace, calls} = stagingFixture();
  const result = await sqlHandler({data_access_id: "old-uuid", sql: "SELECT 1", params: [], include_quality_analysis: true}, {DGIDB_DATA_DO: namespace});
  expect(result).toMatchObject({isError: false, content: expect.any(Array), structuredContent: {success: true, data: {results: [{id: 1}], quality_analysis_note: expect.any(String)}}});
  expect(calls).toEqual([{id: "old-uuid", path: "/query-enhanced", body: {sql: "SELECT 1"}}]);
});

it("stages large SQL results losslessly with the original result flags", async () => {
  const native = {success: true, results: [{id: 1, text: "α".repeat(18000)}], row_count: 1, count_capped: true, total_matching: 500};
  const {namespace, calls} = stagingFixture(native);
  const result = await sqlHandler({data_access_id: "old-uuid", sql: "SELECT * FROM records"}, {DGIDB_DATA_DO: namespace});
  expect(result).toMatchObject({isError: false, structuredContent: {staged: true, _staging: expect.any(Object)}});
  expect(calls.filter(call => call.path === "/process").map(call => call.body?.data)).toContainEqual(native);
});

it.each([{sql: "DELETE FROM records"}, {sql: "SELECT ?", params: ["value"]}])("rejects unsafe or unsupported SQL input before creating a DO %#", async args => {
  const {namespace, calls} = stagingFixture();
  expect(await sqlHandler({data_access_id: "id", ...args}, {DGIDB_DATA_DO: namespace})).toMatchObject({isError: true, structuredContent: {success: false}});
  expect(calls).toHaveLength(0);
});

it.each([null, [], {success: false, error: "α".repeat(60000)}])("preserves SQL failures as bounded both-field errors %#", async native => {
  const {namespace} = stagingFixture(native);
  const result = await sqlHandler({data_access_id: "id", sql: "SELECT 1"}, {DGIDB_DATA_DO: namespace});
  expect(result).toMatchObject({content: expect.any(Array), structuredContent: {success: false}, isError: true});
  expect(new TextEncoder().encode(JSON.stringify(result.structuredContent)).byteLength).toBeLessThan(100000);
});
