import {afterEach, expect, it, vi} from "vitest";
import worker, {DGIdbMCP} from "./index";
import {datasetRegistry, type DgidbEnv} from "./lib/datasets";
import type {DgidbToolResult} from "./lib/tool-response";

afterEach(() => {vi.unstubAllGlobals(); datasetRegistry.clear();});
const json = (value: unknown, status = 200) => new Response(JSON.stringify(value), {status, headers: {"content-type": "application/json"}});
async function registered(env: DgidbEnv = {}) {
  const instance = new DGIdbMCP(undefined, {...env, CODE_MODE_LOADER: {get: () => ({})}});
  await instance.init();
  // SAFETY: SDK registry access is test-only and exercises actual registered handlers.
  return (instance.server as unknown as {_registeredTools: Record<string, {handler: (args: unknown, extra: unknown) => Promise<DgidbToolResult>}>})._registeredTools;
}
function namespace(handler: (request: Request, id: string) => Promise<Response>) {
  // SAFETY: these are the three namespace methods used by the production helpers.
  return {idFromName: (id: string) => id, get: (id: string) => ({fetch: (input: Request | string, init?: RequestInit) => handler(typeof input === "string" ? new Request(input, init) : input, id)})} as unknown as DurableObjectNamespace;
}

it("registers every active DGIdb tool under both names", async () => {
  const tools = await registered();
  for (const suffix of ["graphql_query", "query_sql", "query_data", "get_schema", "execute", "search"]) {
    expect(tools[`dgidb_${suffix}`]).toBeDefined(); expect(tools[`mcp_dgidb_${suffix}`]).toBeDefined();
  }
  expect(Object.keys(tools)).toHaveLength(12);
});

it("returns both-field errors through both aliases of each legacy tool", async () => {
  vi.stubGlobal("fetch", vi.fn().mockRejectedValue(new Error("fixture network failure")));
  const tools = await registered();
  const inputs = {graphql_query: {query: "{}"}, query_sql: {data_access_id: "id", sql: "SELECT 1"}, query_data: {data_access_id: "id", sql: "SELECT 1"}, get_schema: {data_access_id: "id"}};
  for (const prefix of ["dgidb", "mcp_dgidb"]) for (const [suffix, args] of Object.entries(inputs)) {
    expect(await tools[`${prefix}_${suffix}`].handler(args, {})).toMatchObject({content: expect.any(Array), structuredContent: {success: false}, isError: true});
  }
});

it("returns native success, including an empty data object, through both GraphQL aliases", async () => {
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async () => json({data: {}})));
  const tools = await registered();
  for (const prefix of ["dgidb", "mcp_dgidb"]) expect(await tools[`${prefix}_graphql_query`].handler({query: "{}"}, {})).toMatchObject({isError: false, content: expect.any(Array), structuredContent: {success: true, data: {data: {}}, _meta: {citation: {result_hash: expect.any(String)}}}});
});

it("keeps workspace query and schema routing on both aliases", async () => {
  const calls: Array<{path: string; id: string}> = [];
  const ws = namespace(async (request, id) => {
    const path = new URL(request.url).pathname; calls.push({path, id});
    return json(path === "/ws/query" ? {success: true, rows: [{id: 1}], row_count: 1} : {success: true, dataset_count: 1, datasets: [{dataset: "dgidb"}]});
  });
  const tools = await registered({WORKSPACE_DO: ws});
  for (const prefix of ["dgidb", "mcp_dgidb"]) {
    expect(await tools[`${prefix}_query_data`].handler({workspace: "workspace-A", sql: "SELECT 1"}, {})).toMatchObject({content: expect.any(Array), structuredContent: {success: true}});
    expect(await tools[`${prefix}_get_schema`].handler({workspace: "workspace-A", dataset: "dgidb"}, {})).toMatchObject({content: expect.any(Array), structuredContent: {success: true}});
  }
  expect(calls).toEqual([{path: "/ws/query", id: "ws:workspace-A"}, {path: "/ws/schema", id: "ws:workspace-A"}, {path: "/ws/query", id: "ws:workspace-A"}, {path: "/ws/schema", id: "ws:workspace-A"}]);
});

it("retains dataset listing/deletion for newly staged passthrough responses", async () => {
  vi.stubGlobal("fetch", vi.fn().mockImplementation(async () => json({data: {text: "α".repeat(20000)}})));
  const deleted: string[] = [];
  const ns = namespace(async (request, id) => {
    const path = new URL(request.url).pathname;
    if (path === "/delete") deleted.push(id);
    if (path === "/process") return json({success: true, tables_created: ["records"], total_rows: 1});
    if (path === "/schema") return json({success: true, schema: {tables: {records: {columns: [{name: "text"}]}}}});
    return json({success: true});
  });
  const tools = await registered({DGIDB_DATA_DO: ns});
  const result = await tools.dgidb_graphql_query.handler({query: "{ __typename }"}, {});
  const id = result.structuredContent.data_access_id;
  expect(typeof id).toBe("string");
  // SAFETY: route uses only the supplied namespace; no execution context methods are invoked.
  const env = {DGIDB_DATA_DO: ns, MCP_OBJECT: ns, ADMIN_TOKEN: "fixture-admin"} as unknown as Env, ctx = {} as ExecutionContext;
  const headers = {authorization: "Bearer fixture-admin"};
  expect(await (await worker.fetch(new Request("https://fixture/datasets", {headers}), env, ctx)).json()).toMatchObject({datasets: [{data_access_id: id, table_count: 1, total_rows: 1}]});
  expect((await worker.fetch(new Request(`https://fixture/datasets/${id}`, {method: "DELETE", headers}), env, ctx)).status).toBe(200);
  expect(deleted).toEqual([id]); expect(datasetRegistry.has(String(id))).toBe(false);
  expect((await worker.fetch(new Request(`https://fixture/datasets/${id}`, {method: "DELETE", headers}), env, ctx)).status).toBe(404);
});

it("keeps a dataset registered if its deletion fails", async () => {
  datasetRegistry.set("existing", {created: "fixture"});
  const ns = namespace(async () => json({success: false}, 500));
  // SAFETY: these are the only environment/context members this route consumes.
  const result = await worker.fetch(new Request("https://fixture/datasets/existing", {method: "DELETE", headers: {authorization: "Bearer fixture-admin"}}), {DGIDB_DATA_DO: ns, MCP_OBJECT: ns, ADMIN_TOKEN: "fixture-admin"} as unknown as Env, {} as ExecutionContext);
  expect(result.status).toBe(500); expect(datasetRegistry.has("existing")).toBe(true);
});

it("refuses unauthenticated dataset listing and deletion", async () => {
  datasetRegistry.set("victim", {created: "fixture"});
  const deleted: string[] = [];
  const ns = namespace(async (_request, id) => {deleted.push(id); return json({success: true});});
  // SAFETY: these are the only environment/context members this route consumes.
  const ctx = {} as ExecutionContext, open = {DGIDB_DATA_DO: ns, MCP_OBJECT: ns} as unknown as Env, locked = {DGIDB_DATA_DO: ns, MCP_OBJECT: ns, ADMIN_TOKEN: "fixture-admin"} as unknown as Env;
  const list = (env: Env, headers?: HeadersInit) => worker.fetch(new Request("https://fixture/datasets", {headers}), env, ctx);
  const del = (env: Env, headers?: HeadersInit) => worker.fetch(new Request("https://fixture/datasets/victim", {method: "DELETE", headers}), env, ctx);
  const listed = await list(open);
  expect(listed.status).toBe(503); expect(JSON.stringify(await listed.json())).not.toContain("victim");
  expect((await del(open)).status).toBe(503);
  expect((await list(locked)).status).toBe(403);
  expect((await list(locked, {authorization: "Bearer wrong"})).status).toBe(403);
  expect((await del(locked)).status).toBe(403);
  expect((await del(locked, {authorization: "Bearer fixture-admin-x"})).status).toBe(403);
  expect(deleted).toEqual([]); expect(datasetRegistry.has("victim")).toBe(true);
});
