import {buildHealthResponse, configureCitationSigning} from "@bio-mcp/shared";
import {StatelessMcpWorker, McpServer} from "@bio-mcp/shared/mcp";
import {DgidbDataDO} from "./do.js";
import {registerCodeMode} from "./tools/code-mode.js";
import {registerGraphql, registerSql} from "./tools/legacy.js";
import {registerGetSchema, registerQueryData} from "./tools/data.js";
import {datasetRegistry, type DgidbEnv} from "./lib/datasets.js";

const API_CONFIG = {name: "DGIdbExplorer", version: "0.1.0"};

function adminJson(body: unknown, status: number): Response {
	return new Response(JSON.stringify(body), {status, headers: {"Content-Type": "application/json"}});
}

function constantTimeEqual(left: string, right: string): boolean {
	const a = new TextEncoder().encode(left), b = new TextEncoder().encode(right);
	let mismatch = a.length ^ b.length;
	for (let i = 0; i < Math.max(a.length, b.length); i++) mismatch |= (a[i] ?? 0) ^ (b[i] ?? 0);
	return mismatch === 0;
}

/**
 * The /datasets routes sit outside MCP session scoping and a data_access_id is
 * the only capability for its rows, so they are admin-only. Missing ADMIN_TOKEN
 * disables them (503); a missing or wrong Bearer token is 403.
 */
function datasetsAuthError(request: Request, env: Env): Response | null {
	// ADMIN_TOKEN is an optional secret (`wrangler secret put ADMIN_TOKEN`), not in the generated Env.
	const token: unknown = Reflect.get(env, "ADMIN_TOKEN");
	if (typeof token !== "string" || !token) return adminJson({error: "admin disabled", reason: "ADMIN_TOKEN secret not set"}, 503);
	const auth = request.headers.get("authorization") ?? "";
	if (!auth.startsWith("Bearer ") || !constantTimeEqual(auth.slice(7), token)) return adminJson({error: "forbidden"}, 403);
	return null;
}

export class DGIdbMCP extends StatelessMcpWorker {
  server = new McpServer(API_CONFIG);

  async init() {
    configureCitationSigning(this.env);
    // SAFETY: deployment bindings are optional here so missing bindings produce tool errors.
    const env = this.env as unknown as DgidbEnv;
    registerGraphql(this.server, env);
    registerSql(this.server, env);
    registerQueryData(this.server, env);
    registerGetSchema(this.server, env);
    if (env.CODE_MODE_LOADER) registerCodeMode(this.server, {...env, CODE_MODE_LOADER: env.CODE_MODE_LOADER});
  }
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// Health check endpoint
		if (url.pathname === "/health") {
			return buildHealthResponse("dgidb");
		}

		if (url.pathname === "/readyz") {
			// Deep check: builds the MCP server the way a real request does, so a
			// factory that throws is a 503 here instead of a green /health over a
			// server that 500s every MCP call.
			return DGIdbMCP.readiness(env, "dgidb");
		}

		// Handle standard MCP requests via Streamable HTTP
		if (url.pathname.startsWith("/mcp")) {
			return DGIdbMCP.serve("/mcp").fetch(request, env, ctx);
		}

		// Dataset management endpoints (admin-only; see datasetsAuthError)
		const isDatasetsRoute = (url.pathname === "/datasets" && request.method === "GET")
			|| (url.pathname.startsWith("/datasets/") && request.method === "DELETE");
		if (isDatasetsRoute) {
			const denied = datasetsAuthError(request, env);
			if (denied) return denied;
		}

		if (url.pathname === "/datasets" && request.method === "GET") {
			const list = Array.from(datasetRegistry.entries()).map(([id, info]) => ({
				data_access_id: id,
				...info
			}));
			return new Response(JSON.stringify({ datasets: list }), {
				headers: { "Content-Type": "application/json" }
			});
		}

		if (url.pathname.startsWith("/datasets/") && request.method === "DELETE") {
			const id = url.pathname.split("/")[2];
			if (!id || !datasetRegistry.has(id)) {
				return new Response(JSON.stringify({ error: "Dataset not found" }), {
					status: 404,
					headers: { "Content-Type": "application/json" }
				});
			}

			const doId = env.DGIDB_DATA_DO.idFromName(id);
			const stub = env.DGIDB_DATA_DO.get(doId);
			const resp = await stub.fetch("http://do/delete", { method: "DELETE" });
			if (resp.ok) {
				datasetRegistry.delete(id);
				return new Response(JSON.stringify({ success: true }), {
					headers: { "Content-Type": "application/json" }
				});
			}

			const text = await resp.text();
			return new Response(JSON.stringify({ success: false, error: text }), {
				status: 500,
				headers: { "Content-Type": "application/json" }
			});
		}

		return new Response(
			`${API_CONFIG.name} - Available on /mcp endpoint`,
			{ status: 404, headers: { "Content-Type": "text/plain" } }
		);
	},
};

export { DGIdbMCP as MyMCP };
export { DgidbDataDO };
// Retain the old export during the class-rename rollout; no new namespace is created.
export { DgidbDataDO as JsonToSqlDO };
