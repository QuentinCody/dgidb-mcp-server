import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { JsonToSqlDO } from "./do.js";
// DGIdb: Drug-Gene Interaction Database
// Uses GraphQL API at dgidb.org/api/graphql
// Migrated to shared RestStagingDO infrastructure (March 2026)

// ========================================
// TYPES
// ========================================

interface GraphQLResponse {
	data?: Record<string, unknown>;
	errors?: Array<{ message: string; [key: string]: unknown }>;
}

interface StagingProcessResult {
	table_count?: number;
	total_rows?: number;
}

interface StagingResponse {
	data_access_id: string;
	processing_details: StagingProcessResult;
}

interface SQLQueryResult {
	quality_analysis_note?: string;
	[key: string]: unknown;
}

// ========================================
// API CONFIGURATION
// ========================================
const API_CONFIG = {
	name: "DGIdbExplorer",
	version: "0.1.0",
	description: "MCP Server for querying GraphQL APIs and converting responses to queryable SQLite tables",

	// GraphQL API settings
	endpoint: 'https://dgidb.org/api/graphql',
	headers: {
		"User-Agent": "MCPDGIdbServer/0.1.0"
	},

	// Tool names and descriptions
	tools: {
		graphql: {
			name: "dgidb_graphql_query",
			description: `Executes GraphQL queries against the DGIdb (Drug-Gene Interaction Database) API. Responses are staged in SQLite tables.

IMPORTANT: Gene.interactions is a plain list (NOT a Relay connection). Do NOT use pagination args (first/after) or .nodes on interactions.
• Correct: { genes(names: ["EGFR"]) { nodes { interactions { interactionScore drug { name approved } } } } }
• Wrong: interactions(first: 10) { nodes { ... } }

Top-level queries (genes, drugs) use Relay connections with nodes/edges, but nested fields like interactions and interactionTypes are plain arrays.`
		},
		sql: {
			name: "dgidb_query_sql",
			description: "Execute read-only SQL queries against staged data. Use the data_access_id from dgidb_graphql_query to query the SQLite tables."
		}
	}
};

// In-memory registry of staged datasets
const datasetRegistry = new Map<string, { created: string; table_count?: number; total_rows?: number }>();

// ========================================
// CORE MCP SERVER CLASS
// ========================================

export class DGIdbMCP extends McpAgent {
	server = new McpServer({
		name: API_CONFIG.name,
		version: API_CONFIG.version,
	});

	async init() {
		// Tool #1: GraphQL to SQLite staging
		this.server.tool(
			API_CONFIG.tools.graphql.name,
			API_CONFIG.tools.graphql.description,
			{
				query: z.string().describe("GraphQL query string"),
				variables: z.record(z.string(), z.any()).optional().describe("Optional variables for the GraphQL query")
			},
			async ({ query, variables }) => {
				try {
					const graphqlResult = await this.executeGraphQLQuery(query, variables);

					if (this.shouldBypassStaging(graphqlResult, query)) {
						return {
							content: [{
								type: "text" as const,
								text: JSON.stringify(graphqlResult)
							}],
							isError: false,
						};
					}

					const stagingResult = await this.stageDataInDurableObject(graphqlResult);
					return {
						content: [{
							type: "text" as const,
							text: JSON.stringify(stagingResult)
						}],
						isError: false,
					};

				} catch (error) {
					return this.createErrorResponse("GraphQL execution failed", error);
				}
			}
		);

		// Tool #2: SQL querying against staged data
		this.server.tool(
			API_CONFIG.tools.sql.name,
			API_CONFIG.tools.sql.description,
			{
				data_access_id: z.string().describe("Data access ID from the GraphQL query tool"),
				sql: z.string().describe("SQL SELECT query to execute"),
				params: z.array(z.string()).optional().describe("Optional query parameters"),
				include_quality_analysis: z.boolean().optional().describe("Include data quality analysis in response")
			},
			async ({ data_access_id, sql, include_quality_analysis = false }) => {
				try {
					const queryResult = await this.executeSQLQuery(data_access_id, sql, include_quality_analysis);
					return {
						content: [{
							type: "text" as const,
							text: JSON.stringify(queryResult)
						}],
						isError: false,
					};
				} catch (error) {
					return this.createErrorResponse("SQL execution failed", error);
				}
			}
		);
	}

	// ========================================
	// GRAPHQL CLIENT
	// ========================================
	private async executeGraphQLQuery(query: string, variables?: Record<string, unknown>): Promise<GraphQLResponse> {
		const headers = {
			"Content-Type": "application/json",
			...API_CONFIG.headers
		};

		const body = { query, ...(variables && { variables }) };

		const response = await fetch(API_CONFIG.endpoint, {
			method: 'POST',
			headers,
			body: JSON.stringify(body),
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`HTTP ${response.status}: ${errorText}`);
		}

		return response.json() as Promise<GraphQLResponse>;
	}

	private isIntrospectionQuery(query: string): boolean {
		if (!query) return false;

		const normalizedQuery = query
			.replace(/\s*#.*$/gm, '')
			.replace(/\s+/g, ' ')
			.trim()
			.toLowerCase();

		const introspectionPatterns = [
			'__schema',
			'__type',
			'__typename',
			'introspectionquery',
			'getintrospectionquery'
		];

		return introspectionPatterns.some(pattern =>
			normalizedQuery.includes(pattern)
		);
	}

	private shouldBypassStaging(result: GraphQLResponse, originalQuery?: string): boolean {
		if (!result) return true;

		if (originalQuery && this.isIntrospectionQuery(originalQuery)) {
			return true;
		}

		if (result.errors) {
			return true;
		}

		if (result.data) {
			if (result.data.__schema || result.data.__type) {
				return true;
			}

			const hasSchemaMetadata = Object.values(result.data).some((value: unknown) => {
				if (value && typeof value === 'object') {
					const keys = Object.keys(value);
					const schemaFields = ['types', 'queryType', 'mutationType', 'subscriptionType', 'directives'];
					const typeFields = ['name', 'kind', 'description', 'fields', 'interfaces', 'possibleTypes', 'enumValues', 'inputFields'];

					return schemaFields.some(field => keys.includes(field)) ||
						   typeFields.filter(field => keys.includes(field)).length >= 2;
				}
				return false;
			});

			if (hasSchemaMetadata) {
				return true;
			}
		}

		// Skip staging for small responses (<1.5KB) — not worth the DO overhead
		try {
			if (JSON.stringify(result).length < 1500) {
				return true;
			}
		} catch {
			return true;
		}

		// Detect mostly empty data objects
		if (result.data) {
			const values = Object.values(result.data);
			const hasContent = values.some((v) => {
				if (v === null || v === undefined) return false;
				if (Array.isArray(v)) return v.length > 0;
				if (typeof v === "object") return Object.keys(v).length > 0;
				return true;
			});
			if (!hasContent) return true;
		}

		return false;
	}

	// ========================================
	// DURABLE OBJECT INTEGRATION
	// ========================================
	private async stageDataInDurableObject(graphqlResult: GraphQLResponse): Promise<StagingResponse> {
		const env = this.env as unknown as Cloudflare.Env;
		if (!env?.JSON_TO_SQL_DO) {
			throw new Error("JSON_TO_SQL_DO binding not available");
		}

		const accessId = crypto.randomUUID();
		const doId = env.JSON_TO_SQL_DO.idFromName(accessId);
		const stub = env.JSON_TO_SQL_DO.get(doId);

		const response = await stub.fetch("http://do/process", {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(graphqlResult)
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`DO staging failed: ${errorText}`);
		}

		const processingResult = await response.json() as StagingProcessResult;
		datasetRegistry.set(accessId, {
			created: new Date().toISOString(),
			table_count: processingResult.table_count,
			total_rows: processingResult.total_rows
		});
		return {
			data_access_id: accessId,
			processing_details: processingResult
		};
	}

	private async executeSQLQuery(dataAccessId: string, sql: string, includeQualityAnalysis: boolean = false): Promise<SQLQueryResult> {
		const env = this.env as unknown as Cloudflare.Env;
		if (!env?.JSON_TO_SQL_DO) {
			throw new Error("JSON_TO_SQL_DO binding not available");
		}

		const doId = env.JSON_TO_SQL_DO.idFromName(dataAccessId);
		const stub = env.JSON_TO_SQL_DO.get(doId);

		// Use enhanced SQL execution that automatically resolves chunked content
		const response = await stub.fetch("http://do/query-enhanced", {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ sql })
		});

		if (!response.ok) {
			const errorText = await response.text();
			throw new Error(`SQL execution failed: ${errorText}`);
		}

		const queryResult = await response.json() as SQLQueryResult;

		if (includeQualityAnalysis) {
			queryResult.quality_analysis_note = "Quality analysis feature available - contact for details";
		}

		return queryResult;
	}

	// ========================================
	// ERROR HANDLING
	// ========================================
	private createErrorResponse(message: string, error: unknown) {
		const errorDetails = error instanceof Error ? error.message : String(error);
		const timestamp = new Date().toISOString();

		return {
			content: [{
				type: "text" as const,
				text: JSON.stringify({
					success: false,
					error: message,
					details: errorDetails,
					timestamp,
					help: "Check the DGIdb API documentation for valid query formats and parameters"
				})
			}],
			isError: true,
			_meta: {
				progress: 0.0,
				statusMessage: message,
				error_code: "EXECUTION_FAILED",
				error_category: error instanceof Error && error.name ? error.name : "UnknownError",
				timestamp,
				recovery_suggestions: [
					"Verify your GraphQL query syntax",
					"Check that all required variables are provided",
					"Ensure the DGIdb API is accessible"
				]
			}
		};
	}
}

// ========================================
// CLOUDFLARE WORKERS FETCH HANDLER
// ========================================

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// Handle standard MCP requests via Streamable HTTP
		if (url.pathname.startsWith("/mcp")) {
			return DGIdbMCP.serve("/mcp", { binding: "MCP_OBJECT" }).fetch(request, env, ctx);
		}

		// Dataset management endpoints
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

			const doId = env.JSON_TO_SQL_DO.idFromName(id);
			const stub = env.JSON_TO_SQL_DO.get(doId);
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
export { JsonToSqlDO };
