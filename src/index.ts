import { McpAgent } from "agents/mcp";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { JsonToSqlDO } from "./do.js";
import { DataQualityManager } from "./lib/DataQualityManager.js";

// ========================================
// API CONFIGURATION - Customize for your GraphQL API
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
			description: "Executes GraphQL queries against DGIdb API, processes responses into SQLite tables, and returns metadata for subsequent SQL querying. Returns a data_access_id and schema information."
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
// ENVIRONMENT INTERFACE
// ========================================
interface DGIdbEnv {
	MCP_HOST?: string;
	MCP_PORT?: string;
	JSON_TO_SQL_DO: DurableObjectNamespace;
}

// ========================================
// CORE MCP SERVER CLASS - Reusable template
// ========================================

export class DGIdbMCP extends McpAgent {
	server = new McpServer({
		name: API_CONFIG.name,
		version: API_CONFIG.version,
		description: API_CONFIG.description
	});
	
	private qualityManager = new DataQualityManager();

	async init() {
		// Tool #1: GraphQL to SQLite staging
		this.server.tool(
			API_CONFIG.tools.graphql.name,
			API_CONFIG.tools.graphql.description,
			{
				query: z.string().describe("GraphQL query string"),
				variables: z.record(z.any()).optional().describe("Optional variables for the GraphQL query"),
			},
			async ({ query, variables }) => {
				try {
					const graphqlResult = await this.executeGraphQLQuery(query, variables);

					if (this.shouldBypassStaging(graphqlResult, query)) {
						return {
							content: [{
								type: "text" as const,
								text: JSON.stringify(graphqlResult, null, 2)
							}]
						};
					}

					const stagingResult = await this.stageDataInDurableObject(graphqlResult);
					return {
						content: [{
							type: "text" as const,
							text: JSON.stringify(stagingResult, null, 2)
						}]
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
				include_quality_analysis: z.boolean().optional().describe("Include data quality analysis in response"),
			},
			async ({ data_access_id, sql, include_quality_analysis = false }) => {
				try {
					const queryResult = await this.executeSQLQuery(data_access_id, sql, include_quality_analysis);
					return { content: [{ type: "text" as const, text: JSON.stringify(queryResult, null, 2) }] };
				} catch (error) {
					return this.createErrorResponse("SQL execution failed", error);
				}
			}
		);
	}

	// ========================================
	// GRAPHQL CLIENT - Customize headers/auth as needed
	// ========================================
	private async executeGraphQLQuery(query: string, variables?: Record<string, any>): Promise<any> {
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
		
		return await response.json();
	}

	private isIntrospectionQuery(query: string): boolean {
		if (!query) return false;
		
		// Remove comments and normalize whitespace for analysis
		const normalizedQuery = query
			.replace(/\s*#.*$/gm, '') // Remove comments
			.replace(/\s+/g, ' ')     // Normalize whitespace
			.trim()
			.toLowerCase();
		
		// Check for common introspection patterns
		const introspectionPatterns = [
			'__schema',           // Schema introspection
			'__type',            // Type introspection
			'__typename',        // Typename introspection
			'introspectionquery', // Named introspection queries
			'getintrospectionquery'
		];
		
		return introspectionPatterns.some(pattern => 
			normalizedQuery.includes(pattern)
		);
	}

	private shouldBypassStaging(result: any, originalQuery?: string): boolean {
		if (!result) return true;

		// Bypass if this was an introspection query
		if (originalQuery && this.isIntrospectionQuery(originalQuery)) {
			return true;
		}

		// Bypass if GraphQL reported errors
		if (result.errors) {
			return true;
		}

		// Check if response contains introspection-like data structure
		if (result.data) {
			// Common introspection response patterns
			if (result.data.__schema || result.data.__type) {
				return true;
			}
			
			// Check for schema metadata structures
			const hasSchemaMetadata = Object.values(result.data).some((value: any) => {
				if (value && typeof value === 'object') {
					// Look for typical schema introspection fields
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

		// Rough size check to avoid storing very small payloads
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
	// DURABLE OBJECT INTEGRATION - Use this.env directly
	// ========================================
	private async stageDataInDurableObject(graphqlResult: any): Promise<any> {
		const env = this.env as DGIdbEnv;
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
		
		const processingResult = await response.json() as any;
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

	private async executeSQLQuery(dataAccessId: string, sql: string, includeQualityAnalysis: boolean = false): Promise<any> {
		const env = this.env as DGIdbEnv;
		if (!env?.JSON_TO_SQL_DO) {
			throw new Error("JSON_TO_SQL_DO binding not available");
		}
		
		const doId = env.JSON_TO_SQL_DO.idFromName(dataAccessId);
		const stub = env.JSON_TO_SQL_DO.get(doId);
		
		// Analyze query complexity for performance warnings
		const performanceMetrics = this.qualityManager.analyzeQueryComplexity(sql);
		
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
		
		const queryResult = await response.json() as any;
		
		// Add performance metrics to result
		if (performanceMetrics.resourceWarnings.length > 0) {
			queryResult.performance_warnings = performanceMetrics.resourceWarnings;
			queryResult.complexity_score = performanceMetrics.queryComplexityScore;
		}
		
		// Include comprehensive quality analysis if requested
		if (includeQualityAnalysis) {
			try {
				// Get schema information for quality analysis
				const schemasResponse = await stub.fetch("http://do/schemas");
				if (schemasResponse.ok) {
					const schemasData = await schemasResponse.json() as any;
					const qualityReport = await this.qualityManager.generateQualityReport(
						dataAccessId, 
						schemasData.schemas, 
						{ exec: (sql: string) => ({ one: () => null, changes: 0 }) } // Mock SQL interface for analysis
					);
					queryResult.data_quality_report = qualityReport;
				}
			} catch (error) {
				queryResult.quality_analysis_error = `Could not generate quality report: ${error}`;
			}
		}
		
		return queryResult;
	}

	private async deleteDataset(dataAccessId: string): Promise<boolean> {
		const env = this.env as DGIdbEnv;
		if (!env?.JSON_TO_SQL_DO) {
			throw new Error("JSON_TO_SQL_DO binding not available");
		}

		const doId = env.JSON_TO_SQL_DO.idFromName(dataAccessId);
		const stub = env.JSON_TO_SQL_DO.get(doId);

		const response = await stub.fetch("http://do/delete", { method: 'DELETE' });

		return response.ok;
	}

	// ========================================
	// ERROR HANDLING - Reusable
	// ========================================
	private createErrorResponse(message: string, error: unknown) {
		return {
			content: [{
				type: "text" as const,
				text: JSON.stringify({
					success: false,
					error: message,
					details: error instanceof Error ? error.message : String(error)
				}, null, 2)
			}]
		};
	}
}

// ========================================
// CLOUDFLARE WORKERS BOILERPLATE - Simplified
// ========================================
interface Env {
	MCP_HOST?: string;
	MCP_PORT?: string;
	JSON_TO_SQL_DO: DurableObjectNamespace;
}

interface ExecutionContext {
	waitUntil(promise: Promise<any>): void;
	passThroughOnException(): void;
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		if (url.pathname === "/sse" || url.pathname.startsWith("/sse/")) {
			// @ts-ignore - SSE transport handling
			return DGIdbMCP.serveSSE("/sse").fetch(request, env, ctx);
		}

		if (url.pathname === "/datasets" && request.method === "GET") {
			const list = Array.from(datasetRegistry.entries()).map(([id, info]) => ({
				data_access_id: id,
				...info
			}));
			return new Response(JSON.stringify({ datasets: list }, null, 2), {
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

		// Schema initialization endpoint
		if (url.pathname === "/initialize-schema" && request.method === "POST") {
			const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
			const stub = env.JSON_TO_SQL_DO.get(globalDoId);
			const resp = await stub.fetch("http://do/initialize-schema", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: await request.text()
			});
			return new Response(await resp.text(), {
				status: resp.status,
				headers: { "Content-Type": "application/json" }
			});
		}

		// Chunking stats endpoint
		if (url.pathname === "/chunking-stats" && request.method === "GET") {
			const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
			const stub = env.JSON_TO_SQL_DO.get(globalDoId);
			const resp = await stub.fetch("http://do/chunking-stats");
			return new Response(await resp.text(), {
				status: resp.status,
				headers: { "Content-Type": "application/json" }
			});
		}

		// Chunking analysis endpoint
		if (url.pathname === "/chunking-analysis" && request.method === "GET") {
			const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
			const stub = env.JSON_TO_SQL_DO.get(globalDoId);
			const resp = await stub.fetch("http://do/chunking-analysis");
			return new Response(await resp.text(), {
				status: resp.status,
				headers: { "Content-Type": "application/json" }
			});
		}

		return new Response(
			`${API_CONFIG.name} - Available on /sse endpoint`,
			{ status: 404, headers: { "Content-Type": "text/plain" } }
		);
	},
};

export { DGIdbMCP as MyMCP };
export { JsonToSqlDO };