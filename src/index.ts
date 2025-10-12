import { z } from "zod";
import { JsonToSqlDO } from "./do";
import { DataQualityManager } from "./lib/DataQualityManager";

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
	// Note: title fields added for MCP 2025-06-18 spec support (human-friendly display names)
	tools: {
		graphql: {
			name: "dgidb_graphql_query",
			title: "DGIdb GraphQL Query",
			description: "Executes GraphQL queries against DGIdb API, processes responses into SQLite tables, and returns metadata for subsequent SQL querying. Returns a data_access_id and schema information."
		},
		sql: {
			name: "dgidb_query_sql",
			title: "SQL Query Executor", 
			description: "Execute read-only SQL queries against staged data. Use the data_access_id from dgidb_graphql_query to query the SQLite tables."
		}
	}
};

// In-memory registry of staged datasets
const datasetRegistry = new Map<string, { created: string; table_count?: number; total_rows?: number }>();

const textEncoder = new TextEncoder();

const MCP_TOOL_DEFINITIONS = [
	{
		name: API_CONFIG.tools.graphql.name,
		description: API_CONFIG.tools.graphql.description,
		inputSchema: {
			type: "object",
			properties: {
				query: { type: "string", description: "GraphQL query string" },
				variables: { type: "object", description: "Optional variables for the GraphQL query" }
			},
			required: ["query"]
		}
	},
	{
		name: API_CONFIG.tools.sql.name,
		description: API_CONFIG.tools.sql.description,
		inputSchema: {
			type: "object",
			properties: {
				data_access_id: { type: "string", description: "Data access ID from the GraphQL query tool" },
				sql: { type: "string", description: "SQL SELECT query to execute" },
				params: { type: "array", items: { type: "string" }, description: "Optional query parameters" },
				include_quality_analysis: { type: "boolean", description: "Include data quality analysis in response" }
			},
			required: ["data_access_id", "sql"]
		}
	}
] as const;

function acceptsMime(acceptHeader: string | null, mimeType: string): boolean {
	if (!acceptHeader) {
		return true;
	}
	const header = acceptHeader.toLowerCase();
	return header.includes(mimeType) || header.includes("*/*");
}

function createJsonResponse(payload: unknown, status: number, baseHeaders: Record<string, string>) {
	const headers = new Headers(baseHeaders);
	headers.set("Content-Type", "application/json");
	return new Response(JSON.stringify(payload, null, 2), {
		status,
		headers
	});
}

function createAcceptedResponse(baseHeaders: Record<string, string>) {
	const headers = new Headers(baseHeaders);
	headers.delete("Content-Type");
	return new Response(null, {
		status: 202,
		headers
	});
}

function buildJsonRpcError(id: string | number | null, code: number, message: string, data?: unknown) {
	const errorBody: any = {
		jsonrpc: "2.0",
		id,
		error: {
			code,
			message
		}
	};
	if (data !== undefined) {
		errorBody.error.data = data;
	}
	return errorBody;
}

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

export class DGIdbMCP {
	private qualityManager = new DataQualityManager();
	private env: DGIdbEnv;

	constructor(env: DGIdbEnv) {
		this.env = env;
	}

	public async handleGraphQLTool(args: any) {
		const { query, variables } = args;
		try {
			const graphqlResult = await this.executeGraphQLQuery(query, variables);

			if (this.shouldBypassStaging(graphqlResult, query)) {
				return {
					content: [{
						type: "text" as const,
						text: JSON.stringify(graphqlResult, null, 2)
					}],
					isError: false,
				};
			}

			const stagingResult = await this.stageDataInDurableObject(graphqlResult);
			return {
				content: [{
					type: "text" as const,
					text: JSON.stringify(stagingResult, null, 2)
				}],
				isError: false,
			};

		} catch (error) {
			return this.createErrorResponse("GraphQL execution failed", error);
		}
	}

	public async handleSQLTool(args: any) {
		const { data_access_id, sql, include_quality_analysis = false } = args;
		try {
			const queryResult = await this.executeSQLQuery(data_access_id, sql, include_quality_analysis);
			return { 
				content: [{ 
					type: "text" as const, 
					text: JSON.stringify(queryResult, null, 2) 
				}],
				isError: false,
			};
		} catch (error) {
			return this.createErrorResponse("SQL execution failed", error);
		}
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
	// ERROR HANDLING - Enhanced for MCP 2025-06-18
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
				}, null, 2)
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
			},
			resourceLinks: [{
				uri: "https://dgidb.org/",
				description: "DGIdb main website"
			}, {
				uri: "https://dgidb.org/api",
				description: "DGIdb API documentation"
			}, {
				uri: API_CONFIG.endpoint,
				description: "DGIdb GraphQL API endpoint"
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

async function handleStreamableHttp(request: Request, env: Env, standardHeaders: Record<string, string>): Promise<Response> {
	const acceptHeader = request.headers.get("Accept");
	const acceptsJson = acceptsMime(acceptHeader, "application/json");
	const acceptsSse = acceptsMime(acceptHeader, "text/event-stream");

	if (request.method === "OPTIONS") {
		const headers = new Headers(standardHeaders);
		headers.set("Allow", "GET,POST,OPTIONS");
		return new Response(null, { status: 204, headers });
	}

	if (request.method === "GET") {
		if (!acceptsSse) {
			return createJsonResponse(
				buildJsonRpcError(null, -32000, "Client must accept text/event-stream to open an SSE stream"),
				406,
				standardHeaders
			);
		}

		const { readable, writable } = new TransformStream();
		const writer = writable.getWriter();
		const initialEvent = {
			jsonrpc: "2.0",
			method: "notifications/ready",
			params: { message: "SSE stream established", timestamp: new Date().toISOString() }
		};

		await writer.write(textEncoder.encode(`data: ${JSON.stringify(initialEvent)}\n\n`));

		request.signal.addEventListener("abort", () => {
			writer.close().catch(() => {});
		});

		const headers = new Headers(standardHeaders);
		headers.set("Content-Type", "text/event-stream");
		headers.set("Cache-Control", "no-cache, no-transform");
		headers.set("Connection", "keep-alive");

		return new Response(readable, { status: 200, headers });
	}

	if (request.method !== "POST") {
		return createJsonResponse(
			buildJsonRpcError(null, -32600, "Unsupported HTTP method for MCP endpoint"),
			405,
			standardHeaders
		);
	}

	let jsonData: any;

	try {
		const bodyText = await request.text();
		jsonData = JSON.parse(bodyText);
	} catch {
		return createJsonResponse(buildJsonRpcError(null, -32700, "Parse error"), 200, standardHeaders);
	}

	if (typeof jsonData !== "object" || jsonData === null) {
		return createJsonResponse(buildJsonRpcError(null, -32600, "Invalid request"), 200, standardHeaders);
	}

	const hasMethod = typeof jsonData.method === "string";
	const hasId = Object.prototype.hasOwnProperty.call(jsonData, "id");

	// Notifications do not include an id and should be acknowledged with 202
	if (hasMethod && !hasId) {
		return createAcceptedResponse(standardHeaders);
	}

	// JSON-RPC responses should be acknowledged with 202
	if (!hasMethod && hasId) {
		return createAcceptedResponse(standardHeaders);
	}

	if (!hasMethod) {
		return createJsonResponse(buildJsonRpcError(null, -32600, "Invalid request"), 200, standardHeaders);
	}

	const requestId = jsonData.id ?? null;

	if (jsonData.method === "initialize") {
		if (!acceptsJson) {
			return createJsonResponse(
				buildJsonRpcError(requestId, -32000, "Client must accept application/json responses"),
				406,
				standardHeaders
			);
		}

		return createJsonResponse(
			{
				jsonrpc: "2.0",
				id: requestId,
				result: {
					protocolVersion: "2025-06-18",
					capabilities: {
						tools: {}
					},
					serverInfo: {
						name: API_CONFIG.name,
						version: API_CONFIG.version
					}
				}
			},
			200,
			standardHeaders
		);
	}

	if (jsonData.method === "tools/list") {
		if (!acceptsJson) {
			return createJsonResponse(
				buildJsonRpcError(requestId, -32000, "Client must accept application/json responses"),
				406,
				standardHeaders
			);
		}

		return createJsonResponse(
			{
				jsonrpc: "2.0",
				id: requestId,
				result: { tools: MCP_TOOL_DEFINITIONS }
			},
			200,
			standardHeaders
		);
	}

	if (jsonData.method === "tools/call") {
		if (!acceptsSse) {
			return createJsonResponse(
				buildJsonRpcError(requestId, -32000, "Client must accept text/event-stream for tool calls"),
				406,
				standardHeaders
			);
		}

		const { readable, writable } = new TransformStream();
		const writer = writable.getWriter();

		(async () => {
			try {
				const { name, arguments: args } = jsonData.params ?? {};
				if (typeof name !== "string") {
					const message = buildJsonRpcError(requestId, -32602, "Tool name missing in request");
					await writer.write(textEncoder.encode(`data: ${JSON.stringify(message)}\n\n`));
					return;
				}

				const mcpServer = new DGIdbMCP(env);
				let result;

				if (name === API_CONFIG.tools.graphql.name) {
					result = await mcpServer.handleGraphQLTool(args);
				} else if (name === API_CONFIG.tools.sql.name) {
					result = await mcpServer.handleSQLTool(args);
				} else {
					const message = buildJsonRpcError(requestId, -32601, `Unknown tool: ${name}`);
					await writer.write(textEncoder.encode(`data: ${JSON.stringify(message)}\n\n`));
					return;
				}

				const response = {
					jsonrpc: "2.0",
					id: requestId,
					result
				};
				await writer.write(textEncoder.encode(`data: ${JSON.stringify(response)}\n\n`));
			} catch (error) {
				const message = buildJsonRpcError(
					requestId,
					-32603,
					"Internal error",
					error instanceof Error ? error.message : String(error)
				);
				await writer.write(textEncoder.encode(`data: ${JSON.stringify(message)}\n\n`));
			} finally {
				await writer.close();
			}
		})();

		const headers = new Headers(standardHeaders);
		headers.set("Content-Type", "text/event-stream");
		headers.set("Cache-Control", "no-cache, no-transform");
		headers.set("Connection", "keep-alive");

		return new Response(readable, { status: 200, headers });
	}

	return createJsonResponse(buildJsonRpcError(requestId, -32601, "Method not found"), 200, standardHeaders);
}

export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const url = new URL(request.url);

		// Standard MCP headers for all responses with enhanced security and CORS support
		const standardHeaders = {
			"Content-Type": "application/json",
			"MCP-Protocol-Version": "2025-06-18",
			"X-Content-Type-Options": "nosniff",
			"X-Frame-Options": "DENY",
			"Referrer-Policy": "strict-origin-when-cross-origin",
			"Access-Control-Allow-Origin": "*",
			"Access-Control-Allow-Methods": "GET, POST, OPTIONS",
			"Access-Control-Allow-Headers": "*"
		};

		// Handle CORS preflight requests globally
		if (request.method === "OPTIONS") {
			return new Response(null, {
				status: 204,
				headers: standardHeaders
			});
		}

		// Validate MCP protocol version in requests (required by 2025-06-18 spec)
		const clientProtocolVersion = request.headers.get("MCP-Protocol-Version");
		if (clientProtocolVersion && !["2025-06-18", "2025-03-26"].includes(clientProtocolVersion)) {
			return new Response(JSON.stringify({
				error: "Unsupported MCP protocol version",
				supported_versions: ["2025-06-18", "2025-03-26"],
				client_version: clientProtocolVersion
			}), {
				status: 400,
				headers: standardHeaders
			});
		}

		if (url.pathname === "/sse" || url.pathname.startsWith("/sse/")) {
			// Handle SSE transport
			if (request.method === "GET") {
				// SSE connection setup
				const { readable, writable } = new TransformStream();
				const writer = writable.getWriter();
				
				// Setup SSE headers
				const headers = new Headers({
					"Content-Type": "text/event-stream",
					"Cache-Control": "no-cache",
					"Connection": "keep-alive",
					"Access-Control-Allow-Origin": "*",
					"Access-Control-Allow-Headers": "*",
				});
				
				// Simple SSE implementation - send capabilities
				writer.write(new TextEncoder().encode(`data: ${JSON.stringify({
					jsonrpc: "2.0",
					method: "notifications/initialized",
					params: {}
				})}\n\n`));
				
				return new Response(readable, { headers });
			}
			
			return new Response(JSON.stringify({ error: "Method not allowed" }), {
				status: 405,
				headers: standardHeaders
			});
		}

		// Streamable HTTP transport endpoint 
		if (url.pathname === "/mcp" || url.pathname.startsWith("/mcp/")) {
			return handleStreamableHttp(request, env, standardHeaders);
		}

		if (url.pathname === "/datasets" && request.method === "GET") {
			const list = Array.from(datasetRegistry.entries()).map(([id, info]) => ({
				data_access_id: id,
				...info
			}));
			return new Response(JSON.stringify({ datasets: list }, null, 2), {
				headers: standardHeaders
			});
		}

		if (url.pathname.startsWith("/datasets/") && request.method === "DELETE") {
			const id = url.pathname.split("/")[2];
			if (!id || !datasetRegistry.has(id)) {
				return new Response(JSON.stringify({ error: "Dataset not found" }), {
					status: 404,
					headers: standardHeaders
				});
			}

			const doId = env.JSON_TO_SQL_DO.idFromName(id);
			const stub = env.JSON_TO_SQL_DO.get(doId);
			const resp = await stub.fetch("http://do/delete", { method: "DELETE" });
			if (resp.ok) {
				datasetRegistry.delete(id);
				return new Response(JSON.stringify({ success: true }), {
					headers: standardHeaders
				});
			}

			const text = await resp.text();
			return new Response(JSON.stringify({ success: false, error: text }), {
				status: 500,
				headers: standardHeaders
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
				headers: standardHeaders
			});
		}

		// Chunking stats endpoint
		if (url.pathname === "/chunking-stats" && request.method === "GET") {
			const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
			const stub = env.JSON_TO_SQL_DO.get(globalDoId);
			const resp = await stub.fetch("http://do/chunking-stats");
			return new Response(await resp.text(), {
				status: resp.status,
				headers: standardHeaders
			});
		}

		// Chunking analysis endpoint
		if (url.pathname === "/chunking-analysis" && request.method === "GET") {
			const globalDoId = env.JSON_TO_SQL_DO.idFromName("global-schema-config");
			const stub = env.JSON_TO_SQL_DO.get(globalDoId);
			const resp = await stub.fetch("http://do/chunking-analysis");
			return new Response(await resp.text(), {
				status: resp.status,
				headers: standardHeaders
			});
		}

		// MCP capabilities endpoint (2025-06-18 spec)
		if (url.pathname === "/capabilities" && request.method === "GET") {
			const capabilities = {
				protocol_version: "2025-06-18",
				server_info: {
					name: API_CONFIG.name,
					version: API_CONFIG.version,
					description: API_CONFIG.description
				},
				capabilities: {
					tools: {
						supported: true,
						list_changed: false
					},
					resources: {
						supported: false
					},
					prompts: {
						supported: false
					},
					completion: {
						supported: false
					},
					experimental: {
						streamable_http: true,
						structured_output: true,
						resource_links: true
					}
				},
				available_tools: [
					{
						name: API_CONFIG.tools.graphql.name,
						title: API_CONFIG.tools.graphql.title,
						description: API_CONFIG.tools.graphql.description,
						input_schema: {
							type: "object",
							properties: {
								query: { type: "string", description: "GraphQL query string" },
								variables: { type: "object", description: "Optional variables for the GraphQL query" }
							},
							required: ["query"]
						}
					},
					{
						name: API_CONFIG.tools.sql.name,
						title: API_CONFIG.tools.sql.title,
						description: API_CONFIG.tools.sql.description,
						input_schema: {
							type: "object",
							properties: {
								data_access_id: { type: "string", description: "Data access ID from the GraphQL query tool" },
								sql: { type: "string", description: "SQL SELECT query to execute" },
								params: { type: "array", items: { type: "string" }, description: "Optional query parameters" },
								include_quality_analysis: { type: "boolean", description: "Include data quality analysis in response" }
							},
							required: ["data_access_id", "sql"]
						}
					}
				]
			};
			
			return new Response(JSON.stringify(capabilities, null, 2), {
				headers: standardHeaders
			});
		}

		return new Response(
			`${API_CONFIG.name} - Available endpoints:\n- /mcp (Streamable HTTP)\n- /sse (SSE legacy)`,
			{ status: 404, headers: { "Content-Type": "text/plain", "MCP-Protocol-Version": "2025-06-18" } }
		);
	},
};

export { DGIdbMCP as MyMCP };
export { JsonToSqlDO };
