/**
 * Search tool factory — creates a `<prefix>_search` tool for API discovery.
 *
 * Two modes:
 * 1. **Catalog mode** (legacy) — runs in-process keyword search over a static ApiCatalog.
 * 2. **OpenAPI mode** (new) — evaluates agent-written JS with the full resolved
 *    OpenAPI spec available. The agent can search paths, list tags, describe
 *    operations, etc., using injected helper functions.
 *
 * When `openApiSpec` is provided, the tool switches to OpenAPI mode.
 * When only `catalog` is provided, the tool uses the original catalog mode.
 */
import { z } from "zod";
import { buildOpenApiSearchSource } from "./openapi-search";
/**
 * Token-based search over catalog endpoints.
 */
function searchEndpoints(endpoints, query, maxResults) {
    const tokens = query.toLowerCase().split(/\s+/).filter(Boolean);
    if (tokens.length === 0)
        return [];
    const scored = endpoints.map((ep) => {
        const text = [
            ep.path,
            ep.summary,
            ep.description || "",
            ep.category,
            ep.method,
            ...(ep.pathParams || []).map((p) => `${p.name} ${p.description}`),
            ...(ep.queryParams || []).map((p) => `${p.name} ${p.description}`),
        ]
            .join(" ")
            .toLowerCase();
        let score = 0;
        for (const token of tokens) {
            if (text.includes(token))
                score++;
        }
        return { endpoint: ep, score };
    });
    return scored
        .filter((s) => s.score > 0)
        .sort((a, b) => b.score - a.score)
        .slice(0, maxResults)
        .map((s) => s.endpoint);
}
/**
 * Format an endpoint for display.
 */
function formatEndpoint(ep) {
    const lines = [`${ep.method} ${ep.path} — ${ep.summary}`];
    if (ep.coveredByTool)
        lines.push(`  (also available via tool: ${ep.coveredByTool})`);
    if (ep.pathParams?.length) {
        for (const p of ep.pathParams) {
            lines.push(`  Path: {${p.name}} (${p.type}, ${p.required ? "required" : "optional"}) — ${p.description}`);
        }
    }
    if (ep.queryParams?.length) {
        for (const p of ep.queryParams) {
            const extras = [];
            if (p.default !== undefined)
                extras.push(`default: ${JSON.stringify(p.default)}`);
            if (p.enum)
                extras.push(`values: ${JSON.stringify(p.enum)}`);
            lines.push(`  Query: ${p.name} (${p.type}, ${p.required ? "required" : "optional"}) — ${p.description}${extras.length ? ` [${extras.join(", ")}]` : ""}`);
        }
    }
    if (ep.body) {
        lines.push(`  Body: ${ep.body.contentType}${ep.body.description ? ` — ${ep.body.description}` : ""}`);
    }
    return lines.join("\n");
}
/**
 * Count the total number of operations in a resolved OpenAPI spec.
 */
function countSpecOperations(spec) {
    const methods = ["get", "post", "put", "delete", "patch", "options", "head", "trace"];
    let count = 0;
    for (const pathItem of Object.values(spec.paths)) {
        if (!pathItem || typeof pathItem !== "object")
            continue;
        for (const method of methods) {
            if (pathItem[method])
                count++;
        }
    }
    return count;
}
function formatOperation(op) {
    const lines = [`${op.method.toUpperCase()} ${op.path} — ${op.summary || op.operationId || "No summary"}`];
    if (op.operationId)
        lines.push(`  Operation ID: ${op.operationId}`);
    if (op.tags?.length)
        lines.push(`  Tags: ${op.tags.join(", ")}`);
    for (const param of op.parameters || []) {
        const type = param.schema?.type || param.type || "unknown";
        const location = param.in || "unknown";
        lines.push(`  Param: ${param.name || "(unnamed)"} (${location}, ${type}, ${param.required ? "required" : "optional"})` +
            `${param.description ? ` — ${param.description}` : ""}`);
    }
    const contentTypes = Object.keys(op.requestBody?.content || {});
    if (contentTypes.length > 0) {
        lines.push(`  Body: ${contentTypes[0]}${op.requestBody?.description ? ` — ${op.requestBody.description}` : ""}`);
    }
    if (op.responses) {
        for (const [status, response] of Object.entries(op.responses)) {
            if (response?.description) {
                lines.push(`  Response: ${status} — ${response.description}`);
                break;
            }
        }
    }
    return lines.join("\n");
}
function createOpenApiHelpers(specJson) {
    const searchSource = buildOpenApiSearchSource(specJson);
    const fn = new Function(`${searchSource}; return { searchPaths, listTags, getOperation, describeOperation, searchSpec, listCategories, getEndpoint, describeEndpoint, spec, SPEC };`);
    return fn();
}
/**
 * Create a search tool in OpenAPI mode.
 *
 * The tool accepts a `code` parameter — agent-written JavaScript that runs
 * with the full resolved OpenAPI spec and helper functions (searchPaths,
 * listTags, getOperation, describeOperation) available.
 */
function createOpenApiSearchTool(prefix, spec) {
    const toolName = `${prefix}_search`;
    const operationCount = countSpecOperations(spec);
    const specJson = JSON.stringify(spec);
    const helpers = createOpenApiHelpers(specJson);
    return {
        name: toolName,
        description: `Search the ${spec.info.title} API (${operationCount} operations across ${Object.keys(spec.paths).length} paths). ` +
            `Write JavaScript code to search the OpenAPI spec, or use the legacy query/category arguments for keyword search. Available functions:\n\n` +
            `- searchPaths(query, maxResults=10) — keyword search across paths, summaries, tags, parameters\n` +
            `- listTags() — list all tags with operation counts\n` +
            `- getOperation(idOrPath) — get full operation by operationId or path\n` +
            `- describeOperation(idOrPath) — formatted documentation for an operation\n` +
            `- searchSpec/query helpers are also available for backward compatibility inside execute()\n` +
            `- spec — the full frozen OpenAPI spec object (spec.paths, spec.info, etc.)\n\n` +
            `Use ${prefix}_search to discover endpoints, then write code in ${prefix}_execute to call them.\n\n` +
            `USAGE IN ${prefix}_execute:\n` +
            `- api.get(path, params) for GET, api.post(path, body, params) for POST\n` +
            `- Path params like /lookup/{id} are auto-interpolated from params\n` +
            `- Large responses (>100KB) are auto-staged; use ${prefix}_query_data to explore`,
        schema: {
            code: z.string().describe("JavaScript code to search the API spec. Use searchPaths(), listTags(), " +
                "getOperation(), describeOperation(), or access spec.paths directly. " +
                'Examples: \'return searchPaths("studies")\', \'return listTags()\', ' +
                '\'return describeOperation("getStudies")\''),
            query: z.string().optional().describe("Legacy keyword search. Optional alternative to code. Use '*' or an empty string to browse operations."),
            category: z.string().optional().describe("Legacy category filter. Matches OpenAPI tags case-insensitively."),
            max_results: z.number().optional().describe("Maximum results to return for legacy keyword search (default 10, max 25)."),
        },
        register(server) {
            const description = this.description;
            const schema = this.schema;
            server.tool(toolName, description, schema, async (input) => {
                const code = input.code?.trim() || "";
                const query = input.query?.trim() || "";
                const category = input.category?.trim();
                const maxResults = Math.min(input.max_results || 10, 25);
                if (!code) {
                    let results = query === "*" || query === ""
                        ? helpers.searchPaths("", operationCount)
                        : helpers.searchPaths(query, category ? operationCount : maxResults);
                    if (category) {
                        const normalized = category.toLowerCase();
                        results = results.filter((op) => (op.tags || []).some((tag) => tag.toLowerCase() === normalized));
                    }
                    if (query === "*" || query === "") {
                        results = results.slice(0, maxResults);
                    }
                    if (results.length === 0) {
                        const availableTags = helpers.listTags()
                            .map((entry) => `  ${entry.tag} (${entry.count} operations)`)
                            .join("\n");
                        return {
                            content: [{
                                    type: "text",
                                    text: `No operations found for "${query || "*"}"${category ? ` in category "${category}"` : ""}.\n\n` +
                                        `Available categories:\n${availableTags}\n\nTry broader search terms, browse by category, or provide code.`,
                                }],
                            structuredContent: {
                                success: true,
                                data: {
                                    total_operations: operationCount,
                                    total_endpoints: operationCount,
                                    results_count: 0,
                                    operations: [],
                                    endpoints: [],
                                },
                            },
                        };
                    }
                    const formatted = results.map(formatOperation).join("\n\n");
                    const header = `Found ${results.length} operation(s) in ${spec.info.title} API (${operationCount} total):`;
                    return {
                        content: [{ type: "text", text: `${header}\n\n${formatted}` }],
                        structuredContent: {
                            success: true,
                            data: {
                                total_operations: operationCount,
                                total_endpoints: operationCount,
                                results_count: results.length,
                                operations: results,
                                endpoints: results,
                            },
                        },
                    };
                }
                try {
                    // Build the search helpers source and wrap user code.
                    // The helpers also expose legacy aliases used by execute().
                    const searchSource = buildOpenApiSearchSource(specJson);
                    const wrappedCode = `${searchSource}\n${code}`;
                    // Evaluate using new Function() — same sandboxing approach
                    // as catalog-search tests. The V8 isolate integration comes
                    // later when wired to DynamicWorkerExecutor.
                    const fn = new Function(wrappedCode);
                    const result = fn();
                    // Format the result for display
                    let textOutput;
                    if (typeof result === "string") {
                        textOutput = result;
                    }
                    else {
                        textOutput = JSON.stringify(result, null, 2);
                    }
                    return {
                        content: [{ type: "text", text: textOutput }],
                        structuredContent: {
                            success: true,
                            data: result,
                        },
                    };
                }
                catch (err) {
                    const message = err instanceof Error ? err.message : String(err);
                    return {
                        content: [{
                                type: "text",
                                text: `Search code error: ${message}`,
                            }],
                        structuredContent: {
                            success: false,
                            error: { code: "SEARCH_ERROR", message },
                        },
                        isError: true,
                    };
                }
            });
        },
    };
}
/**
 * Create a search tool in catalog mode (legacy).
 *
 * The tool accepts query/category/max_results parameters and performs
 * keyword-based search over the static ApiCatalog.
 */
function createCatalogSearchTool(prefix, catalog) {
    const toolName = `${prefix}_search`;
    // Collect categories for the description
    const categories = new Map();
    for (const ep of catalog.endpoints) {
        categories.set(ep.category, (categories.get(ep.category) || 0) + 1);
    }
    const categoryList = Array.from(categories.entries())
        .map(([cat, count]) => `${cat} (${count})`)
        .join(", ");
    const notesSection = catalog.notes ? `\n\nNOTES:\n${catalog.notes}` : "";
    return {
        name: toolName,
        description: `Search the ${catalog.name} API catalog (${catalog.endpointCount} endpoints). ` +
            `Returns matching endpoints with full parameter docs. Use this to discover API capabilities before calling ${prefix}_execute.\n\n` +
            `Categories: ${categoryList}\n\n` +
            `USAGE IN ${prefix}_execute:\n` +
            `- api.get(path, params) for GET, api.post(path, body, params) for POST\n` +
            `- Path params like /lookup/{id} are auto-interpolated from params: api.get('/lookup/{id}', {id: 'ENSG...'})\n` +
            `- Remaining params become query string\n` +
            `- Large responses (>100KB) are auto-staged: check result.__staged, return the staging info, use ${prefix}_query_data to explore\n` +
            `- Use limit/pagination params to control response size. Large datasets auto-stage for SQL queries.` +
            notesSection,
        schema: {
            query: z.string().describe("Search query — keywords matching endpoint paths, descriptions, parameters, or categories. Examples: 'gene expression', 'variant annotation', 'tissue'"),
            category: z.string().optional().describe("Filter to a specific category. Use query='*' with a category to list all endpoints in that category."),
            max_results: z.number().optional().describe("Maximum results to return (default 10, max 25)"),
        },
        register(server) {
            server.tool(toolName, this.description, this.schema, async (input) => {
                const maxResults = Math.min(input.max_results || 10, 25);
                const query = input.query?.trim() || "";
                let endpoints = catalog.endpoints;
                // Filter by category if specified
                if (input.category) {
                    endpoints = endpoints.filter((ep) => ep.category.toLowerCase() === input.category.toLowerCase());
                }
                let results;
                if (query === "*" || query === "") {
                    // List mode — return all (within category filter)
                    results = endpoints.slice(0, maxResults);
                }
                else {
                    results = searchEndpoints(endpoints, query, maxResults);
                }
                if (results.length === 0) {
                    // Return available categories as a hint
                    const categories = new Map();
                    for (const ep of catalog.endpoints) {
                        categories.set(ep.category, (categories.get(ep.category) || 0) + 1);
                    }
                    const catList = Array.from(categories.entries())
                        .map(([cat, count]) => `  ${cat} (${count} endpoints)`)
                        .join("\n");
                    return {
                        content: [{
                                type: "text",
                                text: `No endpoints found for "${query}"${input.category ? ` in category "${input.category}"` : ""}.\n\nAvailable categories:\n${catList}\n\nTry broader search terms or browse by category.`,
                            }],
                    };
                }
                const formatted = results.map(formatEndpoint).join("\n\n");
                const header = `Found ${results.length} endpoint(s) in ${catalog.name} API (${catalog.endpointCount} total):`;
                return {
                    content: [{ type: "text", text: `${header}\n\n${formatted}` }],
                    structuredContent: {
                        success: true,
                        data: {
                            total_endpoints: catalog.endpointCount,
                            results_count: results.length,
                            endpoints: results,
                        },
                    },
                };
            });
        },
    };
}
/**
 * Create a search tool registration object.
 * Returns { name, description, schema, register } for the server to use.
 *
 * When `openApiSpec` is provided, creates a code-execution search tool.
 * When only `catalog` is provided, creates a keyword search tool (legacy).
 */
export function createSearchTool(options) {
    const { prefix, catalog, openApiSpec } = options;
    if (openApiSpec) {
        return createOpenApiSearchTool(prefix, openApiSpec);
    }
    if (catalog) {
        return createCatalogSearchTool(prefix, catalog);
    }
    throw new Error("createSearchTool requires either 'catalog' or 'openApiSpec'");
}
//# sourceMappingURL=search-tool.js.map